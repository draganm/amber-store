package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// DefaultJobs is the default number of parallel transfer workers.
const DefaultJobs = 4

// Opts configures Push and Pull.
type Opts struct {
	BatchBytes uint64 // per-batch payload target; 0 = DefaultBatchBytes
	Jobs       int    // parallel workers; <= 0 = DefaultJobs
	// Progress, when non-nil, is called as work completes. For Push, done
	// counts negotiated keys out of total reachable keys; for Pull, done
	// counts fetched objects and total is 0 (unknown up front).
	Progress func(done, total int)
}

func (o Opts) batchBytes() uint64 {
	if o.BatchBytes == 0 {
		return DefaultBatchBytes
	}
	return o.BatchBytes
}

func (o Opts) jobs() int {
	if o.Jobs <= 0 {
		return DefaultJobs
	}
	return o.Jobs
}

// splitJobs divides the jobs budget between negotiation and upload workers
// so total in-flight requests stay <= jobs. jobs=1 floors at 1+1, the
// minimum for a pipeline and the one documented budget overshoot.
func splitJobs(jobs int) (checkers, uploaders int) {
	checkers = max(1, jobs/4)
	uploaders = max(1, jobs-checkers)
	return checkers, uploaders
}

// PushStats summarizes one Push.
type PushStats struct {
	ObjectsTotal  int   // reachable objects under the root
	ObjectsPushed int   // objects the server was missing and received
	BytesPushed   int64 // payload bytes of pushed objects
}

// Push uploads every object reachable from root that the server is missing:
// walk the local reachable set, bin it into byte-balanced batches, and per
// batch (N workers in parallel) negotiate the missing subset and upload an
// amberpack of exactly those objects. Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *diskstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	batches := Batches(keys, opts.batchBytes(), PushSizer(store))

	var mu sync.Mutex
	stats := PushStats{ObjectsTotal: len(keys)}
	done := 0

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.jobs())
	for _, batch := range batches {
		g.Go(func() error {
			missing, err := rc.Missing(gctx, batch)
			if err != nil {
				return err
			}
			var pushedBytes int64
			if len(missing) > 0 {
				objs := make([]fstree.Object, 0, len(missing))
				for _, k := range missing {
					data, err := store.Get(k)
					if err != nil {
						return fmt.Errorf("reading %s: %w", k, err)
					}
					objs = append(objs, fstree.Object{Key: k, Bytes: data})
					pushedBytes += int64(len(data))
				}
				if _, err := rc.PushPack(gctx, objs); err != nil {
					return err
				}
			}
			mu.Lock()
			stats.ObjectsPushed += len(missing)
			stats.BytesPushed += pushedBytes
			done += len(batch)
			progressDone, progressTotal := done, stats.ObjectsTotal
			mu.Unlock()
			// Called outside the lock so a slow callback (e.g. an HTTP flush)
			// cannot stall the other workers; values are monotonic snapshots
			// but may arrive out of order under contention.
			if opts.Progress != nil {
				opts.Progress(progressDone, progressTotal)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}
