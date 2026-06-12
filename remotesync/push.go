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
	// Progress, when non-nil, is called as work completes, possibly
	// concurrently from several workers. For Push, done counts settled keys
	// — confirmed present at negotiation, or uploaded — out of total
	// reachable keys; for Pull, done counts fetched objects and total is 0
	// (unknown up front).
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

// Push uploads every object reachable from root that the server is missing,
// as a three-stage pipeline: checkers negotiate each byte-balanced batch of
// the local reachable set against the server, a re-batcher coalesces the
// sparse missing subsets into full upload batches, and uploaders send each
// batch as one amberpack. Negotiation and upload overlap, with the jobs
// budget split between the two pools. Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *diskstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	checkBatches := Batches(keys, opts.batchBytes(), PushSizer(store))
	checkers, uploaders := splitJobs(opts.jobs())

	var mu sync.Mutex
	stats := PushStats{ObjectsTotal: len(keys)}
	done := 0
	// settle records n settled keys (pushed of them uploaded, totaling
	// pushedBytes) and reports progress outside the lock so a slow callback
	// (e.g. an HTTP flush) cannot stall the workers; values are monotonic
	// snapshots but may arrive out of order under contention.
	settle := func(n, pushed int, pushedBytes int64) {
		mu.Lock()
		done += n
		stats.ObjectsPushed += pushed
		stats.BytesPushed += pushedBytes
		progressDone, progressTotal := done, stats.ObjectsTotal
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(progressDone, progressTotal)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	missingCh := make(chan []key.Key)
	uploadCh := make(chan []key.Key)

	// Checkers: negotiate each check batch; keys the server already has
	// settle immediately, missing ones flow to the re-batcher. The
	// companion closes missingCh once every checker is done.
	g.Go(func() error {
		cg, cctx := errgroup.WithContext(gctx)
		cg.SetLimit(checkers)
		for _, batch := range checkBatches {
			cg.Go(func() error {
				missing, err := rc.Missing(cctx, batch)
				if err != nil {
					return err
				}
				// Clamped so a server replying with keys outside the queried
				// batch cannot drive progress backwards.
				settle(max(0, len(batch)-len(missing)), 0, 0)
				if len(missing) == 0 {
					return nil
				}
				select {
				case missingCh <- missing:
					return nil
				case <-cctx.Done():
					return cctx.Err()
				}
			})
		}
		err := cg.Wait()
		close(missingCh)
		return err
	})

	// Re-batcher: coalesce sparse missing subsets into full upload batches.
	g.Go(func() error {
		defer close(uploadCh)
		return rebatch(gctx, missingCh, uploadCh, opts.batchBytes(), PushSizer(store))
	})

	// Uploaders: long-lived workers draining uploadCh.
	for range uploaders {
		g.Go(func() error {
			for {
				var batch []key.Key
				var ok bool
				select {
				case batch, ok = <-uploadCh:
					if !ok {
						return nil
					}
				case <-gctx.Done():
					return gctx.Err()
				}
				objs := make([]fstree.Object, 0, len(batch))
				var pushedBytes int64
				for _, k := range batch {
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
				settle(len(batch), len(batch), pushedBytes)
			}
		})
	}

	if err := g.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}
