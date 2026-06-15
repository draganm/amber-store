package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
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

// PushStats summarizes one Push.
type PushStats struct {
	ObjectsTotal  int   // reachable objects under the root
	ObjectsPushed int   // objects the server was missing and received
	BytesPushed   int64 // payload bytes of pushed objects
}

// Push uploads every object reachable from root that the server is missing, in
// two phases: a whole-set have/want negotiation — the reachable key list is
// checked against the server in parallel chunks — followed by a parallel upload
// of byte-balanced packs holding exactly the missing objects. Collecting the
// whole missing set before batching coalesces sparse misses into full packs.
// Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, name string, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	stats := PushStats{ObjectsTotal: len(keys)}

	var mu sync.Mutex
	done := 0
	// settle records n newly-settled keys (pushed of which were uploaded,
	// totaling pushedBytes) and reports progress outside the lock so a slow
	// callback cannot stall the workers.
	settle := func(n, pushed int, pushedBytes int64) {
		mu.Lock()
		done += n
		stats.ObjectsPushed += pushed
		stats.BytesPushed += pushedBytes
		progressDone := done
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(progressDone, len(keys))
		}
	}

	// Phase 1: negotiate the whole reachable set against the server in parallel
	// chunks; keys the server already has settle immediately, the rest are
	// collected into the missing set.
	var missMu sync.Mutex
	var missing []key.Key
	neg, negCtx := errgroup.WithContext(ctx)
	neg.SetLimit(opts.jobs())
	for _, chunk := range checkChunks(keys) {
		neg.Go(func() error {
			miss, err := rc.Missing(negCtx, chunk)
			if err != nil {
				return err
			}
			// Clamped so a server replying with keys outside the queried chunk
			// cannot drive progress backwards.
			settle(max(0, len(chunk)-len(miss)), 0, 0)
			if len(miss) > 0 {
				missMu.Lock()
				missing = append(missing, miss...)
				missMu.Unlock()
			}
			return nil
		})
	}
	if err := neg.Wait(); err != nil {
		return stats, err
	}

	// Phase 2: upload byte-balanced packs holding exactly the missing objects.
	// readSlots bounds concurrent record reads across all upload workers: a pack
	// assembling alone (while its peers upload) gets up to jobs()-way parallel
	// reads — overlapping cold page faults — without letting total in-flight
	// reads exceed jobs().
	readSlots := make(chan struct{}, opts.jobs())
	up, upCtx := errgroup.WithContext(ctx)
	up.SetLimit(opts.jobs())
	for _, batch := range Batches(missing, opts.batchBytes(), PushSizer(store)) {
		up.Go(func() error {
			recs := make([][]byte, len(batch))
			// Read the pack's records concurrently into their slots; each read
			// holds a shared slot only while it runs. Distinct indices, so no
			// lock is needed on recs.
			rg, rgCtx := errgroup.WithContext(upCtx)
			for i, k := range batch {
				rg.Go(func() error {
					select {
					case readSlots <- struct{}{}:
					case <-rgCtx.Done():
						return rgCtx.Err()
					}
					defer func() { <-readSlots }()
					// Copy the on-disk record verbatim: it is wire-format-
					// identical, so the push skips the decompress/recompress
					// round trip.
					rec, err := store.GetRecord(k)
					if err != nil {
						return fmt.Errorf("reading %s: %w", k, err)
					}
					recs[i] = rec
					return nil
				})
			}
			if err := rg.Wait(); err != nil {
				return err
			}
			var pushedBytes int64
			for _, rec := range recs {
				pushedBytes += int64(len(rec) - amberpack.RecHeaderSize)
			}
			if err := rc.PushPackRaw(upCtx, name, root, recs); err != nil {
				return err
			}
			settle(len(batch), len(batch), pushedBytes)
			return nil
		})
	}
	if err := up.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}

// checkChunks splits the reachable key list into have/want request bodies, each
// at most maxBatchKeys keys so the request body stays well under the server's
// cap (maxBatchKeys * key.Size bytes).
func checkChunks(keys []key.Key) [][]key.Key {
	var chunks [][]key.Key
	for len(keys) > maxBatchKeys {
		chunks = append(chunks, keys[:maxBatchKeys])
		keys = keys[maxBatchKeys:]
	}
	if len(keys) > 0 {
		chunks = append(chunks, keys)
	}
	return chunks
}
