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

// DefaultJobs is the default number of parallel transfer workers (reader and
// uploader pool size).
const DefaultJobs = 8

// Opts configures Push and Pull.
type Opts struct {
	BatchBytes uint64 // per-batch payload target; 0 = DefaultBatchBytes
	Jobs       int    // parallel workers; <= 0 = DefaultJobs
	// Prefetch is the number of assembled-but-not-yet-uploaded packs Push keeps
	// queued ahead of the uploaders, so a slow disk read never starves the
	// network and vice versa; <= 0 = Jobs. Peak push memory is roughly
	// (Prefetch + 2*Jobs) * BatchBytes, so lower it (or BatchBytes) to cap RAM.
	Prefetch int
	// Progress, when non-nil, is called as work completes, possibly
	// concurrently from several workers. For Push, done counts settled keys
	// — confirmed present at negotiation, or uploaded — out of total
	// reachable keys; for Pull, done counts fetched objects and total is 0
	// (unknown up front).
	Progress func(done, total int)
	// OnBytes, when non-nil, is called with wire-transfer byte increments as
	// they happen, possibly concurrently from several workers. Unlike
	// Progress it ticks DURING a batch — Progress goes silent for the whole
	// duration of a large batch on a slow link, so a liveness watchdog fed
	// only by Progress cannot tell a slow-but-moving transfer from a wedged
	// one. n is an increment, not a cumulative count. Keep it cheap: it is
	// called once per network read/write.
	OnBytes func(n int)
	// MaxBytes, when > 0, fails a Pull with ErrTooLarge once more than
	// this many payload bytes have been fetched. Push ignores it.
	MaxBytes int64
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

func (o Opts) prefetch() int {
	if o.Prefetch <= 0 {
		return o.jobs()
	}
	return o.Prefetch
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

	// Order the missing set by on-disk layout before batching so each pack's
	// reads sweep the segment files sequentially instead of chasing the
	// reachable-walk order across random offsets.
	store.SortByLocation(missing)

	// Phase 2: assemble and upload byte-balanced packs holding exactly the
	// missing objects, as a decoupled pipeline — a reader pool assembles packs
	// (reading records in the disk order set above) into a bounded prefetch
	// queue, an uploader pool drains it. Decoupling keeps the disk continuously
	// reading ahead and the network continuously uploading, so neither stalls
	// waiting on the other; a coupled read-then-upload worker would idle the
	// network during every next-pack read.
	batches := Batches(missing, opts.batchBytes(), PushSizer(store))
	ready := make(chan assembledPack, opts.prefetch())
	g, gctx := errgroup.WithContext(ctx)

	// Reader pool: assemble packs into ready, then close it so the uploaders
	// drain and finish. Bounded by jobs(); each reader sweeps its pack's records
	// sequentially in disk order.
	g.Go(func() error {
		rg, rgCtx := errgroup.WithContext(gctx)
		rg.SetLimit(opts.jobs())
		for _, batch := range batches {
			rg.Go(func() error {
				pack, err := assemblePack(rgCtx, store, batch)
				if err != nil {
					return err
				}
				select {
				case ready <- pack:
					return nil
				case <-rgCtx.Done():
					return rgCtx.Err()
				}
			})
		}
		err := rg.Wait()
		close(ready)
		return err
	})

	// Uploader pool: drain ready. Running directly in g means an upload error
	// cancels gctx, which stops the readers.
	for range opts.jobs() {
		g.Go(func() error {
			for pack := range ready {
				if err := rc.PushPackRaw(gctx, name, root, pack.recs, opts.OnBytes); err != nil {
					return err
				}
				settle(pack.count, pack.count, pack.bytes)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}

// assembledPack is one pack read from the store, ready to upload.
type assembledPack struct {
	recs  [][]byte
	count int   // objects in the pack
	bytes int64 // stored (on-the-wire) payload bytes
}

// assemblePack reads every record of batch from the store in order, copying the
// on-disk records verbatim (they are wire-format-identical, so no decompress/
// recompress). It checks ctx between reads so a cancelled push stops promptly.
func assemblePack(ctx context.Context, store *packstore.Store, batch []key.Key) (assembledPack, error) {
	recs := make([][]byte, len(batch))
	var pushedBytes int64
	for i, k := range batch {
		if err := ctx.Err(); err != nil {
			return assembledPack{}, err
		}
		rec, err := store.GetRecord(k)
		if err != nil {
			return assembledPack{}, fmt.Errorf("reading %s: %w", k, err)
		}
		recs[i] = rec
		pushedBytes += int64(len(rec) - amberpack.RecHeaderSize)
	}
	return assembledPack{recs: recs, count: len(batch), bytes: pushedBytes}, nil
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
