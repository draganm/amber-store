package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// PullStats summarizes one Pull.
type PullStats struct {
	ObjectsFetched int   // objects downloaded from the server
	BytesFetched   int64 // their payload bytes
}

// Pull completes the local store under root: a round-based BFS where each
// round classifies the frontier (locally-present keys contribute their
// children without a fetch — so a partial local tree completes correctly —
// and missing keys are binned into byte-balanced batches fetched by N
// parallel workers), stores each fetched object with hash verification, and
// feeds the children of fetched tree/file nodes into the next round.
// Idempotent: a re-run fetches nothing, and a run interrupted mid-round is
// safe to repeat — already-stored objects are skipped by the Has check.
func Pull(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PullStats, error) {
	seen := map[key.Key]bool{}
	frontier := []key.Key{root}
	var stats PullStats

	for len(frontier) > 0 {
		var want []key.Key
		var next []key.Key
		for _, k := range frontier {
			if seen[k] {
				continue
			}
			seen[k] = true
			has, err := store.Has(k)
			if err != nil {
				return stats, err
			}
			if has {
				if k.Type() == key.Blob || k.Type() == key.XattrSet {
					continue
				}
				data, err := store.Get(k)
				if err != nil {
					return stats, err
				}
				kids, err := fstree.ChildKeys(k, data)
				if err != nil {
					return stats, err
				}
				next = append(next, kids...)
				continue
			}
			want = append(want, k)
		}

		var mu sync.Mutex
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(opts.jobs())
		for _, batch := range Batches(want, opts.batchBytes(), PullSizer()) {
			g.Go(func() error {
				objs, err := rc.FetchObjects(gctx, batch)
				if err != nil {
					return err
				}
				seq := func(yield func(packstore.Object, error) bool) {
					for _, o := range objs {
						if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
							return
						}
					}
				}
				// Verify: the store re-hashes every object against its key, so a
				// hostile or corrupted stream can never poison the local store.
				if _, err := store.WriteParallel(seq, packstore.WriteOpts{Verify: true}); err != nil {
					return fmt.Errorf("storing fetched objects: %w", err)
				}
				var kids []key.Key
				var fetchedBytes int64
				for _, o := range objs {
					fetchedBytes += int64(len(o.Bytes))
					ck, err := fstree.ChildKeys(o.Key, o.Bytes)
					if err != nil {
						return err
					}
					kids = append(kids, ck...)
				}
				mu.Lock()
				next = append(next, kids...)
				stats.ObjectsFetched += len(objs)
				stats.BytesFetched += fetchedBytes
				progressDone := stats.ObjectsFetched
				mu.Unlock()
				// Outside the lock, mirroring Push: a slow callback cannot
				// stall the other workers.
				if opts.Progress != nil {
					opts.Progress(progressDone, 0)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return stats, err
		}
		frontier = next
	}
	return stats, nil
}
