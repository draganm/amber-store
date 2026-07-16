package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// PullStats summarizes one Pull.
type PullStats struct {
	ObjectsFetched int   // objects downloaded from the server
	BytesFetched   int64 // their payload bytes
}

// Pull completes the local store under root from the remote: it asks the server
// for the full set of reachable keys, fetches every key the local store lacks as
// byte-balanced packs in parallel, then runs a local completeness walk as the
// authoritative gate — any object still missing (a key the server omitted from
// its list) is fetched and the gate re-runs, to a fixpoint. In the common case
// the gate walks once and fetches nothing. Idempotent: a re-run fetches nothing,
// and an interrupted run is safe to repeat — already-stored objects are skipped,
// and the gate guarantees completeness before the caller writes the reference.
func Pull(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PullStats, error) {
	var stats PullStats
	var mu sync.Mutex
	// settle records n fetched objects and reports progress outside the lock so
	// a slow callback cannot stall the workers. Pull's total is unknown to the
	// progress contract, so it reports 0.
	settle := func(n int, fetchedBytes int64) {
		mu.Lock()
		stats.ObjectsFetched += n
		stats.BytesFetched += fetchedBytes
		done := stats.ObjectsFetched
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(done, 0)
		}
	}

	// Bulk phase: fetch everything the server says is reachable that we lack.
	serverKeys, err := rc.ReachableKeys(ctx, root)
	if err != nil {
		return stats, fmt.Errorf("listing reachable keys: %w", err)
	}
	toFetch, err := store.Missing(serverKeys)
	if err != nil {
		return stats, fmt.Errorf("checking the reachable set against the local store: %w", err)
	}

	// Fetch, then walk locally to confirm completeness; any straggler the
	// server's list omitted is fetched and the gate re-runs, to a fixpoint. The
	// first iteration performs the bulk fetch; later iterations (if any) recover
	// stragglers.
	for {
		if len(toFetch) > 0 {
			if err := fetchAll(ctx, store, rc, toFetch, opts, settle); err != nil {
				return stats, err
			}
		}
		toFetch, err = localMissing(root, store)
		if err != nil {
			return stats, err
		}
		if len(toFetch) == 0 {
			return stats, nil
		}
	}
}

// fetchAll downloads keys as byte-balanced packs in parallel, verifying and
// storing each pack against its keys.
func fetchAll(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, keys []key.Key, opts Opts, settle func(n int, fetchedBytes int64)) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.jobs())
	for _, batch := range Batches(keys, opts.batchBytes(), PullSizer()) {
		g.Go(func() error {
			objs, err := rc.FetchObjects(gctx, batch, opts.OnBytes)
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
			var fetchedBytes int64
			for _, o := range objs {
				fetchedBytes += int64(len(o.Bytes))
			}
			settle(len(objs), fetchedBytes)
			return nil
		})
	}
	return g.Wait()
}

// localMissing walks the tree from root over the LOCAL store and returns the
// keys that are reachable but not yet present — the set still to fetch. A
// present non-leaf node is descended; an absent node is collected and not
// descended (its children are unknown until it is fetched). Blob and XattrSet
// keys are leaves and are never descended.
func localMissing(root key.Key, store *packstore.Store) ([]key.Key, error) {
	seen := map[key.Key]bool{}
	var missing []key.Key
	frontier := []key.Key{root}
	for len(frontier) > 0 {
		var next []key.Key
		for _, k := range frontier {
			if seen[k] {
				continue
			}
			seen[k] = true
			has, err := store.Has(k)
			if err != nil {
				return nil, err
			}
			if !has {
				missing = append(missing, k)
				continue
			}
			if k.Type() == key.Blob || k.Type() == key.XattrSet {
				continue
			}
			data, err := store.Get(k)
			if err != nil {
				return nil, err
			}
			kids, err := fstree.ChildKeys(k, data)
			if err != nil {
				return nil, err
			}
			next = append(next, kids...)
		}
		frontier = next
	}
	return missing, nil
}
