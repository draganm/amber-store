package remotesync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"golang.org/x/sync/errgroup"
)

// Source is the read access Pull needs from a remote. *remoteclient.Client
// implements it. Pull re-verifies every object against its key, so a Source
// is never trusted.
type Source interface {
	ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error)
	FetchObjects(ctx context.Context, keys []key.Key, onBytes func(n int)) ([]fstree.Object, error)
}

// RecordSource additionally streams encoded records. Pull prefers it:
// records enter the store verbatim (verified) as they arrive, so no
// recompression, and verification overlaps the download.
type RecordSource interface {
	StreamRecords(ctx context.Context, keys []key.Key, onBytes func(n int), emit func(amberpack.RawRecord) error) error
}

// ErrTooLarge reports a Pull that exceeded Opts.MaxBytes.
var ErrTooLarge = errors.New("remotesync: pull exceeds the byte limit")

var errStopStream = errors.New("remotesync: stream consumer stopped")

// PullStats summarizes one Pull.
type PullStats struct {
	ObjectsFetched int   // objects downloaded from the server
	BytesFetched   int64 // their payload bytes
}

// Pull completes the local store under root: it fetches the keys the
// local store lacks as byte-balanced packs, then a local completeness
// walk gates the result, fetching stragglers to a fixpoint. Idempotent
// and safe to interrupt and repeat.
func Pull(ctx context.Context, store *packstore.Store, src Source, root key.Key, opts Opts) (PullStats, error) {
	var stats PullStats
	var mu sync.Mutex
	// settle records n fetched objects and reports progress outside the lock so
	// a slow callback cannot stall the workers. Pull's total is unknown to the
	// progress contract, so it reports 0.
	settle := func(n int, fetchedBytes int64) error {
		mu.Lock()
		stats.ObjectsFetched += n
		stats.BytesFetched += fetchedBytes
		done, total := stats.ObjectsFetched, stats.BytesFetched
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(done, 0)
		}
		if opts.MaxBytes > 0 && total > opts.MaxBytes {
			return fmt.Errorf("%w: %d bytes fetched, limit %d", ErrTooLarge, total, opts.MaxBytes)
		}
		return nil
	}

	// Already complete locally: no round trip at all, the common case for
	// re-ensuring a tree whose chunks are shared with live paths.
	if missing, err := localMissing(root, store); err != nil {
		return stats, err
	} else if len(missing) == 0 {
		return stats, nil
	}

	// Bulk phase: fetch everything the server says is reachable that we lack.
	serverKeys, err := src.ReachableKeys(ctx, root)
	if err != nil {
		return stats, fmt.Errorf("listing reachable keys: %w", err)
	}
	toFetch, err := store.Missing(serverKeys)
	if err != nil {
		return stats, fmt.Errorf("checking the reachable set against the local store: %w", err)
	}

	// The first iteration bulk-fetches. Later ones recover stragglers the
	// server's list omitted or another pull left in flight.
	for {
		before := stats.ObjectsFetched
		if len(toFetch) > 0 {
			if err := fetchAll(ctx, store, src, toFetch, opts, settle); err != nil {
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
		if stats.ObjectsFetched == before {
			// Every remaining key is in flight on another pull: wait for
			// its ingest (or failure) instead of spinning.
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

// fetchAll downloads keys as byte-balanced packs in parallel, verifying and
// storing each pack against its keys.
func fetchAll(ctx context.Context, store *packstore.Store, src Source, keys []key.Key, opts Opts, settle func(n int, fetchedBytes int64) error) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.jobs())
	rs, _ := src.(RecordSource)
	for _, batch := range Batches(keys, opts.batchBytes(), PullSizer()) {
		g.Go(func() error {
			batch := inflight.claim(store, batch)
			if len(batch) == 0 {
				return nil
			}
			defer inflight.release(store, batch)
			// Re-check after claiming: another pull may have ingested some
			// of these keys since this pull's missing list was computed.
			batch, err := store.Missing(batch)
			if err != nil || len(batch) == 0 {
				return err
			}
			if rs != nil {
				return fetchRecordBatch(gctx, store, rs, batch, opts, settle)
			}
			return fetchObjectBatch(gctx, store, src, batch, opts, settle)
		})
	}
	return g.Wait()
}

func fetchObjectBatch(ctx context.Context, store *packstore.Store, src Source, batch []key.Key, opts Opts, settle func(n int, fetchedBytes int64) error) error {
	objs, err := src.FetchObjects(ctx, batch, opts.OnBytes)
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
	if err := settle(len(objs), fetchedBytes); err != nil {
		return err
	}
	return checkDelivered(batch, len(objs))
}

// Servers answer all-or-nothing; a short answer would otherwise be
// retried forever by the fixpoint loop.
func checkDelivered(batch []key.Key, got int) error {
	if got < len(batch) {
		return fmt.Errorf("remotesync: source delivered %d of %d requested objects", got, len(batch))
	}
	return nil
}

func fetchRecordBatch(ctx context.Context, store *packstore.Store, rs RecordSource, batch []key.Key, opts Opts, settle func(n int, fetchedBytes int64) error) error {
	var n int
	var fetchedBytes int64
	seq := func(yield func(amberpack.RawRecord, error) bool) {
		err := rs.StreamRecords(ctx, batch, opts.OnBytes, func(r amberpack.RawRecord) error {
			n++
			fetchedBytes += int64(len(r.Bytes))
			if !yield(r, nil) {
				return errStopStream
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopStream) {
			yield(amberpack.RawRecord{}, err)
		}
	}
	// An aborted stream leaves only verified records behind, which a
	// retry from another source deduplicates.
	recycle := func(r amberpack.RawRecord) { amberpack.PutBuf(r.Bytes) }
	if _, err := store.WriteRecords(seq, packstore.WriteOpts{}, recycle); err != nil {
		return fmt.Errorf("storing fetched records: %w", err)
	}
	if err := settle(n, fetchedBytes); err != nil {
		return err
	}
	return checkDelivered(batch, n)
}

// localMissing returns keys reachable from root but absent locally. An
// absent node is collected, not descended, since its children are unknown
// until fetched. Blob and XattrSet keys are leaves.
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
