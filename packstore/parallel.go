package packstore

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize is the byte threshold at which a writer fsyncs the active
// segment, making everything appended so far durable.
const DefaultBatchSize = 16 << 20 // 16 MiB

// WriteStats summarizes one WriteParallel run.
// On a non-nil error, the stats reflect the work done before the abort.
type WriteStats struct {
	Stored      int   // objects newly written
	Deduped     int   // objects skipped (already present, or duplicated in the stream)
	BytesStored int64 // payload bytes of newly-written objects (uncompressed)
}

// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int  // concurrent writers; <= 0 means GOMAXPROCS
	BatchSize int  // fsync when a writer has appended this many bytes; <= 0 means DefaultBatchSize
	Verify    bool // recompute and check each new object's key before storing it
}

// WriteParallel stores every object the iterator yields using multiple
// concurrent workers. Compression and (optional) verification run in
// parallel; appends serialize on the active segment. Each worker fsyncs after
// appending BatchSize bytes and once more when the input is exhausted.
//
// Like WriteBatch, WriteParallel is durable-on-return but NOT atomic: on
// error or crash a valid prefix remains, which a content-addressed re-run
// deduplicates (a dedup hit against a record appended by a concurrent,
// uncommitted run rides on that run's eventual fsync). If the iterator yields
// an error, WriteParallel stops and returns it. With opts.Verify, a
// key/payload mismatch stops the run with a wrapped ErrVerify.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error) {
	writers := opts.Writers
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan Object, writers*2)
	seen := newSeenSet()
	eg := &errgroup.Group{}
	var stored, deduped, bytesStored atomic.Int64

	// Distributor: forward objects from the iterator to the worker pool. A
	// yielded error cancels the run and propagates as the group's error.
	eg.Go(func() error {
		defer close(ch)
		for obj, err := range seq {
			if err != nil {
				cancel()
				return err
			}
			select {
			case ch <- obj:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	for range writers {
		eg.Go(func() error {
			err := s.runWriter(ctx, ch, seen, batchSize, opts.Verify, &stored, &deduped, &bytesStored)
			if err != nil {
				cancel() // stop the distributor and sibling workers
			}
			return err
		})
	}

	err := eg.Wait()
	if err != nil && stored.Load() > 0 {
		// Mirror WriteBatch's error-path contract: records appended before
		// the error are Has-visible, so they must not stay non-durable.
		// Best-effort; an fsync failure poisons the store via setFailed.
		s.syncActive()
	}
	return WriteStats{
		Stored:      int(stored.Load()),
		Deduped:     int(deduped.Load()),
		BytesStored: bytesStored.Load(),
	}, err
}

// runWriter consumes objects, encoding (compressing, optionally verifying)
// them concurrently with its siblings and appending them to the store. It
// fsyncs after batchSize appended bytes and once more when the channel
// closes. On ctx cancellation it returns without flushing; WriteParallel
// issues a final best-effort fsync for the whole run's appends (the segment
// file is shared, so one sync covers every worker).
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int, verify bool, stored, deduped, bytesStored *atomic.Int64) error {
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		pending = 0
		return s.syncActive()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case obj, ok := <-ch:
			if !ok {
				return flush()
			}
			if !seen.addIfAbsent(obj.Key) {
				deduped.Add(1)
				continue
			}
			has, err := s.Has(obj.Key)
			if err != nil {
				return err
			}
			if has {
				deduped.Add(1)
				continue
			}
			if verify {
				if err := verifyObject(obj); err != nil {
					return err
				}
			}
			rec, err := amberpack.EncodeRecord(obj.Key, obj.Data)
			if err != nil {
				return err
			}
			if err := s.append(obj.Key, rec, false); err != nil {
				return err
			}
			stored.Add(1)
			bytesStored.Add(int64(len(obj.Data)))
			pending += len(rec)
			if pending >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// seenSet is a concurrency-safe set of keys, sharded on the key's last byte
// (uniformly distributed) to spread lock contention across writers.
type seenSet struct {
	shards [256]seenShard
}

type seenShard struct {
	mu sync.Mutex
	m  map[key.Key]struct{}
}

func newSeenSet() *seenSet {
	s := &seenSet{}
	for i := range s.shards {
		s.shards[i].m = make(map[key.Key]struct{})
	}
	return s
}

// addIfAbsent records k and reports true if it was not already present.
func (s *seenSet) addIfAbsent(k key.Key) bool {
	sh := &s.shards[k[key.Size-1]]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[k]; ok {
		return false
	}
	sh.m[k] = struct{}{}
	return true
}
