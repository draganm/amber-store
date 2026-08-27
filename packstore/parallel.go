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

// WriteParallel stores every yielded object with concurrent workers.
// Compression and optional verification run in parallel, appends
// serialize. Durable on return but NOT atomic, like WriteBatch: a crash
// leaves a valid, deduplicating prefix. With opts.Verify a key/payload
// mismatch stops the run with a wrapped ErrVerify.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error) {
	return writePipeline(s, seq, opts, keyOfObject, nil, func(obj Object, _ *[]byte) (recLen, dataLen int, err error) {
		if opts.Verify {
			if err := verifyObject(obj); err != nil {
				return 0, 0, err
			}
		}
		rec, err := amberpack.EncodeRecord(obj.Key, obj.Data)
		if err != nil {
			return 0, 0, err
		}
		return len(rec), len(obj.Data), s.append(obj.Key, rec, false)
	})
}

func keyOfObject(o Object) key.Key { return o.Key }

// writePipeline is the shared scaffold of WriteParallel and WriteRecords:
// a distributor feeding workers that dedup, ingest, and fsync every
// batchSize bytes plus once at the end. recycle, when non-nil, runs once
// per consumed item.
func writePipeline[T any](s *Store, seq iter.Seq2[T, error], opts WriteOpts, keyOf func(T) key.Key, recycle func(T), ingest func(T, *[]byte) (recLen, dataLen int, err error)) (WriteStats, error) {
	w := s.beginWrite()
	defer s.endWrite(w)
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

	ch := make(chan T, writers*2)
	seen := newSeenSet()
	eg := &errgroup.Group{}
	var stored, deduped, bytesStored atomic.Int64

	// Distributor: forward items from the iterator to the worker pool. A
	// yielded error cancels the run and propagates as the group's error.
	eg.Go(func() error {
		defer close(ch)
		for item, err := range seq {
			if err != nil {
				cancel()
				return err
			}
			select {
			case ch <- item:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	for range writers {
		eg.Go(func() error {
			err := runIngest(s, ctx, ch, seen, batchSize, keyOf, recycle, ingest, &stored, &deduped, &bytesStored)
			if err != nil {
				cancel() // stop the distributor and sibling workers
			}
			return err
		})
	}

	err := eg.Wait()
	// Always fsync, as WriteBatch does. Even with nothing appended a dedup
	// hit may have matched another run's unsynced record. On error the
	// appended prefix is Has-visible and must become durable too.
	if serr := s.syncActive(); err == nil {
		err = serr
	}
	return WriteStats{
		Stored:      int(stored.Load()),
		Deduped:     int(deduped.Load()),
		BytesStored: bytesStored.Load(),
	}, err
}

func runIngest[T any](s *Store, ctx context.Context, ch <-chan T, seen *seenSet, batchSize int, keyOf func(T) key.Key, recycle func(T), ingest func(T, *[]byte) (recLen, dataLen int, err error), stored, deduped, bytesStored *atomic.Int64) error {
	var scratch []byte
	pending := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case item, ok := <-ch:
			if !ok {
				return nil
			}
			k := keyOf(item)
			s.observe(k)
			if !seen.addIfAbsent(k) {
				deduped.Add(1)
				free(recycle, item)
				continue
			}
			has, err := s.Has(k)
			if err != nil {
				return err
			}
			if has {
				deduped.Add(1)
				free(recycle, item)
				continue
			}
			recLen, dataLen, err := ingest(item, &scratch)
			free(recycle, item)
			if err != nil {
				return err
			}
			stored.Add(1)
			bytesStored.Add(int64(dataLen))
			pending += recLen
			if pending >= batchSize {
				pending = 0
				if err := s.syncActive(); err != nil {
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

func free[T any](recycle func(T), item T) {
	if recycle != nil {
		recycle(item)
	}
}
