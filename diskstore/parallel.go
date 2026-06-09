package diskstore

import (
	"context"
	"iter"
	"runtime"
	"sync"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize is the byte threshold at which a writer commits its current
// Pebble batch and starts a fresh one.
const DefaultBatchSize = 16 << 20 // 16 MiB

// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int // concurrent batch writers; <= 0 means GOMAXPROCS
	BatchSize int // commit when a batch reaches this many bytes; <= 0 means DefaultBatchSize
}

// WriteParallel stores every object the iterator yields using multiple
// concurrent batch writers. Each writer accumulates objects into its own Pebble
// batch and commits when the batch reaches BatchSize bytes (and once more when
// the input is exhausted); commits run concurrently. External (large) objects
// are written to disk durably before the commit that references them. Objects
// already in the store, or already seen earlier in this run, are skipped.
//
// Unlike WriteBatch, WriteParallel is NOT atomic: on error or crash, objects
// from already-committed batches remain alongside harmless orphan blob files.
// Because the store is content-addressed and idempotent, a re-run converges. If
// the iterator yields an error, WriteParallel stops and returns it.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) error {
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

	// Distributor: forward objects from the iterator to the writer pool. It does
	// no Has/commit work, so it never bottlenecks. A yielded error cancels the
	// run and propagates as the group's error.
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
			err := s.runWriter(ctx, ch, seen, batchSize)
			if err != nil {
				cancel() // stop the distributor and sibling writers
			}
			return err
		})
	}

	return eg.Wait()
}

// runWriter consumes objects, accumulating them into a Pebble batch it commits
// when the batch reaches batchSize bytes and once more when the channel closes.
// On ctx cancellation it returns without committing (the run is being aborted;
// partial commits are safe in a content-addressed store).
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int) (err error) {
	b := s.db.NewBatch()
	defer func() {
		if b != nil {
			b.Close()
		}
	}()
	commit := func() error {
		if b.Empty() {
			return nil
		}
		if err := b.Commit(s.writeOpts); err != nil {
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case obj, ok := <-ch:
			if !ok {
				return commit()
			}
			if !seen.addIfAbsent(obj.Key) {
				continue
			}
			has, err := s.Has(obj.Key)
			if err != nil {
				return err
			}
			if has {
				continue
			}
			if len(obj.Data) > s.threshold {
				if err := s.writeExternal(obj.Key, obj.Data); err != nil {
					return err
				}
				if err := b.Set(obj.Key[:], []byte{tagExternal}, nil); err != nil {
					return err
				}
			} else {
				val := make([]byte, 1+len(obj.Data))
				val[0] = tagInline
				copy(val[1:], obj.Data)
				if err := b.Set(obj.Key[:], val, nil); err != nil {
					return err
				}
			}
			if b.Len() >= batchSize {
				if err := commit(); err != nil {
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
