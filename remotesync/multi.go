package remotesync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// Stats holds per-source throughput measurements. Shared across pulls so
// only a source's first batch ever is scheduled blind.
type Stats struct {
	mu   sync.Mutex
	cond *sync.Cond
	m    map[any]*srcStat
}

type srcStat struct {
	rateEwma float64       // bytes/s over recent attempts; 0 = unmeasured
	busyInt  time.Duration // integral of inflight over time
	last     time.Time
	inflight int
	fails    int
	lastFail time.Time
}

func (st *srcStat) tick(now time.Time) {
	if !st.last.IsZero() {
		st.busyInt += time.Duration(st.inflight) * now.Sub(st.last)
	}
	st.last = now
}

func (st *srcStat) backoffEnd() time.Time {
	if st.fails == 0 {
		return time.Time{}
	}
	d := min(time.Second<<min(st.fails-1, 8), 5*time.Minute)
	return st.lastFail.Add(d)
}

func (st *srcStat) backedOff(now time.Time) bool {
	return now.Before(st.backoffEnd())
}

func NewStats() *Stats {
	s := &Stats{m: map[any]*srcStat{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Stats) get(k any) *srcStat {
	st, ok := s.m[k]
	if !ok {
		st = &srcStat{}
		s.m[k] = st
	}
	return st
}

// acquire picks the source with the lowest (inflight+1)/rate. An
// unmeasured source may hold one probe batch. If no source is eligible,
// acquire waits for a release, a backoff expiry, or ctx.
func (s *Stats) acquire(ctx context.Context, keys []any) (int, time.Duration, error) {
	if len(keys) == 0 {
		return -1, 0, errors.New("remotesync: no sources")
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cond.Broadcast()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return -1, 0, err
		}
		now := time.Now()
		best, bestETA := -1, 0.0
		var wake time.Time
		for i, k := range keys {
			st := s.get(k)
			if end := st.backoffEnd(); now.Before(end) {
				if wake.IsZero() || end.Before(wake) {
					wake = end
				}
				continue
			}
			r := st.rateEwma
			if r == 0 {
				if st.inflight > 0 {
					continue
				}
				best = i // probe
				break
			}
			eta := float64(st.inflight+1) / r
			if best == -1 || eta < bestETA {
				best, bestETA = i, eta
			}
		}
		if best >= 0 {
			return best, s.claimLocked(keys[best]), nil
		}
		if !wake.IsZero() {
			t := time.AfterFunc(time.Until(wake), func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.cond.Broadcast()
			})
			s.cond.Wait()
			t.Stop()
		} else {
			s.cond.Wait()
		}
	}
}

func (s *Stats) claim(k any) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimLocked(k)
}

func (s *Stats) claimLocked(k any) time.Duration {
	st := s.get(k)
	st.tick(time.Now())
	st.inflight++
	return st.busyInt
}

// release folds an attempt's normalized rate into the estimate;
// failures back the source off exponentially.
func (s *Stats) release(k any, busyStart time.Duration, got int64, elapsed time.Duration, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(k)
	st.tick(time.Now())
	st.inflight--
	if r := normRate(st, busyStart, got, elapsed); r > 0 {
		if st.rateEwma == 0 {
			st.rateEwma = r
		} else {
			st.rateEwma += (r - st.rateEwma) * 0.3
		}
	}
	if failed {
		st.fails++
		st.lastFail = time.Now()
	} else {
		st.fails = 0
	}
	s.cond.Broadcast()
}

// probeStalled reports whether an attempt runs far below the best rate
// on offer (rate over queue depth, so aborts self-disarm when every
// alternative is loaded).
func (s *Stats) probeStalled(keys []any, k any, busyStart time.Duration, got int64, elapsed time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best float64
	for _, key := range keys {
		st := s.get(key)
		if st.backedOff(time.Now()) {
			continue
		}
		if r := st.rateEwma / float64(st.inflight+1); r > best {
			best = r
		}
	}
	st := s.get(k)
	st.tick(time.Now())
	return best > 0 && normRate(st, busyStart, got, elapsed) < best/4
}

// normRate is an attempt's throughput times the average concurrency it
// ran under. The elapsed floor keeps token-bucket bursts from producing
// phantom rates.
func normRate(st *srcStat, busyStart time.Duration, got int64, elapsed time.Duration) float64 {
	e := max(elapsed, 500*time.Millisecond)
	avg := max(float64(st.busyInt-busyStart)/float64(e), 1)
	return float64(got) / e.Seconds() * avg
}

// failoverOrder ranks the remaining sources fastest-first.
func (s *Stats) failoverOrder(keys []any, skip int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var order []int
	for i := range keys {
		if i != skip {
			order = append(order, i)
		}
	}
	rate := func(i int) float64 { return s.get(keys[i]).rateEwma }
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && rate(order[j]) > rate(order[j-1]); j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	return order
}

func (s *Stats) unmeasured(k any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(k).rateEwma == 0
}

// MultiSource stripes Pull's batches across sources of the same content,
// scheduling each batch on the source expected to finish soonest. A
// failed batch fails over. Pull re-verifies every object regardless.
type MultiSource struct {
	sources []Source
	keys    []any
	stats   *Stats
}

// NewMultiSource schedules with fresh, pull-local measurements.
func NewMultiSource(sources ...Source) *MultiSource {
	keys := make([]any, len(sources))
	for i := range keys {
		keys[i] = i
	}
	return &MultiSource{sources: sources, keys: keys, stats: NewStats()}
}

// NewMultiSourceKeyed schedules with measurements persisted in stats
// under the given per-source keys.
func NewMultiSourceKeyed(stats *Stats, keys []any, sources []Source) *MultiSource {
	return &MultiSource{sources: sources, keys: keys, stats: stats}
}

// ReachableKeys asks each source in turn until one answers.
func (m *MultiSource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	if len(m.sources) == 0 {
		return nil, errors.New("remotesync: no sources")
	}
	var errs []error
	for _, s := range m.sources {
		keys, err := s.ReachableKeys(ctx, root)
		if err == nil {
			return keys, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

const abortFloor = 750 * time.Millisecond

// attempt runs try on source i. Only probes of unmeasured sources are
// abortable, and only with failover left: established sources are
// ranked and backed off, never killed mid-batch.
func (m *MultiSource) attempt(ctx context.Context, i int, busyStart time.Duration, abortable bool, try func(ctx context.Context, i int, onBytes func(int)) error) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	k := m.keys[i]
	var got atomic.Int64
	start := time.Now()
	if abortable && m.stats.unmeasured(k) {
		go func() {
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-wctx.Done():
					return
				case <-t.C:
					elapsed := time.Since(start)
					if elapsed >= abortFloor &&
						m.stats.probeStalled(m.keys, k, busyStart, got.Load(), elapsed) {
						cancel()
						return
					}
				}
			}
		}()
	}
	err := try(wctx, i, func(n int) { got.Add(int64(n)) })
	m.stats.release(k, busyStart, got.Load(), time.Since(start), err != nil && !errors.Is(err, errStopStream))
	return err
}

func (m *MultiSource) fetch(ctx context.Context, try func(ctx context.Context, i int, onBytes func(int)) error) error {
	first, busyStart, err := m.stats.acquire(ctx, m.keys)
	if err != nil {
		return err
	}
	err = m.attempt(ctx, first, busyStart, len(m.sources) > 1, try)
	if err == nil || errors.Is(err, errStopStream) {
		return err
	}
	errs := []error{err}
	rest := m.stats.failoverOrder(m.keys, first)
	for n, i := range rest {
		err := m.attempt(ctx, i, m.stats.claim(m.keys[i]), n < len(rest)-1, try)
		if err == nil || errors.Is(err, errStopStream) {
			return err
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *MultiSource) FetchObjects(ctx context.Context, keys []key.Key, onBytes func(n int)) ([]fstree.Object, error) {
	var objs []fstree.Object
	err := m.fetch(ctx, func(ctx context.Context, i int, ob func(int)) error {
		var err error
		objs, err = m.sources[i].FetchObjects(ctx, keys, join(ob, onBytes))
		return err
	})
	return objs, err
}

// StreamRecords stripes like FetchObjects. A source without record
// support counts as failed for the batch.
func (m *MultiSource) StreamRecords(ctx context.Context, keys []key.Key, onBytes func(n int), emit func(amberpack.RawRecord) error) error {
	return m.fetch(ctx, func(ctx context.Context, i int, ob func(int)) error {
		rs, ok := m.sources[i].(RecordSource)
		if !ok {
			return fmt.Errorf("source %d: no record support", i)
		}
		return rs.StreamRecords(ctx, keys, join(ob, onBytes), emit)
	})
}

func join(a, b func(int)) func(int) {
	if b == nil {
		return a
	}
	return func(n int) { a(n); b(n) }
}
