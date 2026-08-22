package packstore

import (
	"cmp"
	"slices"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
)

// recordSpan is a run of bytes in one segment covering one or more whole
// records. keys are kept for the per-record fallback when the segment is
// compacted away mid-stream.
type recordSpan struct {
	segID      uint64
	start, end uint64
	keys       []key.Key
	active     bool
}

// ViewRecordSpans calls fn with runs of record bytes covering keys in
// disk order. Adjacent records coalesce into spans up to maxSpan bytes.
// Spans are copied out of the mmap under the lock and handed to fn
// outside it, in one buffer reused across calls, so fn must not retain
// them. A span whose segment disappeared degrades to per-record reads.
func (s *Store) ViewRecordSpans(keys []key.Key, maxSpan int, fn func([]byte) error) error {
	spans, err := s.resolveSpans(keys, maxSpan)
	if err != nil {
		return err
	}
	var buf []byte
	for _, sp := range spans {
		if err := s.viewSpan(sp, &buf, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) resolveSpans(keys []key.Key, maxSpan int) ([]recordSpan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	spans := make([]recordSpan, 0, len(keys))
	for _, k := range keys {
		sp, err := s.resolveOne(k)
		if err != nil {
			return nil, err
		}
		spans = append(spans, sp)
	}
	slices.SortFunc(spans, func(a, b recordSpan) int {
		if c := cmp.Compare(a.segID, b.segID); c != 0 {
			return c
		}
		return cmp.Compare(a.start, b.start)
	})
	return coalesce(spans, uint64(maxSpan)), nil
}

func (s *Store) resolveOne(k key.Key) (recordSpan, error) {
	if s.active != nil {
		if loc, ok := s.active.index[k]; ok {
			return recordSpan{segID: s.active.id, active: true, keys: []key.Key{k},
				start: uint64(loc.off), end: uint64(loc.off) + uint64(amberpack.RecHeaderSize) + uint64(loc.slen)}, nil
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		g := s.sealed[i]
		start, end, ok, err := g.span(k)
		if err != nil {
			return recordSpan{}, err
		}
		if ok {
			return recordSpan{segID: g.id, keys: []key.Key{k}, start: start, end: end}, nil
		}
	}
	return recordSpan{}, ErrNotFound
}

func coalesce(spans []recordSpan, maxSpan uint64) []recordSpan {
	out := spans[:0]
	for _, sp := range spans {
		if n := len(out); n > 0 {
			prev := &out[n-1]
			if !prev.active && !sp.active && prev.segID == sp.segID &&
				prev.end == sp.start && sp.end-prev.start <= maxSpan {
				prev.end = sp.end
				prev.keys = append(prev.keys, sp.keys...)
				continue
			}
		}
		out = append(out, sp)
	}
	return out
}

func (s *Store) viewSpan(sp recordSpan, buf *[]byte, fn func([]byte) error) error {
	s.mu.RLock()
	if !s.closed && !sp.active {
		for i := len(s.sealed) - 1; i >= 0; i-- {
			if g := s.sealed[i]; g.id == sp.segID {
				*buf = append((*buf)[:0], g.mm[sp.start:sp.end]...)
				s.mu.RUnlock()
				return fn(*buf)
			}
		}
	}
	s.mu.RUnlock()
	// Active record, or the sealed segment was compacted away: the records
	// live elsewhere now, so read them one by one through the normal path.
	for _, k := range sp.keys {
		if err := s.ViewRecord(k, fn); err != nil {
			return err
		}
	}
	return nil
}
