package remotesync

import (
	"sync"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// The registry lets concurrent pulls skip keys another pull is already
// fetching. Safe because the straggler re-walk fetches anything still
// missing, so a dead claimant delays a chunk, never loses it.
var inflight = inflightSet{m: map[inflightKey]struct{}{}}

type inflightKey struct {
	store *packstore.Store
	k     key.Key
}

type inflightSet struct {
	mu sync.Mutex
	m  map[inflightKey]struct{}
}

// claim marks and returns the keys no other pull holds in flight.
func (s *inflightSet) claim(store *packstore.Store, keys []key.Key) []key.Key {
	s.mu.Lock()
	defer s.mu.Unlock()
	claimed := keys[:0:0]
	for _, k := range keys {
		ik := inflightKey{store, k}
		if _, held := s.m[ik]; held {
			continue
		}
		s.m[ik] = struct{}{}
		claimed = append(claimed, k)
	}
	return claimed
}

func (s *inflightSet) release(store *packstore.Store, keys []key.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.m, inflightKey{store, k})
	}
}
