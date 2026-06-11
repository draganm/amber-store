// Package nonces is the remote server's replay cache: a set of recently seen
// (key fingerprint, nonce) pairs. Entries are kept for twice the timestamp
// window — requests older than the window are rejected before the nonce
// check, so an expired entry can never be replayed successfully.
package nonces

import (
	"sync"
	"time"
)

// Cache remembers nonces for 2× the timestamp window. Safe for concurrent use.
type Cache struct {
	mu     sync.Mutex
	window time.Duration
	seen   map[string]time.Time // (id + "\x00" + nonce) → insertion time
}

// New returns a Cache for the given timestamp window.
func New(window time.Duration) *Cache {
	return &Cache{window: window, seen: map[string]time.Time{}}
}

// SeenBefore reports whether (id, nonce) was already recorded within the
// retention period, recording it if not. id should identify the signing key
// (e.g. its fingerprint).
func (c *Cache) SeenBefore(id string, nonce []byte, now time.Time) bool {
	k := id + "\x00" + string(nonce)
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := now.Add(-2 * c.window)
	// O(entries) sweep on every call. Entries are bounded by request rate ×
	// window, and the remote protocol's request rate is batch-granular — fine
	// for v1. This is simpler than a priority queue and avoids background
	// goroutines.
	for ek, et := range c.seen {
		if et.Before(cutoff) {
			delete(c.seen, ek)
		}
	}
	if t, ok := c.seen[k]; ok && !t.Before(cutoff) {
		return true
	}
	c.seen[k] = now
	return false
}
