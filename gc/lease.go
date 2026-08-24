// Upload leases: objects arrive long before their reference. The inbox (in
// the daemon) takes one per root on its first pack, refreshes it per pack,
// releases it on the reference PUT; a pull holds one for its root. A lease
// expires Grace after its last refresh; the horizon never passes the start
// of a live lease.

package gc

import (
	"time"

	"github.com/draganm/amber-store/key"
)

// A Lease covers one root's in-flight upload against the horizon.
type Lease struct {
	c     *Collector
	root  key.Key
	start time.Time
	last  time.Time
}

// Lease takes an upload lease for root.
func (c *Collector) Lease(root key.Key) *Lease {
	now := time.Now()
	l := &Lease{c: c, root: root, start: now, last: now}
	c.mu.Lock()
	c.leases[l] = true
	c.mu.Unlock()
	return l
}

// Refresh marks the upload alive; call it as packs keep arriving.
func (l *Lease) Refresh() {
	l.c.mu.Lock()
	l.last = time.Now()
	l.c.mu.Unlock()
}

// Release drops the lease; call it when the reference PUT lands (or the
// upload is abandoned — expiry would get it eventually).
func (l *Lease) Release() {
	l.c.mu.Lock()
	delete(l.c.leases, l)
	l.c.mu.Unlock()
}

// horizon is the pack-eligibility cutoff: min(now − grace, start of the
// oldest in-flight write, start of the oldest live upload lease). Only
// packs sealed before it can be reaped. Expired leases are dropped here.
func (c *Collector) horizon(now time.Time) time.Time {
	h := now.Add(-c.opts.Grace)
	if t, ok := c.objects.OldestInflightWrite(); ok && t.Before(h) {
		h = t
	}
	c.mu.Lock()
	for l := range c.leases {
		if now.Sub(l.last) > c.opts.Grace {
			delete(c.leases, l)
			continue
		}
		if l.start.Before(h) {
			h = l.start
		}
	}
	c.mu.Unlock()
	return h
}
