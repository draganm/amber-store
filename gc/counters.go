package gc

// Counters is the cheap subset of Status — everything that needs no pack
// scoring — for metrics scraped on an interval (amber_gc_*).
type Counters struct {
	Refs     int // reference names
	Closures int // closure files on disk
	Pending  int // named roots without a valid closure yet
	Union    int // live tails
	Leases   int // live upload leases
	Last     *CycleStats
	LastErr  string
}

// Counters reports the collector's in-memory figures plus one directory
// listing; unlike Status it scores nothing, so it is safe per scrape.
func (c *Collector) Counters() Counters {
	var ct Counters
	ct.Union = c.union.Load().size()
	if onDisk, err := c.d.list(); err == nil {
		ct.Closures = len(onDisk)
	}
	c.mu.Lock()
	for _, n := range c.roots {
		ct.Refs += n
	}
	ct.Pending = len(c.pending)
	ct.Leases = len(c.leases)
	ct.Last = c.last
	if c.lastErr != nil {
		ct.LastErr = c.lastErr.Error()
	}
	c.mu.Unlock()
	return ct
}
