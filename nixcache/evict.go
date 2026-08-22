package nixcache

import "sync"

// evictPolicy is an S3-FIFO over indexed paths, budgeted by NarSize
// (exclusive sizes are unknowable under dedup): a probationary FIFO at
// ~10% of budget filters one-hit wonders, a ghost FIFO readmits recent
// evictions straight to main, pinned paths are never victims. Mistakes
// cost bandwidth only: the NAR route repopulates on demand.
type evictPolicy struct {
	mu      sync.Mutex
	budget  int64
	small   fifo
	main    fifo
	byPath  map[string]*evictEntry
	pinned  map[string]bool
	ghost   []string
	ghosted map[string]bool
	used    int64
}

type evictEntry struct {
	hashpart string
	size     int64
	freq     uint8
}

type fifo struct {
	q    []*evictEntry
	head int
	size int64
}

func (f *fifo) push(e *evictEntry) {
	if f.head > 0 && len(f.q) == cap(f.q) {
		n := copy(f.q, f.q[f.head:])
		clear(f.q[n:])
		f.q, f.head = f.q[:n], 0
	}
	f.q = append(f.q, e)
	f.size += e.size
}

func (f *fifo) pop() *evictEntry {
	if f.head == len(f.q) {
		return nil
	}
	e := f.q[f.head]
	f.q[f.head] = nil
	f.head++
	f.size -= e.size
	if f.head == len(f.q) {
		f.q, f.head = f.q[:0], 0
	}
	return e
}

func newEvictPolicy(budget int64) *evictPolicy {
	return &evictPolicy{
		budget:  budget,
		byPath:  map[string]*evictEntry{},
		pinned:  map[string]bool{},
		ghosted: map[string]bool{},
	}
}

func (p *evictPolicy) insert(hashpart string, narSize uint64, probation bool) {
	if p.byPath[hashpart] != nil {
		return
	}
	e := &evictEntry{hashpart: hashpart, size: int64(narSize)}
	p.byPath[hashpart] = e
	p.used += e.size
	if probation {
		p.small.push(e)
	} else {
		p.main.push(e)
	}
}

// seed registers a path that survived a restart. It goes straight to
// main: queue positions and hit counters are not persisted, and treating
// survivors as probationary would let one restart flush the cache.
func (p *evictPolicy) seed(hashpart string, narSize uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.insert(hashpart, narSize, false)
}

// Admit registers an indexed path. Ghost hits skip probation.
func (p *evictPolicy) Admit(hashpart string, narSize uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.insert(hashpart, narSize, !p.ghosted[hashpart])
}

// Touch records a hit. Never reorders, so scans stay cheap.
func (p *evictPolicy) Touch(hashpart string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.byPath[hashpart]; e != nil && e.freq < 3 {
		e.freq++
	}
}

func (p *evictPolicy) Pin(hashpart string)   { p.setPin(hashpart, true) }
func (p *evictPolicy) Unpin(hashpart string) { p.setPin(hashpart, false) }

func (p *evictPolicy) setPin(hashpart string, v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if v {
		p.pinned[hashpart] = true
	} else {
		delete(p.pinned, hashpart)
	}
}

// Evict pops victims until the budget holds, returning their hashparts
// (oldest first) and NarSize sum. It returns nothing when the budget
// already holds or only pinned paths remain.
func (p *evictPolicy) Evict() (victims []string, bytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.used > p.budget {
		e := p.evictOne()
		if e == nil {
			break
		}
		delete(p.byPath, e.hashpart)
		p.used -= e.size
		p.ghostAdd(e.hashpart)
		victims = append(victims, e.hashpart)
		bytes += e.size
	}
	return victims, bytes
}

// evictOne prefers probationary paths while small exceeds its share,
// then main, then any unpinned path. All scans are bounded, so
// exhaustion returns nil instead of spinning on pinned entries.
func (p *evictPolicy) evictOne() *evictEntry {
	if e := p.fromSmall(false); e != nil {
		return e
	}
	if e := p.fromMain(); e != nil {
		return e
	}
	return p.fromSmall(true)
}

// fromSmall pops the oldest untouched probationary path, promoting
// touched ones to main. Without force it stops once small is within its
// share. With force (main exhausted) it takes any unpinned path.
func (p *evictPolicy) fromSmall(force bool) *evictEntry {
	for scans := len(p.small.q) - p.small.head; scans > 0; scans-- {
		if !force && p.small.size <= p.budget/10 {
			return nil
		}
		e := p.small.pop()
		switch {
		case p.pinned[e.hashpart]:
			p.small.push(e)
		case !force && e.freq > 0:
			e.freq = 0
			p.main.push(e)
		default:
			return e
		}
	}
	return nil
}

// fromMain rotates until a path with a zero counter surfaces,
// decrementing as it goes. Four passes suffice: the counter caps at 3.
func (p *evictPolicy) fromMain() *evictEntry {
	for scans := 4 * (len(p.main.q) - p.main.head); scans > 0; scans-- {
		e := p.main.pop()
		switch {
		case p.pinned[e.hashpart]:
			p.main.push(e)
		case e.freq > 0:
			e.freq--
			p.main.push(e)
		default:
			return e
		}
	}
	return nil
}

func (p *evictPolicy) ghostAdd(hashpart string) {
	limit := max(len(p.byPath), 256)
	for len(p.ghost) >= limit {
		delete(p.ghosted, p.ghost[0])
		p.ghost = p.ghost[1:]
	}
	p.ghost = append(p.ghost, hashpart)
	p.ghosted[hashpart] = true
}
