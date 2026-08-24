package gc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// Collector implements the cycle and the reference hooks over an open
// packstore and refstore pair. Single-owner, like the stores it sits next
// to; close it before them.
type Collector struct {
	objects *packstore.Store
	refs    *refstore.Store
	d       *closureDir
	opts    Options

	// The removal lock: a reference PUT holds it shared from its first
	// read to its commit; pack deletion holds it exclusively around the
	// re-test and the probe-list swap.
	removal sync.RWMutex

	mu      sync.Mutex // guards everything below, and union swaps
	union   atomic.Pointer[union]
	roots   map[key.Key]int  // root -> number of reference names
	pending map[key.Key]bool // named roots with no valid closure yet; walked before the first cycle
	walking map[key.Key]int  // in-flight PrepareRef walks; the closure file survives while nonzero
	leases  map[*Lease]bool
	last    *CycleStats
	lastErr error
	// Cycle-skip bookkeeping: a cycle is skipped when the union, the
	// eligible set and the line all match the previous completed cycle.
	haveLast      bool
	lastGen       uint64
	lastEligible  []uint64
	lastThreshold float64
	cancelCycle   context.CancelFunc

	cycleMu sync.Mutex // held for the whole cycle; cycles never overlap

	stop context.CancelFunc // background loop
	done chan struct{}
}

// Open opens the collector whose closures live in dir (<store>/closures),
// next to an already-open packstore and refstore. It sweeps tmp/, deletes
// closures no reference names, and builds the union from the closures of
// the references that exist; a named root without a valid closure is
// walked before the first cycle (or by the reference write that touches
// it). With opts.Interval > 0 a goroutine runs a cycle per interval until
// Close.
func Open(dir string, objects *packstore.Store, refs *refstore.Store, opts Options) (*Collector, error) {
	opts = opts.withDefaults()
	d, err := openClosureDir(dir, !opts.NoSync)
	if err != nil {
		return nil, err
	}
	recs, err := refs.All()
	if err != nil {
		d.close()
		return nil, err
	}
	roots := make(map[key.Key]int, len(recs))
	for _, r := range recs {
		ref, err := reference.Decode(r.Data)
		if err != nil {
			d.close()
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		root, err := key.Parse(ref.Key)
		if err != nil {
			d.close()
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		roots[root]++
	}
	onDisk, err := d.list()
	if err != nil {
		d.close()
		return nil, err
	}
	for _, root := range onDisk {
		if roots[root] == 0 {
			if err := d.remove(root); err != nil {
				d.close()
				return nil, err
			}
		}
	}
	pending := make(map[key.Key]bool)
	var pairs []tailCount
	for root, n := range roots {
		tails, ok := d.read(root)
		if !ok {
			pending[root] = true
			continue
		}
		for _, t := range tails {
			pairs = append(pairs, tailCount{t, uint32(n)})
		}
	}
	c := &Collector{
		objects: objects,
		refs:    refs,
		d:       d,
		opts:    opts,
		roots:   roots,
		pending: pending,
		walking: make(map[key.Key]int),
		leases:  make(map[*Lease]bool),
	}
	c.union.Store(buildUnion(pairs))
	if opts.Interval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		c.stop = cancel
		c.done = make(chan struct{})
		go c.loop(ctx)
	}
	return c, nil
}

// Close stops the background loop, waits out a running cycle and releases
// the closure directory. Close the collector before the stores under it.
func (c *Collector) Close() error {
	if c.stop != nil {
		c.stop()
		<-c.done
	}
	c.mu.Lock()
	if c.cancelCycle != nil {
		c.cancelCycle()
	}
	c.mu.Unlock()
	c.cycleMu.Lock()
	c.cycleMu.Unlock() //nolint:staticcheck // barrier: wait out a running cycle
	return c.d.close()
}

// Wipe cancels a running cycle and empties closures/ and the union. It
// accompanies packstore.Wipe and refstore.Wipe, which the caller runs
// first; afterwards the collector is empty but usable.
//
// walking is deliberately left untouched: its entries belong to PrepareRef
// calls currently walking a root (concurrent with this Wipe, racing the
// caller's packstore/refstore wipe), and clearing it here would let such a
// walk's cleanup underflow or delete a closure file out from under a
// different in-flight walk of the same root.
func (c *Collector) Wipe() error {
	c.mu.Lock()
	if c.cancelCycle != nil {
		c.cancelCycle()
	}
	c.mu.Unlock()
	c.cycleMu.Lock() // wait out the cancelled cycle
	defer c.cycleMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.roots = make(map[key.Key]int)
	c.pending = make(map[key.Key]bool)
	c.leases = make(map[*Lease]bool)
	c.union.Store(buildUnion(nil))
	c.last, c.lastErr = nil, nil
	c.haveLast = false
	return c.d.wipe()
}

// walk computes root's closure: the tails of every key CheckComplete
// visits. A missing object surfaces as the walk error — the caller's 404.
func (c *Collector) walk(root key.Key) ([]uint64, error) {
	keys, err := fstree.CheckComplete(root, c.objects.Get, c.objects.Has, c.opts.Jobs)
	if err != nil {
		return nil, fmt.Errorf("gc: walking root %s: %w", root, err)
	}
	return tailsOf(keys), nil
}

// ensureClosure returns root's tails, reusing a valid closure file or
// walking the tree and writing one.
func (c *Collector) ensureClosure(root key.Key) ([]uint64, error) {
	if tails, ok := c.d.read(root); ok {
		return tails, nil
	}
	tails, err := c.walk(root)
	if err != nil {
		return nil, err
	}
	if err := c.d.write(root, tails); err != nil {
		return nil, err
	}
	return tails, nil
}

// PrepareRef readies a reference PUT naming root under the removal lock,
// held shared until commit or abort: the closure is reused or walked (a
// missing object aborts with the error naming it), written durably, and
// merged into the union. Exactly one of commit (after the reference record
// is stored) or abort (it was not) must be called. On overwrite the caller
// then calls ReleaseRef with the old root — including when old and new are
// the same root.
func (c *Collector) PrepareRef(root key.Key) (commit, abort func(), err error) {
	c.removal.RLock()
	// Reserve the walk: while walking[root] is nonzero a concurrent
	// ReleaseRef of the last name keeps the closure file on disk, so the
	// read below cannot race a removal.
	c.mu.Lock()
	c.walking[root]++
	c.mu.Unlock()

	tails, err := c.ensureClosure(root)

	c.mu.Lock()
	c.walking[root]--
	if c.walking[root] == 0 {
		delete(c.walking, root)
	}
	if err != nil {
		if c.roots[root] == 0 && c.walking[root] == 0 {
			delete(c.pending, root)
			if rerr := c.d.remove(root); rerr != nil {
				// A stale closure that survives here lets a later PUT skip
				// its walk; the next Open sweeps it, but never be silent.
				c.lastErr = fmt.Errorf("gc: removing closure for released root %s: %w", root, rerr)
			}
		}
		c.mu.Unlock()
		c.removal.RUnlock()
		return nil, nil, err
	}
	delta := int32(1)
	if c.pending[root] {
		// The open-time union never saw this root's closure; fold the
		// existing names in now so a later release cannot zero it out.
		delta += int32(c.roots[root])
		delete(c.pending, root)
	}
	c.roots[root]++
	c.union.Store(c.union.Load().merge(tails, delta))
	c.mu.Unlock()
	var once sync.Once
	commit = func() {
		once.Do(c.removal.RUnlock)
	}
	abort = func() {
		once.Do(func() {
			c.mu.Lock()
			c.releaseLocked(root)
			c.mu.Unlock()
			c.removal.RUnlock()
		})
	}
	return commit, abort, nil
}

// ReleaseRef records that one reference naming root was deleted or
// overwritten: the root's tails are merged out of the union, and its
// closure file is deleted once no reference names it. No walk.
func (c *Collector) ReleaseRef(root key.Key) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releaseLocked(root)
}

func (c *Collector) releaseLocked(root key.Key) error {
	if c.roots[root] == 0 {
		return fmt.Errorf("gc: no reference names root %s", root)
	}
	c.roots[root]--
	if !c.pending[root] {
		if tails, ok := c.d.read(root); ok {
			c.union.Store(c.union.Load().merge(tails, -1))
		} else {
			// Bookkeeping hole: the union keeps this name's contribution
			// until a restart rebuilds it. Never silent.
			c.lastErr = fmt.Errorf("gc: closure for root %s unreadable at release; union over-counts until reopen", root)
		}
	}
	if c.roots[root] == 0 {
		delete(c.roots, root)
		delete(c.pending, root)
		if c.walking[root] == 0 {
			return c.d.remove(root)
		}
	}
	return nil
}

func (c *Collector) loop(ctx context.Context) {
	defer close(c.done)
	t := time.NewTicker(c.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.runTick(ctx)
		}
	}
}

// runTick is the background cycle entry.
func (c *Collector) runTick(ctx context.Context) {
	c.Run(ctx, -1) // skip logic inside; errors land in Status.LastError
}
