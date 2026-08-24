// Reference operations: the PUT/DELETE sequence of architecture/simple-gc.md
// §"Writing a reference", packaged for the daemon, server and embedded
// handlers so each performs the identical closure bookkeeping. The sequence
// spans several refstore calls (read the old record, prepare, put, release),
// so calls are serialized per name here — concurrent handlers for one name
// must not interleave their read-old/commit/release steps or the union's
// refcounts drift.

package gc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// nameLocks serializes reference operations per name without holding a
// global lock across a closure walk (minutes on a large root).
type nameLocks struct {
	mu    sync.Mutex
	locks map[string]*nameLock
}

type nameLock struct {
	mu sync.Mutex
	n  int // waiters + holder; the entry is removed when it reaches 0
}

func (nl *nameLocks) lock(name string) (unlock func()) {
	nl.mu.Lock()
	if nl.locks == nil {
		nl.locks = make(map[string]*nameLock)
	}
	l := nl.locks[name]
	if l == nil {
		l = &nameLock{}
		nl.locks[name] = l
	}
	l.n++
	nl.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		nl.mu.Lock()
		l.n--
		if l.n == 0 {
			delete(nl.locks, name)
		}
		nl.mu.Unlock()
	}
}

// PutRef writes the reference record raw (already validated and, where the
// caller requires it, signature-checked) under name, pointing at root,
// through the removal lock: the closure is reused or walked — a missing
// object fails the write with an error naming it (the caller's 404; the
// optimistic PUT) — the record is committed, and an overwritten root is
// released. Calls for one name are serialized against each other and
// against DeleteRef.
func (c *Collector) PutRef(name string, root key.Key, raw []byte) error {
	unlock := c.names.lock(name)
	defer unlock()
	var old *key.Key
	if prev, err := c.refs.Get(name); err == nil {
		prevRef, err := reference.Decode(prev)
		if err != nil {
			return fmt.Errorf("gc: existing reference %q: %w", name, err)
		}
		k, err := key.Parse(prevRef.Key)
		if err != nil {
			return fmt.Errorf("gc: existing reference %q: %w", name, err)
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}
	commit, abort, err := c.PrepareRef(root)
	if err != nil {
		return err
	}
	if err := c.refs.Put(name, raw); err != nil {
		abort()
		return err
	}
	commit()
	if old != nil {
		return c.ReleaseRef(*old)
	}
	return nil
}

// DeleteRef removes the reference and releases its root: the tails leave
// the union; the closure file goes if no other name shares the root. No
// walk. A missing name returns refstore.ErrNotFound (the caller's 404).
func (c *Collector) DeleteRef(name string) error {
	unlock := c.names.lock(name)
	defer unlock()
	prev, err := c.refs.Get(name)
	if err != nil {
		return err
	}
	ref, err := reference.Decode(prev)
	if err != nil {
		return fmt.Errorf("gc: reference %q: %w", name, err)
	}
	root, err := key.Parse(ref.Key)
	if err != nil {
		return fmt.Errorf("gc: reference %q: %w", name, err)
	}
	if err := c.refs.Delete(name); err != nil {
		return err
	}
	return c.ReleaseRef(root)
}
