package gc

import (
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// testStore is an open packstore+refstore+closureDir triple in one temp dir.
type testStore struct {
	dir     string
	objects *packstore.Store
	refs    *refstore.Store
}

func newTestStore(t *testing.T, segSize int64) *testStore {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSegmentSize(segSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	return &testStore{dir: dir, objects: objects, refs: refs}
}

func (ts *testStore) openCollector(t *testing.T, opts Options) *Collector {
	t.Helper()
	c, err := Open(filepath.Join(ts.dir, "closures"), ts.objects, ts.refs, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// storeTree stores a FileNode root over n distinct incompressible 256-byte
// blobs derived from seed and returns the root and every key. Incompressible
// payloads keep on-disk sizes predictable so small segment sizes actually
// rotate.
func storeTree(t *testing.T, st *packstore.Store, seed string, n int) (key.Key, []key.Key) {
	t.Helper()
	base := uint64(crc32.ChecksumIEEE([]byte(seed)))
	var children []key.Key
	var all []key.Key
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewPCG(base, uint64(i)))
		data := make([]byte, 256)
		for j := range data {
			data[j] = byte(rng.UintN(256))
		}
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		children = append(children, o.Key)
		all = append(all, o.Key)
	}
	rootObj, err := fstree.EncodeFileNode(children)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	all = append(all, rootObj.Key)
	return rootObj.Key, all
}

// putRef writes a reference through the collector exactly as a CLI/daemon
// PUT does.
func putTestRef(t *testing.T, c *Collector, refs *refstore.Store, name string, root key.Key) {
	t.Helper()
	rec := reference.Reference{Name: name, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var old *key.Key
	if prev, err := refs.Get(name); err == nil {
		prevRef, err := reference.Decode(prev)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(prevRef.Key)
		if err != nil {
			t.Fatal(err)
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		t.Fatal(err)
	}
	commit, abort, err := c.PrepareRef(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Put(name, raw); err != nil {
		abort()
		t.Fatal(err)
	}
	commit()
	if old != nil {
		if err := c.ReleaseRef(*old); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPrepareRefWritesClosureAndUnion(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, keys := storeTree(t, ts.objects, "one", 4)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "v1", root)
	// Closure file exists and holds every visited tail.
	tails, ok := c.d.read(root)
	if !ok {
		t.Fatal("no closure written")
	}
	want := tailsOf(keys)
	if len(tails) != len(want) {
		t.Fatalf("closure has %d tails, want %d", len(tails), len(want))
	}
	u := c.union.Load()
	for _, k := range keys {
		if !u.contains(Tail(k)) {
			t.Errorf("union missing %s", k)
		}
	}
}

func TestPrepareRefMissingObjectFails(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	// A root whose child blob was never stored.
	orphan, err := fstree.EncodeBlob([]byte("never-stored"))
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := fstree.EncodeFileNode([]key.Key{orphan.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.objects.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	c := ts.openCollector(t, Options{})
	_, _, err = c.PrepareRef(rootObj.Key)
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != orphan.Key {
		t.Fatalf("err = %v, want MissingObjectError naming %s", err, orphan.Key)
	}
	if _, ok := c.d.read(rootObj.Key); ok {
		t.Error("closure written despite failed walk")
	}
}

func TestAbortUndoes(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, _ := storeTree(t, ts.objects, "ab", 3)
	c := ts.openCollector(t, Options{})
	commit, abort, err := c.PrepareRef(root)
	_ = commit
	if err != nil {
		t.Fatal(err)
	}
	abort()
	if c.union.Load().contains(Tail(root)) {
		t.Error("union keeps tails after abort")
	}
	if _, ok := c.d.read(root); ok {
		t.Error("closure file survives abort with no names")
	}
	// abort after commit is a no-op (once).
	putTestRef(t, c, ts.refs, "v1", root)
}

func TestOverwriteAndDelete(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root1, keys1 := storeTree(t, ts.objects, "one", 3)
	root2, _ := storeTree(t, ts.objects, "two", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "v", root1)
	putTestRef(t, c, ts.refs, "v", root2) // overwrite: releases root1
	if c.union.Load().contains(Tail(keys1[0])) {
		t.Error("overwritten root's unique tails still live")
	}
	if _, ok := c.d.read(root1); ok {
		t.Error("unreferenced closure file kept")
	}
	if !c.union.Load().contains(Tail(root2)) {
		t.Error("new root not live")
	}
	// Same-root overwrite stays balanced.
	putTestRef(t, c, ts.refs, "v", root2)
	if !c.union.Load().contains(Tail(root2)) {
		t.Error("same-root overwrite lost liveness")
	}
	// Delete.
	if err := ts.refs.Delete("v"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(root2); err != nil {
		t.Fatal(err)
	}
	if c.union.Load().size() != 0 {
		t.Errorf("union size %d after last delete, want 0", c.union.Load().size())
	}
	if _, ok := c.d.read(root2); ok {
		t.Error("closure survives last delete")
	}
}

func TestTwoNamesOneRoot(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, _ := storeTree(t, ts.objects, "sh", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "a", root)
	putTestRef(t, c, ts.refs, "b", root)
	if err := ts.refs.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
	if !c.union.Load().contains(Tail(root)) {
		t.Error("root died while a name still points at it")
	}
	if _, ok := c.d.read(root); !ok {
		t.Error("shared closure file removed too early")
	}
}

func TestOpenRebuildsAndCleans(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, keys := storeTree(t, ts.objects, "rb", 3)
	orphanRoot, _ := storeTree(t, ts.objects, "or", 2)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "v1", root)
	// Plant an orphan closure (no reference names it).
	if err := c.d.write(orphanRoot, []uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := ts.openCollector(t, Options{})
	if _, ok := c2.d.read(orphanRoot); ok {
		t.Error("orphan closure survives open")
	}
	u := c2.union.Load()
	for _, k := range keys {
		if !u.contains(Tail(k)) {
			t.Errorf("rebuilt union missing %s", k)
		}
	}
}

func TestOpenPendingResolvedByPrepareRef(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, _ := storeTree(t, ts.objects, "pd", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "a", root)
	putTestRef(t, c, ts.refs, "b", root)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the closure so the next open marks the root pending.
	path := filepath.Join(ts.dir, "closures", root.String()+tailsSuffix)
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	c2 := ts.openCollector(t, Options{})
	if c2.union.Load().contains(Tail(root)) {
		t.Fatal("pending root's tails in union before any walk")
	}
	// A third name arrives: PrepareRef must fold in the two existing names
	// too, so one later release cannot zero the root out.
	putTestRef(t, c2, ts.refs, "c", root)
	if err := ts.refs.Delete("c"); err != nil {
		t.Fatal(err)
	}
	if err := c2.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
	if !c2.union.Load().contains(Tail(root)) {
		t.Error("root died while names a and b still point at it")
	}
}

func TestOpenWeighsSharedRoots(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, keys := storeTree(t, ts.objects, "wr", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "a", root)
	putTestRef(t, c, ts.refs, "b", root)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := ts.openCollector(t, Options{})
	// The rebuilt union weighted the closure by both names: one release
	// must not zero it out.
	if err := ts.refs.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := c2.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !c2.union.Load().contains(Tail(k)) {
			t.Fatalf("tail of %s died while name b still points at the root", k)
		}
	}
}

func TestConcurrentPrepareAndRelease(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, _ := storeTree(t, ts.objects, "cc", 4)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "keep", root) // roots stays >= 0 throughout
	for i := 0; i < 100; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			commit, _, err := c.PrepareRef(root)
			if err != nil {
				t.Error(err)
				return
			}
			commit()
		}()
		go func() {
			defer wg.Done()
			if err := c.ReleaseRef(root); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
	}
	// Net zero: exactly the original name remains.
	if err := c.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
	if n := c.union.Load().size(); n != 0 {
		t.Fatalf("union holds %d tails after the last release — a merge-out was lost", n)
	}
	if _, ok := c.d.read(root); ok {
		t.Error("closure file survives the last release")
	}
}
