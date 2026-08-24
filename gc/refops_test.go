package gc

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// encodeRef builds a canonical unsigned record for tests.
func encodeRef(t *testing.T, name string, root key.Key) []byte {
	t.Helper()
	raw, err := reference.Reference{Name: name, Key: root[:], CreatedAt: time.Now().UnixNano()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPutRefLifecycle(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	rootA, keysA := storeTree(t, ts.objects, "a", 4)
	rootB, keysB := storeTree(t, ts.objects, "b", 6)
	c := ts.openCollector(t, Options{})

	if err := c.PutRef("v", rootA, encodeRef(t, "v", rootA)); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.d.read(rootA); !ok {
		t.Fatal("no closure for rootA")
	}
	u := c.union.Load()
	for _, k := range keysA {
		if !u.contains(Tail(k)) {
			t.Errorf("union missing %s", k)
		}
	}

	// Overwrite: rootB in, rootA out, its closure gone.
	if err := c.PutRef("v", rootB, encodeRef(t, "v", rootB)); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.d.read(rootA); ok {
		t.Error("closure for the overwritten rootA survived")
	}
	u = c.union.Load()
	for _, k := range keysB {
		if !u.contains(Tail(k)) {
			t.Errorf("union missing %s", k)
		}
	}
	if u.size() != len(keysB) {
		t.Errorf("union holds %d tails, want %d", u.size(), len(keysB))
	}

	// Delete: everything out.
	if err := c.DeleteRef("v"); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.refs.Get("v"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("record survived delete: %v", err)
	}
	if got := c.union.Load().size(); got != 0 {
		t.Errorf("union holds %d tails after delete, want 0", got)
	}
}

func TestPutRefMissingObjectNamesIt(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	orphan, err := fstree.EncodeBlob([]byte("refops-never-stored"))
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
	err = c.PutRef("v", rootObj.Key, encodeRef(t, "v", rootObj.Key))
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != orphan.Key {
		t.Fatalf("err = %v, want MissingObjectError naming %s", err, orphan.Key)
	}
	if _, gerr := ts.refs.Get("v"); !errors.Is(gerr, refstore.ErrNotFound) {
		t.Fatalf("record written despite failed walk: %v", gerr)
	}
}

func TestDeleteRefNotFound(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	if err := c.DeleteRef("absent"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("err = %v, want refstore.ErrNotFound", err)
	}
}

// TestPutRefSerializedPerName races overwrites of one name between two
// roots: however the writes interleave, the stored record and the union
// must agree afterwards — the loser's root is fully released.
func TestPutRefSerializedPerName(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	rootA, keysA := storeTree(t, ts.objects, "ser-a", 3)
	rootB, keysB := storeTree(t, ts.objects, "ser-b", 5)
	c := ts.openCollector(t, Options{})

	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		for _, root := range []key.Key{rootA, rootB} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.PutRef("v", root, encodeRef(t, "v", root)); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		raw, err := ts.refs.Get("v")
		if err != nil {
			t.Fatal(err)
		}
		rec, err := reference.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		winner, err := key.Parse(rec.Key)
		if err != nil {
			t.Fatal(err)
		}
		wantKeys := keysA
		if winner == rootB {
			wantKeys = keysB
		}
		u := c.union.Load()
		if u.size() != len(wantKeys) {
			t.Fatalf("round %d: union holds %d tails, want %d (winner %s)",
				round, u.size(), len(wantKeys), winner)
		}
		for _, k := range wantKeys {
			if !u.contains(Tail(k)) {
				t.Fatalf("round %d: union missing %s", round, k)
			}
		}
		if err := c.DeleteRef("v"); err != nil {
			t.Fatal(err)
		}
		if got := c.union.Load().size(); got != 0 {
			t.Fatalf("round %d: union holds %d tails after delete", round, got)
		}
	}
}

func TestCounters(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, keys := storeTree(t, ts.objects, "counters", 4)
	c := ts.openCollector(t, Options{})
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("v%d", i)
		if err := c.PutRef(name, root, encodeRef(t, name, root)); err != nil {
			t.Fatal(err)
		}
	}
	l := c.Lease(root)
	defer l.Release()
	ct := c.Counters()
	if ct.Refs != 2 || ct.Closures != 1 || ct.Pending != 0 || ct.Union != len(keys) || ct.Leases != 1 {
		t.Fatalf("counters = %+v, want refs 2, closures 1, pending 0, union %d, leases 1", ct, len(keys))
	}
}
