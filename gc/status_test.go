package gc

import (
	"context"
	"testing"
	"time"
)

func TestStatus(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	root, _ := storeTree(t, ts.objects, "st", 10)
	dead, _ := storeTree(t, ts.objects, "dd", 10)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "v", root)
	putTestRef(t, c, ts.refs, "d", dead)
	if err := ts.refs.Delete("d"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(dead); err != nil {
		t.Fatal(err)
	}
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Packs) == 0 {
		t.Fatal("no packs in status")
	}
	if st.Refs != 1 || st.Closures != 1 || st.Pending != 0 {
		t.Errorf("Refs/Closures/Pending = %d/%d/%d, want 1/1/0", st.Refs, st.Closures, st.Pending)
	}
	if st.Union == 0 {
		t.Error("empty union with a live reference")
	}
	if st.GarbageBytes == 0 {
		t.Error("no garbage reported though a root died")
	}
	if st.Last != nil {
		t.Error("Last set before any cycle")
	}
	if _, err := c.Run(context.Background(), 1.1); err != nil {
		t.Fatal(err)
	}
	st, err = c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Last == nil {
		t.Error("Last missing after a cycle")
	}
}

func TestWhy(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, keys := storeTree(t, ts.objects, "wh", 3)
	other, otherKeys := storeTree(t, ts.objects, "ot", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "v1", root)
	putTestRef(t, c, ts.refs, "v2", root)
	putTestRef(t, c, ts.refs, "w", other)
	names, err := c.Why(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "v1" || names[1] != "v2" {
		t.Errorf("Why = %v, want [v1 v2]", names)
	}
	names, err = c.Why(otherKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "w" {
		t.Errorf("Why = %v, want [w]", names)
	}
	// An unreferenced key names nobody.
	stray, _ := storeTree(t, ts.objects, "xx", 1)
	names, err = c.Why(stray)
	if err != nil || len(names) != 0 {
		t.Errorf("Why(stray) = %v, %v; want none", names, err)
	}
}

func TestWipe(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	root, _ := storeTree(t, ts.objects, "wp", 3)
	c := ts.openCollector(t, Options{})
	putTestRef(t, c, ts.refs, "v", root)
	if err := c.Wipe(); err != nil {
		t.Fatal(err)
	}
	if c.union.Load().size() != 0 {
		t.Error("union survives Wipe")
	}
	roots, err := c.d.list()
	if err != nil || len(roots) != 0 {
		t.Errorf("closures survive Wipe: %v, %v", roots, err)
	}
}
