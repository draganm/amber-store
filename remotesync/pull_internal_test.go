package remotesync

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// TestLocalMissingClassifiesPartialTree checks the completeness-gate walk:
// present interior nodes are descended, and reachable-but-absent objects are
// collected as the set still to fetch.
func TestLocalMissingClassifiesPartialTree(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	mk := func(o fstree.Object, err error) fstree.Object {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	b1 := mk(fstree.EncodeBlob([]byte("one")))
	b2 := mk(fstree.EncodeBlob([]byte("two")))
	fn := mk(fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key}))
	leaf := mk(fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: fn.Key[:]},
	}))

	// Store the two interior nodes but neither blob.
	for _, o := range []fstree.Object{leaf, fn} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	missing, err := localMissing(leaf.Key, store)
	if err != nil {
		t.Fatalf("localMissing: %v", err)
	}
	// The walk descends leaf -> fn (both present) and reports b1, b2 missing.
	want := map[key.Key]bool{b1.Key: true, b2.Key: true}
	if len(missing) != len(want) {
		t.Fatalf("missing = %v, want the two blobs", missing)
	}
	for _, k := range missing {
		if !want[k] {
			t.Errorf("unexpected missing key %s", k)
		}
	}

	// With everything stored, the gate is clean.
	for _, o := range []fstree.Object{b1, b2} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	if m, err := localMissing(leaf.Key, store); err != nil || len(m) != 0 {
		t.Fatalf("complete tree: missing = %v, err = %v, want none", m, err)
	}
}
