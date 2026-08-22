package remotesync

import (
	"context"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
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

type emptySource struct{ root key.Key }

func (e emptySource) ReachableKeys(context.Context, key.Key) ([]key.Key, error) {
	return []key.Key{e.root}, nil
}

func (emptySource) FetchObjects(context.Context, []key.Key, func(int)) ([]fstree.Object, error) {
	return nil, nil
}

func (emptySource) StreamRecords(context.Context, []key.Key, func(int), func(amberpack.RawRecord) error) error {
	return nil
}

// A source that answers OK but delivers nothing must fail the pull, not
// be asked again forever.
func TestPullFailsOnSourceDeliveringNothing(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	obj, err := fstree.EncodeBlob([]byte("never delivered"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, src := range []Source{emptySource{obj.Key}, struct{ Source }{emptySource{obj.Key}}} {
		if _, err := Pull(ctx, store, src, obj.Key, Opts{}); err == nil || ctx.Err() != nil {
			t.Fatalf("%T: err=%v, ctx=%v", src, err, ctx.Err())
		}
	}
}
