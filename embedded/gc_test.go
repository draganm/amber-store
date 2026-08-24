package embedded_test

import (
	"errors"
	"testing"
	"time"

	"github.com/draganm/amber-store/embedded"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// unsignedRecord encodes a canonical local (unsigned) reference record.
func unsignedRecord(t *testing.T, name string, root key.Key) []byte {
	t.Helper()
	raw, err := reference.Reference{Name: name, Key: root[:], CreatedAt: time.Now().UnixNano()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestLocalRefOpsThroughCollector(t *testing.T) {
	st, err := embedded.Open(t.TempDir(), embedded.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	root := buildTree(t, st, "local-ref")

	if err := st.PutRef(unsignedRecord(t, "v", root)); err != nil {
		t.Fatal(err)
	}
	ct := st.GC.Counters()
	if ct.Refs != 1 || ct.Closures != 1 || ct.Union == 0 {
		t.Fatalf("counters after put = %+v, want 1 ref, 1 closure, a nonempty union", ct)
	}
	if err := st.DeleteRef("v"); err != nil {
		t.Fatal(err)
	}
	ct = st.GC.Counters()
	if ct.Refs != 0 || ct.Closures != 0 || ct.Union != 0 {
		t.Fatalf("counters after delete = %+v, want empty", ct)
	}
	if err := st.DeleteRef("v"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("second delete: %v, want refstore.ErrNotFound", err)
	}
}

func TestPutRefNamesMissingObject(t *testing.T) {
	st, err := embedded.Open(t.TempDir(), embedded.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	orphan, err := fstree.EncodeBlob([]byte("embedded-never-stored"))
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := fstree.EncodeFileNode([]key.Key{orphan.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Objects.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	err = st.PutRef(unsignedRecord(t, "v", rootObj.Key))
	var missing *fstree.MissingObjectError
	if !errors.As(err, &missing) || missing.Key != orphan.Key {
		t.Fatalf("err = %v, want MissingObjectError naming %s", err, orphan.Key)
	}
}

func TestNoGCSkipsCollector(t *testing.T) {
	st, err := embedded.Open(t.TempDir(), embedded.Config{NoGC: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if st.GC != nil {
		t.Fatal("NoGC store has a collector")
	}
	root := buildTree(t, st, "nogc")
	if err := st.PutRef(unsignedRecord(t, "v", root)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteRef("v"); err != nil {
		t.Fatal(err)
	}
}
