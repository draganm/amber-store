package fstree_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// mapGetter serves objects from an in-memory map.
func mapGetter(objs ...fstree.Object) func(key.Key) ([]byte, error) {
	m := map[key.Key][]byte{}
	for _, o := range objs {
		m[o.Key] = o.Bytes
	}
	return func(k key.Key) ([]byte, error) {
		b, ok := m[k]
		if !ok {
			return nil, key.ErrBadKeyLength // any error will do
		}
		return b, nil
	}
}

func mustDirLeaf(t *testing.T, entries []fstree.Entry) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestCollectEntries_DirLeaf(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	want := []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: blob.Key[:]},
		{Name: []byte("b"), Mode: 0o120777, Mtime: 2, LinkTarget: []byte("a")},
	}
	leaf := mustDirLeaf(t, want)

	got, err := fstree.CollectEntries(leaf.Key, mapGetter(leaf))
	if err != nil {
		t.Fatalf("CollectEntries: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Name, want[i].Name) || got[i].Mode != want[i].Mode {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestCollectEntries_DescendsDirNode(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	leaf1 := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	leaf2 := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("z"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	node, err := fstree.EncodeDirNode([]fstree.DirPair{
		{SepName: []byte("a"), ChildKey: leaf1.Key[:]},
		{SepName: []byte("z"), ChildKey: leaf2.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := fstree.CollectEntries(node.Key, mapGetter(leaf1, leaf2, node))
	if err != nil {
		t.Fatalf("CollectEntries: %v", err)
	}
	if len(got) != 2 || string(got[0].Name) != "a" || string(got[1].Name) != "z" {
		t.Fatalf("entries = %v, want [a z]", got)
	}
}

func TestCollectEntries_RejectsNonDirKey(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fstree.CollectEntries(blob.Key, mapGetter(blob)); err == nil {
		t.Fatal("expected error for a non-directory key")
	}
}

func TestResolvePath(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	inner := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	mid := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("inner"), Mode: 0o040755, ContentKey: inner.Key[:]},
	})
	root := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, ContentKey: blob.Key[:]},
		{Name: []byte("mid"), Mode: 0o040755, ContentKey: mid.Key[:]},
	})
	get := mapGetter(blob, inner, mid, root)

	// Empty path and "." resolve to the root itself.
	for _, p := range []string{"", ".", "./"} {
		got, err := fstree.ResolvePath(root.Key, p, get)
		if err != nil {
			t.Fatalf("ResolvePath(%q): %v", p, err)
		}
		if got != root.Key {
			t.Fatalf("ResolvePath(%q) = %s, want root", p, got)
		}
	}

	// A nested path descends to the inner directory key.
	got, err := fstree.ResolvePath(root.Key, "mid/inner", get)
	if err != nil {
		t.Fatalf("ResolvePath(mid/inner): %v", err)
	}
	if got != inner.Key {
		t.Fatalf("ResolvePath(mid/inner) = %s, want %s", got, inner.Key)
	}

	// Leading and trailing slashes are tolerated.
	got, err = fstree.ResolvePath(root.Key, "/mid/inner/", get)
	if err != nil || got != inner.Key {
		t.Fatalf("ResolvePath(/mid/inner/) = %s, %v", got, err)
	}

	// A missing component is ErrNotFound.
	if _, err := fstree.ResolvePath(root.Key, "mid/nope", get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("missing component: err = %v, want ErrNotFound", err)
	}

	// A non-directory component is ErrNotDir.
	if _, err := fstree.ResolvePath(root.Key, "file", get); !errors.Is(err, fstree.ErrNotDir) {
		t.Fatalf("file component: err = %v, want ErrNotDir", err)
	}

	// ".." has no meaning in a CAS tree and is rejected.
	if _, err := fstree.ResolvePath(root.Key, "mid/..", get); err == nil {
		t.Fatal("expected error for \"..\" component")
	}
}
