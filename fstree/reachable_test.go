package fstree_test

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// TestReachableKeys walks a tree exercising every object type — a DirNode root,
// DirLeaves, a multi-chunk file (FileNode over Blobs), a spilled XattrSet — and
// a blob referenced twice to check deduplication.
func TestReachableKeys(t *testing.T) {
	blobA, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	blobB, err := fstree.EncodeBlob([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blobA.Key, blobB.Key})
	if err != nil {
		t.Fatal(err)
	}
	xattrs, err := fstree.EncodeXattrSet(map[string][]byte{"user.comment": []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	subLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("big"), Mode: 0o100644, ContentKey: fileNode.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// blobA is referenced both here and as a FileNode child; it must be listed once.
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, ContentKey: blobA.Key[:], XattrsKey: xattrs.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, LinkTarget: []byte("a.txt")},
		{Name: []byte("sub"), Mode: 0o040755, ContentKey: subLeaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := fstree.EncodeDirNode([]fstree.DirPair{
		{SepName: []byte("a.txt"), ChildKey: leaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	get := mapGetter(blobA, blobB, fileNode, xattrs, subLeaf, leaf, root)

	got, err := fstree.ReachableKeys(root.Key, get)
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}

	want := map[key.Key]bool{
		root.Key:     true,
		leaf.Key:     true,
		blobA.Key:    true,
		xattrs.Key:   true,
		subLeaf.Key:  true,
		fileNode.Key: true,
		blobB.Key:    true,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	if got[0] != root.Key {
		t.Errorf("first key = %s, want the root %s", got[0], root.Key)
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("unexpected key %s (type %v)", k, k.Type())
		}
		delete(want, k)
	}
	for k := range want {
		t.Errorf("missing key %s (type %v)", k, k.Type())
	}
}

// TestReachableKeys_FileRoot walks a bare file object: the root may be any
// object type, not only a directory.
func TestReachableKeys_FileRoot(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fstree.ReachableKeys(fileNode.Key, mapGetter(blob, fileNode))
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	if len(got) != 2 || got[0] != fileNode.Key || got[1] != blob.Key {
		t.Fatalf("keys = %v, want [fileNode, blob]", got)
	}
}

// TestReachableKeys_MissingObject surfaces a get failure for an absent child.
func TestReachableKeys_MissingObject(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	missing, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("x"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("gone"), Mode: 0o040755, ContentKey: missing.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The store holds only the parent: walking into "gone" must fail.
	if _, err := fstree.ReachableKeys(parent.Key, mapGetter(parent)); err == nil {
		t.Fatal("expected error for a missing child object")
	}
}
