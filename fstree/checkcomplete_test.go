package fstree_test

import (
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// mapHas reports membership in an in-memory object set.
func mapHas(objs ...fstree.Object) func(key.Key) (bool, error) {
	m := map[key.Key]bool{}
	for _, o := range objs {
		m[o.Key] = true
	}
	return func(k key.Key) (bool, error) {
		return m[k], nil
	}
}

// completeTree builds a small tree exercising every object type and returns
// all of its objects, root last.
func completeTree(t *testing.T) []fstree.Object {
	t.Helper()
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
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, ContentKey: blobA.Key[:], XattrsKey: xattrs.Key[:]},
		{Name: []byte("big"), Mode: 0o100644, ContentKey: fileNode.Key[:]},
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
	return []fstree.Object{blobA, blobB, fileNode, xattrs, leaf, root}
}

func TestCheckComplete_CompleteTree(t *testing.T) {
	objs := completeTree(t)
	root := objs[len(objs)-1]
	if err := fstree.CheckComplete(root.Key, mapGetter(objs...), mapHas(objs...), 4); err != nil {
		t.Fatalf("CheckComplete on a complete tree: %v", err)
	}
}

func TestCheckComplete_MissingLeaf(t *testing.T) {
	objs := completeTree(t)
	root := objs[len(objs)-1]
	missing := objs[1] // blobB
	var present []fstree.Object
	for _, o := range objs {
		if o.Key != missing.Key {
			present = append(present, o)
		}
	}
	err := fstree.CheckComplete(root.Key, mapGetter(present...), mapHas(present...), 4)
	var miss *fstree.MissingObjectError
	if !errors.As(err, &miss) {
		t.Fatalf("err = %v, want a *MissingObjectError", err)
	}
	if miss.Key != missing.Key {
		t.Fatalf("missing key = %s, want %s", miss.Key, missing.Key)
	}
}

func TestCheckComplete_MissingInteriorNode(t *testing.T) {
	objs := completeTree(t)
	root := objs[len(objs)-1]
	missing := objs[2] // fileNode
	var present []fstree.Object
	for _, o := range objs {
		if o.Key != missing.Key {
			present = append(present, o)
		}
	}
	// mapGetter fails absent fetches with key.ErrBadKeyLength; the wrapped get
	// error must surface so callers can map it to their own not-found.
	err := fstree.CheckComplete(root.Key, mapGetter(present...), mapHas(present...), 4)
	if !errors.Is(err, key.ErrBadKeyLength) {
		t.Fatalf("err = %v, want the wrapped get error", err)
	}
}
