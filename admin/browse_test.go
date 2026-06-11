package admin_test

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"

	"github.com/draganm/amber-store/admin"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/allowstore"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// memObjects is an in-memory admin.ObjectGetter.
type memObjects map[key.Key][]byte

func (m memObjects) Get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("object %s: %w", k, diskstore.ErrNotFound)
	}
	return b, nil
}

func (m memObjects) put(o fstree.Object) { m[o.Key] = o.Bytes }

// memRefs is an in-memory admin.RefStore.
type memRefs map[string][]byte

func (m memRefs) Get(name string) ([]byte, error) {
	b, ok := m[name]
	if !ok {
		return nil, refstore.ErrNotFound
	}
	return b, nil
}

func (m memRefs) All() ([]refstore.Record, error) {
	recs := make([]refstore.Record, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		recs = append(recs, refstore.Record{Name: name, Data: m[name]})
	}
	return recs, nil
}

func mustObj(t *testing.T, o fstree.Object, err error) fstree.Object {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// mustRef encodes a reference record pointing name at k.
func mustRef(t *testing.T, name string, k key.Key, user string) []byte {
	t.Helper()
	b, err := reference.Reference{Name: name, Key: k[:], User: user, CreatedAt: 12345}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// seedTree stores this fixture tree in objs and returns its root directory
// key and the key of hello.txt's content:
//
//	hello.txt        "hello, amber"
//	sub/link         -> ../hello.txt
//	sub/nested.txt   "nested"
func seedTree(t *testing.T, objs memObjects) (root, hello key.Key) {
	t.Helper()
	helloBlobObj, helloBlobErr := fstree.EncodeBlob([]byte("hello, amber"))
	helloBlob := mustObj(t, helloBlobObj, helloBlobErr)
	objs.put(helloBlob)
	nestedObj, nestedErr := fstree.EncodeBlob([]byte("nested"))
	nested := mustObj(t, nestedObj, nestedErr)
	objs.put(nested)
	subLeafObj, subLeafErr := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("link"), Mode: 0o120777, Mtime: 3, LinkTarget: []byte("../hello.txt")},
		{Name: []byte("nested.txt"), Mode: 0o100644, Mtime: 2, ContentKey: nested.Key[:]},
	})
	subLeaf := mustObj(t, subLeafObj, subLeafErr)
	objs.put(subLeaf)
	rootLeafObj, rootLeafErr := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("hello.txt"), Mode: 0o100644, Mtime: 1, ContentKey: helloBlob.Key[:]},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: 4, ContentKey: subLeaf.Key[:]},
	})
	rootLeaf := mustObj(t, rootLeafObj, rootLeafErr)
	objs.put(rootLeaf)
	return rootLeaf.Key, helloBlob.Key
}

// browseServer starts an admin server whose browse endpoints see objs and
// refs, returning it with a logged-in session cookie.
func browseServer(t *testing.T, objs memObjects, refs memRefs) (*httptest.Server, *http.Cookie) {
	t.Helper()
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = keys.Close() })
	h, err := admin.New(admin.Config{
		Password: password, Keys: keys, Objects: objs, Refs: refs, UI: testUI,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, login(t, srv)
}
