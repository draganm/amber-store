package daemon_test

import (
	"context"
	"crypto/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
)

// gcServer serves the daemon over a unix socket with a small-segment store
// (so packs actually rotate) and a millisecond-grace collector (so a short
// sleep makes them eligible), returning the client and the store.
func gcServer(t *testing.T) (*client.Client, *packstore.Store) {
	t.Helper()
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false), packstore.WithSegmentSize(4096))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs := openRefs(t)
	coll, err := gc.Open(filepath.Join(t.TempDir(), "closures"), store, refs, gc.Options{Grace: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	dir, err := os.MkdirTemp("", "amber-gc-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.New(store, refs, coll, nil)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return client.New(sock), store
}

// putTree stores a FileNode over n incompressible 256-byte blobs and returns
// the root and every stored key.
func putTree(t *testing.T, store *packstore.Store, n int) (key.Key, []key.Key) {
	t.Helper()
	var children, all []key.Key
	for range n {
		data := make([]byte, 256)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		children = append(children, o.Key)
		all = append(all, o.Key)
	}
	rootObj, err := fstree.EncodeFileNode(children)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	return rootObj.Key, append(all, rootObj.Key)
}

func putGCRef(t *testing.T, c *client.Client, name string, root key.Key) {
	t.Helper()
	err := c.PutRef(context.Background(), reference.Reference{
		Name: name, Key: root[:], CreatedAt: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestGC_HTTPSurface drives status, why and run through the client: garbage
// scores appear, why names the holder, a forced run reaps the orphaned tree
// and the referenced one survives.
func TestGC_HTTPSurface(t *testing.T) {
	c, store := gcServer(t)
	ctx := context.Background()

	liveRoot, liveKeys := putTree(t, store, 30)
	putGCRef(t, c, "gc/live", liveRoot)
	deadRoot, deadKeys := putTree(t, store, 30) // never referenced

	st, err := c.GCStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Refs != 1 {
		t.Errorf("Refs = %d, want 1", st.Refs)
	}
	if st.Marked != len(liveKeys) {
		t.Errorf("Marked = %d, want %d", st.Marked, len(liveKeys))
	}
	if st.GarbageBytes == 0 {
		t.Error("GarbageBytes = 0 with a whole orphaned tree stored")
	}

	names, err := c.GCWhy(ctx, liveKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "gc/live" {
		t.Errorf("GCWhy(live) = %v, want [gc/live]", names)
	}
	names, err = c.GCWhy(ctx, deadRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Errorf("GCWhy(dead) = %v, want none", names)
	}

	time.Sleep(50 * time.Millisecond) // sealed packs age past the 1ms grace
	stats, err := c.GCRun(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) == 0 {
		t.Fatal("forced run reaped nothing")
	}
	if has, err := store.Has(deadKeys[0]); err != nil || has {
		t.Errorf("orphaned blob survived the sweep (has=%v, err=%v)", has, err)
	}
	for _, k := range liveKeys {
		if _, err := store.Get(k); err != nil {
			t.Fatalf("live key %s unreadable after the sweep: %v", k, err)
		}
	}
}

// TestPutRef_IncompleteTree ensures the daemon's reference PUT walks the
// whole tree: a root whose child was never stored is a 404, not a stored
// dangling reference.
func TestPutRef_IncompleteTree(t *testing.T) {
	c, store := gcServer(t)
	blob, err := fstree.EncodeBlob([]byte("never stored"))
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	var root key.Key
	copy(root[:], rootObj.Key[:])
	err = c.PutRef(context.Background(), reference.Reference{
		Name: "gc/incomplete", Key: root[:], CreatedAt: time.Now().UnixNano(),
	})
	if err == nil {
		t.Fatal("PutRef accepted a reference to an incomplete tree")
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Errorf("error %q does not name the incompleteness", err)
	}
	if _, err := c.GetRef(context.Background(), "gc/incomplete"); err == nil {
		t.Error("the rejected reference was stored anyway")
	}
}
