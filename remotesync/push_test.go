package remotesync_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remotesync"
)

func TestPushTransfersAllReachableObjects(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)

	stats, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsTotal != 4 || stats.ObjectsPushed != 4 {
		t.Fatalf("stats = %+v, want 4/4", stats)
	}
	// every reachable object is now on the server
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		t.Fatalf("server-side walk failed: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("server has %d reachable objects, want 4", len(keys))
	}
}

func TestPushIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 0 || stats.BytesPushed != 0 {
		t.Fatalf("re-push transferred %+v, want nothing", stats)
	}
}

func TestPushReportsProgress(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	var last, total int
	_, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{
		Progress: func(done, t int) { last, total = done, t },
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 || total != 4 {
		t.Fatalf("final progress = %d/%d, want 4/4", last, total)
	}
}

// countUploads counts POST /v1/objects requests — uploads, not the
// /v1/objects/missing negotiation.
func countUploads(n *atomic.Int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/v1/objects" {
				n.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// putBlob stores a size-byte blob whose leading byte is seed (distinct
// seeds give distinct keys) and returns it.
func putBlob(t *testing.T, store *diskstore.Store, seed byte, size int) fstree.Object {
	t.Helper()
	payload := make([]byte, size)
	payload[0] = seed
	o, err := fstree.EncodeBlob(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	return o
}

// buildFileTree stores DirLeaf{ file "f" → FileNode → blobKeys } and returns
// the root key; the blobs themselves must already be in store.
func buildFileTree(t *testing.T, store *diskstore.Store, blobKeys []key.Key) key.Key {
	t.Helper()
	fn, err := fstree.EncodeFileNode(blobKeys)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: fn.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{fn, leaf} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return leaf.Key
}

func TestPushCoalescesSparseMissingKeys(t *testing.T) {
	var uploads atomic.Int64
	h := newHarnessMW(t, countUploads(&uploads))
	local := newLocalStore(t)
	ctx := context.Background()
	rc := h.rc(t)
	opts := remotesync.Opts{BatchBytes: 2000}

	// 40 1000-byte blobs; the even-seeded half forms the first tree
	var oldKeys, allKeys []key.Key
	for i := range 40 {
		o := putBlob(t, local, byte(i), 1000)
		if i%2 == 0 {
			oldKeys = append(oldKeys, o.Key)
		}
		allKeys = append(allKeys, o.Key)
	}
	if _, err := remotesync.Push(ctx, local, rc, buildFileTree(t, local, oldKeys), opts); err != nil {
		t.Fatal(err)
	}

	// the second tree interleaves old (present) and new (missing) blobs, so
	// every 2000-byte check batch is half-present: ~1 missing key each
	rootB := buildFileTree(t, local, allKeys)
	uploads.Store(0)
	stats, err := remotesync.Push(ctx, local, rc, rootB, opts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 22 { // 20 new blobs + FileNode + DirLeaf
		t.Fatalf("pushed %d objects, want 22", stats.ObjectsPushed)
	}
	// ~21 sparse check batches must coalesce into ~ceil(22kB/2kB) upload
	// packs, not one sliver-sized pack per check batch
	if n := uploads.Load(); n > 13 {
		t.Fatalf("push made %d upload requests, want coalesced (<= 13)", n)
	}
}
