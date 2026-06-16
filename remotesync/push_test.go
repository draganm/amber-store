package remotesync_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remotesync"
)

func TestPushTransfersAllReachableObjects(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)

	stats, err := remotesync.Push(context.Background(), local, h.rc(t), "site", root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsTotal != 4 || stats.ObjectsPushed != 4 {
		t.Fatalf("stats = %+v, want 4/4", stats)
	}
	// Push acks before the pack is processed; drain the inbox so the
	// server-side reachable walk below sees the stored objects.
	h.inbox.WaitFor(root)
	// every reachable object is now on the server
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		t.Fatalf("server-side walk failed: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("server has %d reachable objects, want 4", len(keys))
	}
}

func TestPushBytesPushedIsCompressed(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	// A single highly compressible blob: its stored (zstd) record is far smaller
	// than the raw payload, so BytesPushed must report the compressed size that
	// actually travels, not the uncompressed length.
	raw := bytes.Repeat([]byte("amber"), 4096) // 20480 bytes, very compressible
	blob, err := fstree.EncodeBlob(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.EncodeRecord(blob.Key, blob.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := int64(len(rec) - amberpack.RecHeaderSize)

	stats, err := remotesync.Push(context.Background(), local, h.rc(t), "site", blob.Key, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.BytesPushed != wantBytes {
		t.Fatalf("BytesPushed = %d, want %d (compressed stored size)", stats.BytesPushed, wantBytes)
	}
	if stats.BytesPushed >= int64(len(raw)) {
		t.Fatalf("BytesPushed %d not below raw %d; not compressed", stats.BytesPushed, len(raw))
	}
}

func TestPushPipelineTransfersEverythingWithShallowPrefetch(t *testing.T) {
	// Many small packs through a shallow prefetch queue (Prefetch 1) and more
	// uploaders than the buffer depth: exercises the reader→queue→uploader
	// pipeline's backpressure and drain without losing or duplicating objects.
	h := newHarness(t)
	local := newLocalStore(t)
	ctx := context.Background()
	var blobKeys []key.Key
	for i := range 50 {
		blobKeys = append(blobKeys, putBlob(t, local, byte(i), 1000).Key)
	}
	root := buildFileTree(t, local, blobKeys)

	stats, err := remotesync.Push(ctx, local, h.rc(t), "site", root, remotesync.Opts{
		BatchBytes: 2000, // ~1 blob/pack → many packs
		Jobs:       4,
		Prefetch:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 52 { // 50 blobs + FileNode + DirLeaf
		t.Fatalf("pushed %d objects, want 52", stats.ObjectsPushed)
	}
	h.inbox.WaitFor(root)
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 52 {
		t.Fatalf("server has %d reachable objects, want 52", len(keys))
	}
}

func TestPushIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, "site", root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	// Drain the first push's pack before re-negotiating, or the server may
	// still report the objects missing and the re-push would not be a no-op.
	h.inbox.WaitFor(root)
	stats, err := remotesync.Push(ctx, local, rc, "site", root, remotesync.Opts{})
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
	_, err := remotesync.Push(context.Background(), local, h.rc(t), "site", root, remotesync.Opts{
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
func putBlob(t *testing.T, store *packstore.Store, seed byte, size int) fstree.Object {
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
func buildFileTree(t *testing.T, store *packstore.Store, blobKeys []key.Key) key.Key {
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
	rootA := buildFileTree(t, local, oldKeys)
	if _, err := remotesync.Push(ctx, local, rc, "site", rootA, opts); err != nil {
		t.Fatal(err)
	}
	// Drain the first tree's pack so its blobs are present on the server and
	// the second push's negotiation correctly counts them as not-missing.
	h.inbox.WaitFor(rootA)

	// the second tree interleaves old (present) and new (missing) blobs, so
	// every 2000-byte check batch is half-present: ~1 missing key each
	rootB := buildFileTree(t, local, allKeys)
	uploads.Store(0)
	stats, err := remotesync.Push(ctx, local, rc, "site", rootB, opts)
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

// failPath 500s every request to path, untouched otherwise. The error
// reaches Push either as a StatusError or — because the injected 500 is
// unsigned — as a server-identity error; both are push failures.
func failPath(path string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == path {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func TestPushNoOpMakesNoUploads(t *testing.T) {
	var uploads atomic.Int64
	h := newHarnessMW(t, countUploads(&uploads))
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, "site", root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	// Drain the first push so the no-op push below sees every object present
	// and makes zero uploads.
	h.inbox.WaitFor(root)

	uploads.Store(0)
	var last, total int
	stats, err := remotesync.Push(ctx, local, rc, "site", root, remotesync.Opts{
		Progress: func(d, tot int) { last, total = d, tot },
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 0 || stats.BytesPushed != 0 {
		t.Fatalf("no-op push transferred %+v, want nothing", stats)
	}
	if n := uploads.Load(); n != 0 {
		t.Fatalf("no-op push made %d upload requests, want 0", n)
	}
	if last != 4 || total != 4 {
		t.Fatalf("final progress = %d/%d, want 4/4 (all keys settle at negotiation)", last, total)
	}
}

func TestPushProgressIsMonotonic(t *testing.T) {
	// the 4-object tree fits one check batch, so the two callbacks —
	// settle-at-negotiation then settle-at-upload — are strictly ordered
	// and the recorded sequence must never decrease
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	var mu sync.Mutex
	var seen []int
	_, err := remotesync.Push(context.Background(), local, h.rc(t), "site", root, remotesync.Opts{
		Progress: func(done, total int) {
			if total != 4 {
				t.Errorf("total = %d, want 4", total) // Errorf: callback runs off the test goroutine
			}
			mu.Lock()
			seen = append(seen, done)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("progress went backwards: %v", seen)
		}
	}
	if len(seen) == 0 || seen[len(seen)-1] != 4 {
		t.Fatalf("recorded progress %v, want it to end at 4", seen)
	}
}

func TestPushSurfacesUploadFailure(t *testing.T) {
	h := newHarnessMW(t, failPath("/v1/objects"))
	local := newLocalStore(t)
	root := buildTree(t, local)
	_, err := remotesync.Push(context.Background(), local, h.rc(t), "site", root, remotesync.Opts{})
	if err == nil {
		t.Fatal("push succeeded although every upload failed")
	}
	// the unwind must surface the real failure, not its own cancellation
	if errors.Is(err, context.Canceled) {
		t.Fatalf("push reported the unwind (%v) instead of the upload failure", err)
	}
}

func TestPushSurfacesNegotiationFailure(t *testing.T) {
	h := newHarnessMW(t, failPath("/v1/objects/missing"))
	local := newLocalStore(t)
	root := buildTree(t, local)
	_, err := remotesync.Push(context.Background(), local, h.rc(t), "site", root, remotesync.Opts{})
	if err == nil {
		t.Fatal("push succeeded although negotiation failed")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("push reported the unwind (%v) instead of the negotiation failure", err)
	}
}
