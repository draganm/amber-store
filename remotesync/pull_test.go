package remotesync_test

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/remotesync"
)

func TestPullFetchesWholeTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store) // tree lives on the SERVER
	local := newLocalStore(t)

	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 4 {
		t.Fatalf("fetched %d objects, want 4", stats.ObjectsFetched)
	}
	// the local store can now serve the full tree
	keys, err := fstree.ReachableKeys(root, local.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("local reachable = %d, want 4", len(keys))
	}
	// payloads survived intact
	for _, k := range keys {
		want, err := h.store.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		got, err := local.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("payload of %s differs", k)
		}
	}
}

func TestPullCompletesPartialLocalTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)

	// pre-seed the local store with the root object only: present-but-
	// incomplete roots must still be descended (spec §3).
	rootData, err := h.store.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(root, rootData); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 3 {
		t.Fatalf("fetched %d objects, want 3 (root was local)", stats.ObjectsFetched)
	}
	if keys, err := fstree.ReachableKeys(root, local.Get); err != nil || len(keys) != 4 {
		t.Fatalf("local tree incomplete: %d keys, err %v", len(keys), err)
	}
}

func TestPullIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 0 {
		t.Fatalf("re-pull fetched %d objects, want 0", stats.ObjectsFetched)
	}
}

func TestPullAbsentRootFails(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	absent, err := fstree.EncodeBlob([]byte("never on server"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remotesync.Pull(context.Background(), local, h.rc(t), absent.Key, remotesync.Opts{}); err == nil {
		t.Fatal("pull of an absent root succeeded")
	}
}

// TestPullOnBytesTicksWithinOneBatch is the issue the byte-level signal
// exists for: a batch that is moving bytes but has not COMPLETED yet must
// still produce progress. The server drips the objects/get response in small
// flushed chunks; the whole tree fits one batch, so object-level Progress can
// fire only once — OnBytes must tick several times before that.
func TestPullOnBytesTicksWithinOneBatch(t *testing.T) {
	h := newHarnessMW(t, dripResponses)
	root := buildTree(t, h.store)
	local := newLocalStore(t)

	var mu sync.Mutex
	var ticks, bytes int
	_, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{
		OnBytes: func(n int) {
			mu.Lock()
			ticks++
			bytes += n
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if ticks < 2 {
		t.Fatalf("OnBytes ticked %d times, want >= 2 (sub-batch granularity)", ticks)
	}
	if bytes <= 0 {
		t.Fatalf("OnBytes reported %d bytes, want > 0", bytes)
	}
}

// dripResponses wraps the handler so every response body is written in small
// flushed chunks with a delay — a slow link in miniature.
func dripResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&drippingWriter{w: w}, r)
	})
}

type drippingWriter struct {
	w http.ResponseWriter
}

func (d *drippingWriter) Header() http.Header    { return d.w.Header() }
func (d *drippingWriter) WriteHeader(status int) { d.w.WriteHeader(status) }
func (d *drippingWriter) Flush() {
	if f, ok := d.w.(http.Flusher); ok {
		f.Flush()
	}
}

func (d *drippingWriter) Write(p []byte) (int, error) {
	const chunk = 64
	written := 0
	for len(p) > 0 {
		n := min(chunk, len(p))
		m, err := d.w.Write(p[:n])
		written += m
		if err != nil {
			return written, err
		}
		d.Flush()
		time.Sleep(2 * time.Millisecond)
		p = p[n:]
	}
	return written, nil
}
