package daemon_test

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// refRecord encodes a canonical unsigned reference record.
func refRecord(t *testing.T, name string, kb []byte) []byte {
	t.Helper()
	return encodeRef(t, reference.Reference{Name: name, Key: kb, User: "u", CreatedAt: 42})
}

// gcHarness is a daemon over a gc-enabled store pair in one temp dir.
type gcHarness struct {
	dir     string
	objects *packstore.Store
	refs    *refstore.Store
	coll    *gc.Collector
	srv     *httptest.Server
}

func newGCHarness(t *testing.T, segSize int64) *gcHarness {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "packstore"),
		packstore.WithSync(false), packstore.WithSegmentSize(segSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	coll, err := gc.Open(filepath.Join(dir, "closures"), objects, refs, gc.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	srv := httptest.NewServer(daemon.New(objects, refs, nil, daemon.WithCollector(coll)))
	t.Cleanup(srv.Close)
	return &gcHarness{dir: dir, objects: objects, refs: refs, coll: coll, srv: srv}
}

// storeTree stores a FileNode over n distinct incompressible 256-byte blobs
// and returns the root plus every key.
func (h *gcHarness) storeTree(t *testing.T, seed uint64, n int) (key.Key, []key.Key) {
	t.Helper()
	var children, all []key.Key
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewPCG(seed, uint64(i)))
		data := make([]byte, 256)
		for j := range data {
			data[j] = byte(rng.UintN(256))
		}
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.objects.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		children, all = append(children, o.Key), append(all, o.Key)
	}
	rootObj, err := fstree.EncodeFileNode(children)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.objects.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	return rootObj.Key, append(all, rootObj.Key)
}

// why fetches GET /v1/gc/why/{key}.
func (h *gcHarness) why(t *testing.T, k key.Key) []string {
	t.Helper()
	resp := doReq(t, http.MethodGet, h.srv.URL+"/v1/gc/why/"+hex.EncodeToString(k[:]), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gc why: status %d", resp.StatusCode)
	}
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		t.Fatal(err)
	}
	return names
}

func TestGCPutRefWalksAndNamesMissing(t *testing.T) {
	h := newGCHarness(t, packstore.DefaultSegmentSize)
	orphan, err := fstree.EncodeBlob([]byte("gc-daemon-missing"))
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := fstree.EncodeFileNode([]key.Key{orphan.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.objects.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	rec := refRecord(t, "v", rootObj.Key[:])
	resp := doReq(t, http.MethodPut, refURL(h.srv.URL, "v"), rec)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404; body %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), orphan.Key.String()) {
		t.Fatalf("404 body %q does not name the missing object %s", body, orphan.Key)
	}
	// Store the missing blob; the retried PUT succeeds and the closure lands.
	if err := h.objects.Put(orphan.Key, orphan.Bytes); err != nil {
		t.Fatal(err)
	}
	resp = doReq(t, http.MethodPut, refURL(h.srv.URL, "v"), rec)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("retried put: status %d, want 204", resp.StatusCode)
	}
	if names := h.why(t, orphan.Key); len(names) != 1 || names[0] != "v" {
		t.Fatalf("gc why = %v, want [v]", names)
	}
	// DELETE releases the root.
	resp = doReq(t, http.MethodDelete, refURL(h.srv.URL, "v"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: status %d, want 204", resp.StatusCode)
	}
	if names := h.why(t, orphan.Key); len(names) != 0 {
		t.Fatalf("gc why after delete = %v, want none", names)
	}
}

func TestGCRunReapsThroughDaemon(t *testing.T) {
	h := newGCHarness(t, 4<<10)
	liveRoot, liveKeys := h.storeTree(t, 1, 8)
	deadRoot, _ := h.storeTree(t, 2, 40)
	for name, root := range map[string]key.Key{"live": liveRoot, "dead": deadRoot} {
		resp := doReq(t, http.MethodPut, refURL(h.srv.URL, name), refRecord(t, name, root[:]))
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("put %s: status %d", name, resp.StatusCode)
		}
	}
	resp := doReq(t, http.MethodDelete, refURL(h.srv.URL, "dead"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete dead: status %d", resp.StatusCode)
	}
	// Age the sealed packs out of grace.
	ents, err := os.ReadDir(filepath.Join(h.dir, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			if err := os.Chtimes(filepath.Join(h.dir, "packstore", e.Name()), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	resp = doReq(t, http.MethodPost, h.srv.URL+"/v1/gc/run", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gc run: status %d", resp.StatusCode)
	}
	var stats gc.CycleStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(stats.Reaped) == 0 || stats.FreedBytes <= 0 {
		t.Fatalf("cycle reaped %d packs, freed %d bytes; want progress", len(stats.Reaped), stats.FreedBytes)
	}
	for _, k := range liveKeys {
		if _, err := h.objects.Get(k); err != nil {
			t.Fatalf("live object %s lost: %v", k, err)
		}
	}
	// Status reflects the finished cycle.
	resp = doReq(t, http.MethodGet, h.srv.URL+"/v1/gc", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gc status: status %d", resp.StatusCode)
	}
	var st gc.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if st.Last == nil || len(st.Last.Reaped) != len(stats.Reaped) || st.Refs != 1 {
		t.Fatalf("status = %+v, want last cycle with %d reaped and 1 ref", st, len(stats.Reaped))
	}
}

func TestGCRoutesDisabled(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	t.Cleanup(srv.Close)
	for _, req := range [][2]string{
		{http.MethodGet, "/v1/gc"},
		{http.MethodPost, "/v1/gc/run"},
		{http.MethodGet, "/v1/gc/why/" + strings.Repeat("00", 32)},
	} {
		resp := doReq(t, req[0], srv.URL+req[1], nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s %s: status %d, want 503", req[0], req[1], resp.StatusCode)
		}
	}
}
