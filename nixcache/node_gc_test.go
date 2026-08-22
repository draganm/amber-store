package nixcache_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/nixcache"
	"github.com/draganm/amber-store/packstore"
)

func TestNodeGC(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()

	var dir string
	n1, srv1, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		dir = c.Dir
	})
	n1.SyncCatalog(t.Context())
	if resp, err := http.Get(srv1.URL + "/" + hashPart(7) + ".narinfo"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("ingest: %v %v", err, resp)
	} else {
		resp.Body.Close()
	}
	srv1.Close()
	n1.Close()

	// Plant an orphan, as a torn WriteBatch would leave behind.
	orphan, err := fstree.EncodeBlob([]byte("orphaned by a crashed ingest"))
	if err != nil {
		t.Fatal(err)
	}
	ps, err := packstore.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Put(orphan.Key, orphan.Bytes); err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	n2, err := nixcache.OpenNode(nixcache.NodeConfig{Dir: dir, Upstream: "http://unreachable.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()

	report, err := n2.Liveness()
	if err != nil {
		t.Fatal(err)
	}
	var live, dead int
	for _, seg := range report {
		live += seg.LiveKeys
		dead += seg.DeadKeys
	}
	if dead != 1 {
		t.Fatalf("dry run: %d dead keys, want 1 (%+v)", dead, report)
	}
	if live == 0 {
		t.Fatal("dry run: no live keys")
	}

	stats, err := n2.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsCompacted == 0 || stats.BytesFreed == 0 {
		t.Fatalf("stats: %+v", stats)
	}

	// The indexed path survives; the orphan is gone.
	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	resp, err := http.Get(srv2.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("narinfo after GC: %d %s", resp.StatusCode, body)
	}
	ni, err := nixcache.ParseNarinfo(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(srv2.URL + "/" + ni.URL)
	if err != nil {
		t.Fatal(err)
	}
	nar, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(nar) == 0 {
		t.Fatalf("nar after GC: %d, %d bytes", resp.StatusCode, len(nar))
	}
	n2.Close()

	ps, err = packstore.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if has, err := ps.Has(orphan.Key); err != nil || has {
		t.Fatalf("orphan still stored (has=%v, err=%v)", has, err)
	}
}

// TestNodeGCBarrier ingests a path between GC's mark and sweep: the mark
// cannot see it, only the write barrier keeps it alive.
func TestNodeGCBarrier(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()

	orphan, err := fstree.EncodeBlob([]byte("orphaned by a crashed ingest"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ps, err := packstore.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Put(orphan.Key, orphan.Bytes); err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := nixcache.OpenNode(nixcache.NodeConfig{
		Dir:         dir,
		Upstream:    u.srv.URL,
		TrustedKeys: []string{"test-1:" + b64(u.pub)},
		CatalogURLs: []string{catalog.URL + "/store-paths"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.SyncCatalog(t.Context())
	srv := httptest.NewServer(n.Handler())
	defer srv.Close()

	nixcache.SetMidMark(n, func() {
		resp, err := http.Get(srv.URL + "/" + hashPart(7) + ".narinfo")
		if err != nil || resp.StatusCode != 200 {
			t.Errorf("mid-mark ingest: %v %v", err, resp)
		}
		if resp != nil {
			resp.Body.Close()
		}
	})
	if _, err := n.GC(0); err != nil {
		t.Fatal(err)
	}
	n.Close()

	// Reopen without upstream: the path must serve from local objects.
	n2, err := nixcache.OpenNode(nixcache.NodeConfig{Dir: dir, Upstream: "http://unreachable.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	resp, err := http.Get(srv2.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("narinfo after barrier GC: %d %s", resp.StatusCode, body)
	}
	ni, err := nixcache.ParseNarinfo(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(srv2.URL + "/" + ni.URL)
	if err != nil {
		t.Fatal(err)
	}
	nar, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(nar) == 0 {
		t.Fatalf("nar after barrier GC: %d, %d bytes", resp.StatusCode, len(nar))
	}
	n2.Close()

	ps, err = packstore.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if has, err := ps.Has(orphan.Key); err != nil || has {
		t.Fatalf("orphan survived (has=%v, err=%v)", has, err)
	}
}
