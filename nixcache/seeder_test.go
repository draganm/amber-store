package nixcache_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/draganm/amber-store/nixcache"
	"github.com/ulikunitz/xz"
)

// multiCatalog lists paths 7 and 8, with an ETag so re-syncs see 304.
func multiCatalog(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", `"v1"`)
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
		io.WriteString(w, "/nix/store/"+hashPart(8)+"-upstream-1.0\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestNodeSeed: a seed pass ingests the whole catalog with no client
// request, and the node keeps serving after upstream is gone.
func TestNodeSeed(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	u.addPath(t, 8, []byte("second path"))
	catalog := multiCatalog(t)

	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	n.SyncCatalog(t.Context())
	n.SeedPass(t.Context())

	u.srv.Close()
	for _, idx := range []int{7, 8} {
		if st, doc := getBody(t, srv.URL+"/"+hashPart(idx)+".narinfo"); st != 200 {
			t.Fatalf("path %d after upstream gone: %d %s", idx, st, doc)
		}
	}
}

// TestSeederConvergence: two seeders ingesting the same upstream
// independently produce byte-identical trees, so a client stripes across
// both as interchangeable holders.
func TestSeederConvergence(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)
	mod := func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	}
	s1, srv1, host1 := newNode(t, u, mod)
	s2, srv2, host2 := newNode(t, u, mod)
	s1.SyncCatalog(t.Context())
	s2.SyncCatalog(t.Context())
	s1.SeedPass(t.Context())
	s2.SeedPass(t.Context())

	// Deterministic import: independent seeders serve identical narinfo,
	// root key included.
	_, doc1 := getBody(t, srv1.URL+"/"+hashPart(7)+".narinfo")
	_, doc2 := getBody(t, srv2.URL+"/"+hashPart(7)+".narinfo")
	if !bytes.Equal(doc1, doc2) || len(doc1) == 0 {
		t.Fatalf("seeders diverged:\n%s\nvs\n%s", doc1, doc2)
	}

	// A client with dead upstream pulls striped across both seeders.
	b, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.Peers = swarmPeers(host1, host2)
	})
	b.SyncCatalog(t.Context())
	ni, err := nixcache.ParseNarinfo(doc1)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("client fetch: %d", st)
	}
	stB, narB := getBody(t, srvB.URL+"/"+ni.URL)
	_, narA := getBody(t, srv1.URL+"/"+ni.URL)
	if stB != 200 || !bytes.Equal(narA, narB) {
		t.Fatalf("NAR: %d, %d vs %d bytes", stB, len(narB), len(narA))
	}
}

// TestCatalogConditionalGet: an unchanged list is not re-downloaded.
func TestCatalogConditionalGet(t *testing.T) {
	var hits, full int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		full++
		w.Header().Set("Etag", `"v1"`)
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer srv.Close()

	u := newUpstream(t, "zstd", nil)
	n, _, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{srv.URL + "/store-paths"}
	})
	if !n.SyncCatalog(t.Context()) {
		t.Fatal("first sync reported no change")
	}
	for range 2 {
		if n.SyncCatalog(t.Context()) {
			t.Fatal("304 sync reported change")
		}
	}
	if hits != 3 || full != 1 {
		t.Fatalf("hits %d (want 3), full downloads %d (want 1)", hits, full)
	}
}

// TestCatalogXZ: the deployment path, store-paths.xz.
func TestCatalogXZ(t *testing.T) {
	var buf bytes.Buffer
	xw, err := xz.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(xw, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	xw.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(buf.Bytes())
	}))
	defer srv.Close()

	u := newUpstream(t, "zstd", nil)
	n, nsrv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{srv.URL + "/store-paths.xz"}
	})
	n.SyncCatalog(t.Context())
	if st, _ := getBody(t, nsrv.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("xz-catalogued path: %d", st)
	}
}

// TestSeedRetry: a pass with failures reruns on the next unchanged sync,
// and a clean pass stops the retries.
func TestSeedRetry(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	u.addPath(t, 8, []byte("second path"))
	catalog := multiCatalog(t)

	// Path 8 fails transiently: drop its narinfo, then restore it.
	doc8 := u.docs["/"+hashPart(8)+".narinfo"]
	delete(u.docs, "/"+hashPart(8)+".narinfo")

	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.Seed = true
	})
	nixcache.SyncOnce(n, t.Context())
	if st, _ := getBody(t, srv.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("healthy path not seeded: %d", st)
	}

	u.docs["/"+hashPart(8)+".narinfo"] = doc8
	nixcache.SyncOnce(n, t.Context()) // catalog 304s: only the retry gate seeds
	u.srv.Close()
	if st, _ := getBody(t, srv.URL+"/"+hashPart(8)+".narinfo"); st != 200 {
		t.Fatalf("failed path not retried: %d", st)
	}
}
