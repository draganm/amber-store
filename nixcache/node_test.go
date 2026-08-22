package nixcache_test

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/draganm/amber-store/nixcache"
	"github.com/klauspost/compress/zstd"
)

func newNode(t *testing.T, u *upstream, mod func(*nixcache.NodeConfig)) (*nixcache.Node, *httptest.Server) {
	t.Helper()
	cfg := nixcache.NodeConfig{
		Dir:         t.TempDir(),
		Upstream:    u.srv.URL,
		TrustedKeys: []string{"test-1:" + b64(u.pub)},
	}
	if mod != nil {
		mod(&cfg)
	}
	n, err := nixcache.OpenNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(srv.Close)
	return n, srv
}

func b64(pub []byte) string {
	return base64.StdEncoding.EncodeToString(pub)
}

func TestNodeEndToEnd(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()

	node, srv := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	node.SyncCatalog(t.Context())

	resp, err := http.Get(srv.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("narinfo: %d %s", resp.StatusCode, doc)
	}
	n, err := nixcache.ParseNarinfo(doc)
	if err != nil {
		t.Fatal(err)
	}

	resp, err = http.Get(srv.URL + "/" + n.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("nar: %d", resp.StatusCode)
	}
	dec, err := zstd.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	nar, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(nar)) != n.NarSize {
		t.Fatalf("NAR size %d != %d", len(nar), n.NarSize)
	}

	// Second request is served from the local index (kill the upstream).
	u.srv.Close()
	resp, err = http.Get(srv.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("local hit: %d", resp.StatusCode)
	}
}

func TestNodeUncataloguedMiss(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	_, srv := newNode(t, u, nil)
	resp, err := http.Get(srv.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestNodeEviction(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	for i := 1; i <= 3; i++ {
		content := bytes.Repeat([]byte{byte(i)}, 4<<10)
		u.addPath(t, i, content)
	}
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 1; i <= 3; i++ {
			io.WriteString(w, "/nix/store/"+hashPart(i)+"-upstream-1.0\n")
		}
	}))
	defer catalog.Close()

	var dir string
	node, srv := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.BudgetBytes = 10 << 10 // fits two ~4KiB NARs, not three
		dir = c.Dir
	})
	node.SyncCatalog(t.Context())

	get := func(srvURL string, i int) int {
		resp, err := http.Get(srvURL + "/" + hashPart(i) + ".narinfo")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for i := 1; i <= 3; i++ {
		if st := get(srv.URL, i); st != 200 {
			t.Fatalf("ingest %d: status %d", i, st)
		}
	}

	// Path 1 (oldest, never hit again) was evicted: with upstream gone it
	// cannot be served, while 2 and 3 come from the local index.
	u.srv.Close()
	if st := get(srv.URL, 1); st != 502 {
		t.Fatalf("evicted path: status %d, want 502", st)
	}
	for i := 2; i <= 3; i++ {
		if st := get(srv.URL, i); st != 200 {
			t.Fatalf("kept path %d: status %d", i, st)
		}
	}
	srv.Close()
	node.Close()

	// Reopen: the policy reseeds from the index, kept paths still serve.
	n2, err := nixcache.OpenNode(nixcache.NodeConfig{
		Dir: dir, Upstream: "http://unreachable.invalid", BudgetBytes: 10 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	for i := 2; i <= 3; i++ {
		if st := get(srv2.URL, i); st != 200 {
			t.Fatalf("after reopen, path %d: status %d", i, st)
		}
	}
}

func TestNodePersistence(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	dir := ""
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()

	n1, srv1 := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		dir = c.Dir
	})
	n1.SyncCatalog(t.Context())
	if resp, err := http.Get(srv1.URL + "/" + hashPart(7) + ".narinfo"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("first fetch: %v", err)
	} else {
		resp.Body.Close()
	}
	srv1.Close()
	n1.Close()
	u.srv.Close() // reopened node must not need upstream

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
		t.Fatalf("reopened node: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "Compression: zstd") {
		t.Fatalf("narinfo: %s", body)
	}
	// NAR still streams from the reopened store.
	ni, err := nixcache.ParseNarinfo(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(srv2.URL + "/" + ni.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("nar after reopen: %d", resp.StatusCode)
	}
	dec, err := zstd.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	if _, err := io.Copy(io.Discard, dec); err != nil {
		t.Fatal(err)
	}
}
