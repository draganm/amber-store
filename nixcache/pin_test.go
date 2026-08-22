package nixcache_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/nixcache"
)

func adminDo(t *testing.T, admin *httptest.Server, method, path string) int {
	t.Helper()
	req, err := http.NewRequest(method, admin.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestExplicitPin: a pinned path survives catalog departure and TTL
// expiry; unpinning lets aging delete it again.
func TestExplicitPin(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	u.addPath(t, 8, []byte("second path"))
	cat := newMutableCatalog(t, map[int32][]int{1: {7, 8}, 2: {7}})

	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{cat.srv.URL + "/store-paths"}
		c.CatalogTTL = time.Hour
	})
	admin := httptest.NewServer(n.AdminHandler())
	defer admin.Close()

	n.SyncCatalog(t.Context())
	for _, idx := range []int{7, 8} {
		if st, _ := getBody(t, srv.URL+"/"+hashPart(idx)+".narinfo"); st != 200 {
			t.Fatalf("ingest path %d", idx)
		}
	}
	if st := adminDo(t, admin, http.MethodPost, "/-/pin/"+hashPart(8)); st != 200 {
		t.Fatalf("pin status %d", st)
	}
	if st := adminDo(t, admin, http.MethodPost, "/-/pin/not-a-hashpart"); st != 400 {
		t.Fatalf("bad pin status %d", st)
	}

	now := time.Now()
	cat.v.Store(2)
	n.SyncCatalog(t.Context())
	nixcache.AgePass(n, now)
	nixcache.AgePass(n, now.Add(2*time.Hour))
	if pi, err := nixcache.LookupPath(n, hashPart(8)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("pinned path aged: %+v, %v", pi, err)
	}

	if st := adminDo(t, admin, http.MethodDelete, "/-/pin/"+hashPart(8)); st != 200 {
		t.Fatalf("unpin status %d", st)
	}
	nixcache.AgePass(n, now)
	nixcache.AgePass(n, now.Add(2*time.Hour))
	if _, err := nixcache.LookupPath(n, hashPart(8)); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("unpinned path still indexed: %v", err)
	}
}

// TestPinPersists: pins survive a restart.
func TestPinPersists(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	cat := newMutableCatalog(t, map[int32][]int{1: {7}, 2: {}})

	dir := t.TempDir()
	cfg := nixcache.NodeConfig{
		Dir:         dir,
		Upstream:    u.srv.URL,
		TrustedKeys: []string{"test-1:" + b64(u.pub)},
		CatalogURLs: []string{cat.srv.URL + "/store-paths"},
		CatalogTTL:  time.Hour,
	}
	n, err := nixcache.OpenNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(n.Handler())
	n.SyncCatalog(t.Context())
	if st, _ := getBody(t, srv.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatal("ingest failed")
	}
	if err := n.Pin(hashPart(7)); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	n2, err := nixcache.OpenNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	cat.v.Store(2)
	n2.SyncCatalog(t.Context())
	now := time.Now()
	nixcache.AgePass(n2, now)
	nixcache.AgePass(n2, now.Add(2*time.Hour))
	if pi, err := nixcache.LookupPath(n2, hashPart(7)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("pin lost across restart: %+v, %v", pi, err)
	}
}
