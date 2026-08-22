package nixcache_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

// TestRestartBeforePublish: after a crash between object commit and
// index publication, the path is absent, GC reclaims the orphans, and
// a refetch succeeds.
func TestRestartBeforePublish(t *testing.T) {
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
	nixcache.SetMidIngest(n1, func() error { return errors.New("injected crash") })
	if resp, err := http.Get(srv1.URL + "/" + hashPart(7) + ".narinfo"); err != nil || resp.StatusCode == 200 {
		t.Fatalf("crashed ingest must not serve: %v %v", err, resp)
	} else {
		resp.Body.Close()
	}
	srv1.Close()
	n1.Close()

	n2, err := nixcache.OpenNode(nixcache.NodeConfig{
		Dir:         dir,
		Upstream:    u.srv.URL,
		TrustedKeys: []string{"test-1:" + b64(u.pub)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if _, err := nixcache.LookupPath(n2, hashPart(7)); err == nil {
		t.Fatal("unpublished path is in the index after restart")
	}
	stats, err := n2.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.BytesFreed == 0 {
		t.Fatal("GC freed nothing; the crashed ingest's objects leaked")
	}

	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	resp, err := http.Get(srv2.URL + "/" + hashPart(7) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("refetch after restart: %d", resp.StatusCode)
	}
}
