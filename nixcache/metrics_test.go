package nixcache_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

// TestMetricsEndpoint: counters on the admin handler reflect activity.
func TestMetricsEndpoint(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()
	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	n.SyncCatalog(t.Context())
	if resp, err := http.Get(srv.URL + "/" + hashPart(7) + ".narinfo"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("ingest: %v %v", err, resp)
	} else {
		resp.Body.Close()
	}

	if resp, err := http.Get(srv.URL + "/" + hashPart(7) + ".narinfo"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("hit: %v %v", err, resp)
	} else {
		resp.Body.Close()
	}

	admin := httptest.NewServer(n.AdminHandler())
	defer admin.Close()
	resp, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`nix_cached_ingest_total{source="upstream"} 1`,
		`nix_cached_narinfo_requests_total{result="fetched"} 1`,
		`nix_cached_narinfo_requests_total{result="hit"} 1`,
		"nix_cached_catalog_paths 1",
		"nix_cached_indexed_paths 1",
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
