package nixcache_test

import (
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/nixcache"
)

// TestFetcherRetryAfter: upstream 429 with Retry-After yields a
// BackoffError carrying the deadline.
func TestFetcherRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	f := &nixcache.Fetcher{
		BaseURL: srv.URL,
		Trusted: map[string]ed25519.PublicKey{},
		Emit:    func(o fstree.Object) error { return nil },
	}
	_, err := f.FetchPath(t.Context(), hashPart(7))
	var be *nixcache.BackoffError
	if !errors.As(err, &be) {
		t.Fatalf("want BackoffError, got %v", err)
	}
	if d := time.Until(be.Until); d < time.Second || d > 3*time.Second {
		t.Fatalf("deadline %s off", d)
	}
}

// TestNodeBackoff: during backoff the node answers 503 with Retry-After
// and stops calling upstream.
func TestNodeBackoff(t *testing.T) {
	var hits atomic.Int64
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer limited.Close()
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := range 3 {
			w.Write([]byte("/nix/store/" + hashPart(i) + "-p\n"))
		}
	}))
	defer catalog.Close()

	n, srv, _ := newNode(t, newUpstream(t, "zstd", nil), func(c *nixcache.NodeConfig) {
		c.Upstream = limited.URL
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	n.SyncCatalog(t.Context())

	resp, err := http.Get(srv.URL + "/" + hashPart(0) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first: %d", resp.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/" + hashPart(1) + ".narinfo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second: %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After header")
	}
	if h := hits.Load(); h != 1 {
		t.Fatalf("upstream hit %d times, want 1", h)
	}
}
