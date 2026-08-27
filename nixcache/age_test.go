package nixcache_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/nixcache"
)

// mutableCatalog serves the paths in the current version with a
// version-derived ETag.
type mutableCatalog struct {
	srv  *httptest.Server
	v    atomic.Int32
	sets map[int32][]int
}

func newMutableCatalog(t *testing.T, sets map[int32][]int) *mutableCatalog {
	t.Helper()
	c := &mutableCatalog{sets: sets}
	c.v.Store(1)
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := c.v.Load()
		tag := `"v` + string(rune('0'+v)) + `"`
		if r.Header.Get("If-None-Match") == tag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", tag)
		for _, idx := range c.sets[v] {
			io.WriteString(w, "/nix/store/"+hashPart(idx)+"-upstream-1.0\n")
		}
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// TestCatalogAging checks the timestamp lifecycle: set on catalog
// departure, cleared on return, record deleted after the TTL.
func TestCatalogAging(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	u.addPath(t, 8, []byte("second path"))
	cat := newMutableCatalog(t, map[int32][]int{
		1: {7, 8},
		2: {7},
		3: {7, 8},
	})

	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{cat.srv.URL + "/store-paths"}
		c.CatalogTTL = time.Hour
	})
	n.SyncCatalog(t.Context())
	for _, idx := range []int{7, 8} {
		if st, _ := getBody(t, srv.URL+"/"+hashPart(idx)+".narinfo"); st != 200 {
			t.Fatalf("ingest path %d", idx)
		}
	}

	now := time.Now()
	nixcache.AgePass(n, now)
	if pi, err := nixcache.LookupPath(n, hashPart(8)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("in-catalog path aged: %+v, %v", pi, err)
	}

	// 8 leaves the catalog
	cat.v.Store(2)
	n.SyncCatalog(t.Context())
	nixcache.AgePass(n, now)
	pi, err := nixcache.LookupPath(n, hashPart(8))
	if err != nil || pi.AgedAt == 0 {
		t.Fatalf("departed path not stamped: %+v, %v", pi, err)
	}
	if pi, err := nixcache.LookupPath(n, hashPart(7)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("current path stamped: %+v, %v", pi, err)
	}

	// 8 returns
	cat.v.Store(3)
	n.SyncCatalog(t.Context())
	nixcache.AgePass(n, now)
	if pi, err := nixcache.LookupPath(n, hashPart(8)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("returned path still stamped: %+v, %v", pi, err)
	}

	// 8 leaves again and crosses the TTL
	cat.v.Store(2)
	n.SyncCatalog(t.Context())
	nixcache.AgePass(n, now)
	nixcache.AgePass(n, now.Add(2*time.Hour))
	if _, err := nixcache.LookupPath(n, hashPart(8)); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("expired path still indexed: %v", err)
	}
	if _, err := nixcache.LookupPath(n, hashPart(7)); err != nil {
		t.Fatalf("current path dropped: %v", err)
	}
}

// TestAgePassNeedsFullView: nothing is aged while a catalog URL
// cannot be fetched.
func TestAgePassNeedsFullView(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	cat := newMutableCatalog(t, map[int32][]int{1: {7}})

	n, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{cat.srv.URL + "/store-paths", "http://unreachable.invalid/list"}
		c.CatalogTTL = time.Hour
	})
	n.SyncCatalog(t.Context())
	if st, _ := getBody(t, srv.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatal("ingest failed")
	}
	nixcache.AgePass(n, time.Now())
	if pi, err := nixcache.LookupPath(n, hashPart(7)); err != nil || pi.AgedAt != 0 {
		t.Fatalf("aged with partial catalog view: %+v, %v", pi, err)
	}
}

// TestSeedSkipsDeparted: seed passes only fetch paths in the latest
// lists.
func TestSeedSkipsDeparted(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	u.addPath(t, 8, []byte("second path"))
	cat := newMutableCatalog(t, map[int32][]int{1: {7, 8}, 2: {7}})

	n, _, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{cat.srv.URL + "/store-paths"}
		c.Seed = true
	})
	n.SyncCatalog(t.Context())
	cat.v.Store(2)
	n.SyncCatalog(t.Context())
	if failed := n.SeedPass(t.Context()); failed != 0 {
		t.Fatalf("%d seed failures", failed)
	}
	if _, err := nixcache.LookupPath(n, hashPart(7)); err != nil {
		t.Fatalf("current path not seeded: %v", err)
	}
	if _, err := nixcache.LookupPath(n, hashPart(8)); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("departed path was seeded: %v", err)
	}
}

// TestSeederPinsCatalog: listed paths survive seeding under a full
// budget; once departed they get evicted.
func TestSeederPinsCatalog(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	cat := newMutableCatalog(t, map[int32][]int{1: {7}, 2: {}})

	n, _, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{cat.srv.URL + "/store-paths"}
		c.Seed = true
		c.CatalogTTL = time.Hour
		c.BudgetBytes = 1
	})
	n.SyncCatalog(t.Context())
	now := time.Now()
	nixcache.AgePass(n, now)
	if failed := n.SeedPass(t.Context()); failed != 0 {
		t.Fatalf("%d seed failures", failed)
	}
	if _, err := nixcache.LookupPath(n, hashPart(7)); err != nil {
		t.Fatalf("pinned path evicted during seeding: %v", err)
	}

	cat.v.Store(2)
	n.SyncCatalog(t.Context())
	nixcache.AgePass(n, now)
	if _, err := nixcache.LookupPath(n, hashPart(7)); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("unpinned path survived the standing budget breach: %v", err)
	}
}
