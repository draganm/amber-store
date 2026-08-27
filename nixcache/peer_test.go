package nixcache_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/nixcache"
)

func catalogServer(t *testing.T, idx int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(idx)+"-upstream-1.0\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

func getBody(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, body
}

func TestNodePeerFetch(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)

	a, srvA, hostA := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	a.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvA.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("seed node A: status %d", st)
	}

	// B reaches only A: upstream is unreachable.
	b, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.Peers = swarmPeers(hostA)
	})
	b.SyncCatalog(t.Context())

	st, doc := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo")
	if st != 200 {
		t.Fatalf("peer fetch: status %d %s", st, doc)
	}
	ni, err := nixcache.ParseNarinfo(doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(ni.Sigs) != 1 {
		t.Fatalf("sigs not preserved: %+v", ni)
	}
	stA, narA := getBody(t, srvA.URL+"/"+ni.URL)
	stB, narB := getBody(t, srvB.URL+"/"+ni.URL)
	if stA != 200 || stB != 200 || !bytes.Equal(narA, narB) {
		t.Fatalf("NAR mismatch: %d/%d, %d vs %d bytes", stA, stB, len(narA), len(narB))
	}

	// B is now independent of A.
	srvA.Close()
	hostA.Close()
	if st, _ := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("after peer gone: status %d", st)
	}
}

func TestNodePeerEnsure(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)
	a, srvA, hostA := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	a.SyncCatalog(t.Context())
	st, doc := getBody(t, srvA.URL+"/"+hashPart(7)+".narinfo")
	if st != 200 {
		t.Fatalf("seed: %d", st)
	}
	ni, err := nixcache.ParseNarinfo(doc)
	if err != nil {
		t.Fatal(err)
	}

	// B never saw the narinfo; the NAR route alone must pull the tree.
	_, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.Peers = swarmPeers(hostA)
	})
	stB, narB := getBody(t, srvB.URL+"/"+ni.URL)
	_, narA := getBody(t, srvA.URL+"/"+ni.URL)
	if stB != 200 || !bytes.Equal(narA, narB) {
		t.Fatalf("ensure: status %d, %d vs %d bytes", stB, len(narB), len(narA))
	}
}

// attachedPeer runs srv's swarm protocols on a host and returns a source
// dialing it from a second host.
func attachedPeer(t *testing.T, srv *nixcache.Server) *nixcache.PeerSource {
	t.Helper()
	server := testSwarm(t)
	srv.Attach(server)
	client := testSwarm(t)
	client.AddAddr(server.Addr())
	return &nixcache.PeerSource{Swarm: client, ID: server.ID()}
}

func TestPeerProbeNeverFetches(t *testing.T) {
	srv := &nixcache.Server{
		Store:   newRecStore(),
		Index:   func() key.Key { return key.Key{} },
		Catalog: func(string) bool { return true },
		Fetch: func(ctx context.Context, hp string) (nixcache.PathInfo, error) {
			t.Error("peer probe reached Fetch")
			return nixcache.PathInfo{}, nil
		},
	}
	src := attachedPeer(t, srv)
	_, err := nixcache.Probe(src, hashPart(7), nil)
	if !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestPeerObjectsKeyCap(t *testing.T) {
	src := attachedPeer(t, &nixcache.Server{Store: newRecStore(), Index: func() key.Key { return key.Key{} }})
	keys := make([]key.Key, 8193)
	if _, err := src.FetchObjects(t.Context(), keys, nil); err == nil {
		t.Fatal("oversized key list accepted")
	}
}

type slowStore struct {
	*recStore
	gate chan struct{}
}

func (s *slowStore) Get(k key.Key) ([]byte, error) {
	<-s.gate
	return s.recStore.Get(k)
}

func TestPeerConcurrencyLimit(t *testing.T) {
	st := newRecStore()
	root := buildTree(t, st, []byte("concurrency"))
	slow := &slowStore{recStore: st, gate: make(chan struct{})}
	src := attachedPeer(t, &nixcache.Server{Store: slow, Index: func() key.Key { return key.Key{} }, PeerConcurrency: 1})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		src.ReachableKeys(t.Context(), root) // blocks on the gate
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := src.ReachableKeys(t.Context(), root); err == nil {
		t.Fatal("second request admitted past the concurrency cap")
	}
	close(slow.gate)
	wg.Wait()
}

func TestPeerByteRate(t *testing.T) {
	st := newRecStore()
	content := make([]byte, 96<<10)
	rand.Read(content) // incompressible, so ~96KiB actually travel
	root := buildTree(t, st, content)
	src := attachedPeer(t, &nixcache.Server{Store: st, Index: func() key.Key { return key.Key{} }, PeerByteRate: 64 << 10})

	start := time.Now()
	keys, err := src.ReachableKeys(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	objs, err := src.FetchObjects(t.Context(), keys, func(b int) { n += int64(b) })
	if err != nil || len(objs) == 0 {
		t.Fatalf("fetch: %v, %d objects", err, len(objs))
	}
	if n < 96<<10 {
		t.Fatalf("only %d bytes travelled", n)
	}
	// 96KiB payload at 64KiB/s with a one-second burst: >= ~500ms.
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("rate cap not applied: served in %v", elapsed)
	}
}

func TestNodePeerFetchDeadPeer(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)
	a, srvA, hostA := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	a.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvA.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("seed: %d", st)
	}

	dead := testSwarm(t)
	deadInfo := swarmPeers(dead)
	dead.Close()
	b, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.Peers = append(deadInfo, swarmPeers(hostA)...)
	})
	b.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("fetch with dead peer in the list: %d", st)
	}
}

// TestNodeIndexSync exercises the tracker role of synced indexes: B learns
// what A holds without a catalog, survives its own GC without re-syncing,
// and serves the path end to end.
func TestNodeIndexSync(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)
	a, srvA, hostA := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	a.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvA.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("seed: %d", st)
	}

	// B has no catalog at all: only the synced peer index vouches.
	b, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.Peers = swarmPeers(hostA)
	})
	b.SyncPeers(t.Context())
	if _, err := b.GC(0); err != nil { // peer index must survive the sweep
		t.Fatal(err)
	}

	st, doc := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo")
	if st != 200 {
		t.Fatalf("index-sync fetch: %d %s", st, doc)
	}
	ni, err := nixcache.ParseNarinfo(doc)
	if err != nil {
		t.Fatal(err)
	}
	_, narA := getBody(t, srvA.URL+"/"+ni.URL)
	stB, narB := getBody(t, srvB.URL+"/"+ni.URL)
	if stB != 200 || !bytes.Equal(narA, narB) {
		t.Fatalf("NAR: %d, %d vs %d bytes", stB, len(narB), len(narA))
	}
}

// A peer pull that finds every object already on disk (evicted, not yet
// collected) writes nothing, so only explicit observation keeps the
// re-indexed tree out of a concurrent sweep.
func TestPeerReindexDuringGCMark(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	catalog := catalogServer(t, 7)
	a, srvA, hostA := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	a.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvA.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("seed node A: status %d", st)
	}
	b, srvB, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Upstream = "http://unreachable.invalid"
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
		c.Peers = swarmPeers(hostA)
	})
	b.SyncCatalog(t.Context())
	if st, _ := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
		t.Fatalf("peer fetch: status %d", st)
	}
	if err := nixcache.Unindex(b, hashPart(7)); err != nil {
		t.Fatal(err)
	}

	nixcache.SetMidMark(b, func() {
		if st, _ := getBody(t, srvB.URL+"/"+hashPart(7)+".narinfo"); st != 200 {
			t.Errorf("mid-mark peer fetch: status %d", st)
		}
	})
	if _, err := b.GC(0); err != nil {
		t.Fatal(err)
	}
	nixcache.SetMidMark(b, nil)
	if _, err := b.Liveness(); err != nil {
		t.Fatalf("index dangles after gc: %v", err)
	}
}

// floodStore answers every object request with the same unrelated record,
// over and over, regardless of the keys asked for.
type floodStore struct {
	*recStore
	mu    sync.Mutex
	rec   key.Key
	times int
}

func (f *floodStore) set(rec key.Key, times int) {
	f.mu.Lock()
	f.rec, f.times = rec, times
	f.mu.Unlock()
}

func (f *floodStore) ViewRecordSpans(_ []key.Key, _ int, fn func([]byte) error) error {
	f.mu.Lock()
	rec, times := f.rec, f.times
	f.mu.Unlock()
	for range times {
		if err := f.recStore.ViewRecord(rec, fn); err != nil {
			return err
		}
	}
	return nil
}

func TestPeerRejectsRecordsOutsideTheBatch(t *testing.T) {
	st := newRecStore()
	junk, err := fstree.EncodeBlob([]byte("not what was asked for"))
	if err != nil {
		t.Fatal(err)
	}
	st.emit(junk)
	wanted, err := fstree.EncodeBlob([]byte("wanted"))
	if err != nil {
		t.Fatal(err)
	}
	flood := &floodStore{recStore: st, rec: junk.Key, times: 1000}
	src := attachedPeer(t, &nixcache.Server{Store: flood, Index: func() key.Key { return key.Key{} }})

	var emitted int
	err = src.StreamRecords(t.Context(), []key.Key{wanted.Key}, nil, func(amberpack.RawRecord) error { emitted++; return nil })
	if err == nil || emitted != 0 {
		t.Fatalf("stream of unrequested records: err=%v, emitted=%d", err, emitted)
	}
	objs, err := src.FetchObjects(t.Context(), []key.Key{wanted.Key}, nil)
	if err == nil || len(objs) != 0 {
		t.Fatalf("fetch of unrequested records: err=%v, objs=%d", err, len(objs))
	}

	// The requested record itself is accepted exactly once.
	st.emit(wanted)
	flood.set(wanted.Key, 2)
	emitted = 0
	err = src.StreamRecords(t.Context(), []key.Key{wanted.Key}, nil, func(amberpack.RawRecord) error { emitted++; return nil })
	if err == nil || emitted != 1 {
		t.Fatalf("duplicate record: err=%v, emitted=%d", err, emitted)
	}
}

func TestPeerStalledReaderReleasesSlot(t *testing.T) {
	st := newRecStore()
	payload := make([]byte, 64<<10)
	rand.Read(payload) // incompressible, so the response really fills the window
	big, err := fstree.EncodeBlob(payload)
	if err != nil {
		t.Fatal(err)
	}
	st.emit(big)
	root := buildTree(t, st, []byte("small"))
	flood := &floodStore{recStore: st, rec: big.Key, times: 1024}
	src := attachedPeer(t, &nixcache.Server{Store: flood, Index: func() key.Key { return key.Key{} }, PeerConcurrency: 1, PeerWriteTimeout: 200 * time.Millisecond})

	stalled, err := nixcache.StalledObjectsRequest(src, []key.Key{big.Key})
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	time.Sleep(100 * time.Millisecond) // let the server fill the flow-control window
	if _, err := src.ReachableKeys(t.Context(), root); err != nil {
		t.Fatalf("slot never released by the stalled stream: %v", err)
	}
}
