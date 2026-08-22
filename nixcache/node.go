package nixcache

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remotesync"
	ikey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// NodeConfig configures a local-only cache node.
type NodeConfig struct {
	Dir         string   // dedicated store + node state
	Upstream    string   // upstream cache base URL
	TrustedKeys []string // "name:base64" narinfo signing keys
	CatalogURLs []string // store-paths list URLs, synced periodically
	// Swarm joins the swarm. Nil disables peering. Peers are probed before
	// upstream.
	Swarm           *Swarm
	Peers           []netaddr.EndpointAddr
	PeerConcurrency int   // cap on in-flight peer-serving requests; 0: 4, seeders 64
	PeerByteRate    int64 // peer-serving bandwidth cap, bytes/second; 0: unlimited
	// Seed ingests every catalogued path eagerly, so the node holds the
	// closure before anyone asks: the seeder role.
	Seed        bool
	CatalogTTL  time.Duration // drop paths this long after leaving the catalog; 0: keep forever
	SyncEvery   time.Duration
	BudgetBytes int64 // eviction target for the NarSize sum. 0: unlimited
	Client      *http.Client
}

// Node owns a dedicated store and serves the substituter endpoint from it,
// ingesting catalogued paths from upstream on demand.
type Node struct {
	cfg     NodeConfig
	store   *packstore.Store
	catalog *Catalog
	server  *Server
	trusted map[string]ed25519.PublicKey

	mu   sync.Mutex // serializes index updates
	root key.Key

	// quiesce protocol of specs/gc.qnt: ingests hold it shared from object
	// commit through index publication, GC's mark..sweep holds it exclusively
	gcMu    sync.RWMutex
	cycleMu sync.Mutex // held for a whole GC cycle

	midMark   func()       // test hook, runs between GC's mark and sweep
	midIngest func() error // test hook, runs between object commit and index publication

	peerMu    sync.Mutex
	peerRoots map[ikey.EndpointID]key.Key  // last synced index root per peer
	peerSet   map[ikey.EndpointID]struct{} // static peers plus discovered ones

	catalogTags sync.Map // catalog URL -> ETag of the last ingested list
	catalogMu   sync.RWMutex
	catalogCur  map[string]*Catalog // last fetched list per URL
	swarmStats  *remotesync.Stats   // per-peer speed estimates, all pulls
	seedRetry   bool                // sync-loop state: last seed pass had failures

	evict          *evictPolicy // nil without a budget
	evictedSinceGC atomic.Int64
	gcAuto         atomic.Bool
}

// OpenNode opens the store and state under cfg.Dir.
func OpenNode(cfg NodeConfig) (*Node, error) {
	trusted := map[string]ed25519.PublicKey{}
	for _, s := range cfg.TrustedKeys {
		name, pub, err := ParseTrustedKey(s)
		if err != nil {
			return nil, err
		}
		trusted[name] = pub
	}
	catalog, err := LoadCatalog(filepath.Join(cfg.Dir, "catalog"))
	if err != nil {
		return nil, err
	}
	root, err := loadRoot(filepath.Join(cfg.Dir, "index-root"))
	if err != nil {
		return nil, err
	}
	store, err := packstore.Open(filepath.Join(cfg.Dir, "store"))
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, store: store, catalog: catalog, trusted: trusted, root: root,
		peerRoots: map[ikey.EndpointID]key.Key{}, peerSet: map[ikey.EndpointID]struct{}{},
		catalogCur: map[string]*Catalog{}, swarmStats: remotesync.NewStats()}
	if cfg.BudgetBytes > 0 {
		n.evict = newEvictPolicy(cfg.BudgetBytes)
		if err := n.seedEvict(); err != nil {
			store.Close()
			return nil, err
		}
	}
	n.server = &Server{
		Store:   store,
		Index:   n.indexRoot,
		Catalog: n.servable,
		Fetch:   n.fetch,
		Touch:   n.touch,

		PeerConcurrency: peerConcurrency(cfg),
		PeerByteRate:    cfg.PeerByteRate,
	}
	if cfg.Swarm != nil {
		n.server.Attach(cfg.Swarm)
		n.server.Ensure = n.ensure
		for _, a := range cfg.Peers {
			n.addPeer(a)
		}
	}
	return n, nil
}

// peerConcurrency lets seeders serve a crowd: 64 streams saturate a
// seeder's uplink where 4 bottleneck the swarm. A leaf's uplink binds
// before its slot count, so the abuse-bounding 4 stays.
func peerConcurrency(cfg NodeConfig) int {
	if cfg.PeerConcurrency == 0 && cfg.Seed {
		return 64
	}
	return cfg.PeerConcurrency
}

// seedEvict rebuilds the policy from the persisted index. Queue positions
// and hit counters are process state and start over.
func (n *Node) seedEvict() error {
	if n.root == (key.Key{}) {
		return nil
	}
	for e, err := range indexEntries(n.root, n.store.Get) {
		if err != nil {
			return err
		}
		pi, err := DecodeRecord(e.LinkTarget)
		if err != nil {
			return err
		}
		n.evict.seed(string(e.Name), pi.NarSize)
	}
	return nil
}

func (n *Node) touch(hashpart string) {
	if n.evict != nil {
		n.evict.Touch(hashpart)
	}
}

// Pin excludes an indexed path from eviction.
func (n *Node) Pin(hashpart string) {
	if n.evict != nil {
		n.evict.Pin(hashpart)
	}
}

func (n *Node) Unpin(hashpart string) {
	if n.evict != nil {
		n.evict.Unpin(hashpart)
	}
}

func (n *Node) Close() error { return n.store.Close() }

func (n *Node) path(name string) string { return filepath.Join(n.cfg.Dir, name) }

// AdminHandler serves privileged endpoints. Only expose it on a
// local Unix socket.
func (n *Node) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /-/gc", func(w http.ResponseWriter, r *http.Request) {
		stats, err := n.GC(0.5)
		if errors.Is(err, ErrGCRunning) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "segments compacted: %d\nrecords copied: %d\nbytes copied: %d\nbytes freed: %d\n",
			stats.SegmentsCompacted, stats.RecordsCopied, stats.BytesCopied, stats.BytesFreed)
	})
	return mux
}

// Handler returns the substituter endpoint.
func (n *Node) Handler() http.Handler { return n.server }

// Run serves the endpoint on l and keeps the catalog synced until ctx ends.
func (n *Node) Run(ctx context.Context, l net.Listener) error {
	go n.syncLoop(ctx)
	srv := &http.Server{Handler: n.server}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	if err := srv.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (n *Node) indexRoot() key.Key {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.root
}

// fetch ingests one catalogued path, probing peers (chunk-level pull)
// before upstream (whole NAR, nix's protocol leaves no choice).
func (n *Node) fetch(ctx context.Context, hashpart string) (PathInfo, error) {
	if n.cfg.Swarm != nil {
		if pi, err := n.fetchFromPeers(ctx, hashpart); err == nil {
			return pi, nil
		}
	}
	return n.fetchOrigin(ctx, hashpart)
}

// fetchOrigin fetches upstream, verifying into a memory buffer first, then
// committing the objects and indexing the path. A failed gate writes nothing.
func (n *Node) fetchOrigin(ctx context.Context, hashpart string) (PathInfo, error) {
	buf := newObjectBuffer()
	f := &Fetcher{
		BaseURL: n.cfg.Upstream,
		Trusted: n.trusted,
		Emit:    buf.emit,
		Get:     buf.get,
		Client:  n.cfg.Client,
	}
	pi, err := f.FetchPath(ctx, hashpart)
	if err != nil {
		return PathInfo{}, err
	}
	n.gcMu.RLock()
	defer n.gcMu.RUnlock()
	if err := n.store.WriteBatch(buf.seq()); err != nil {
		return PathInfo{}, err
	}
	if n.midIngest != nil {
		if err := n.midIngest(); err != nil {
			return PathInfo{}, err
		}
	}
	if err := n.publish([]PathInfo{pi}, nil); err != nil {
		return PathInfo{}, err
	}
	return pi, n.enforceBudget(hashpart, pi.NarSize)
}

// enforceBudget admits the new path and unindexes victims over budget.
// Their objects stay until GC. Runs under the shared gcMu like any writer.
func (n *Node) enforceBudget(hashpart string, narSize uint64) error {
	if n.evict == nil {
		return nil
	}
	if n.cfg.Seed && n.currentHas(hashpart) {
		n.evict.Pin(hashpart)
	}
	n.evict.Admit(hashpart, narSize)
	return n.evictOverBudget()
}

// evictOverBudget unindexes the policy's victims. A quarter budget of
// evictions triggers a background GC.
func (n *Node) evictOverBudget() error {
	if n.evict == nil {
		return nil
	}
	victims, bytes := n.evict.Evict()
	if len(victims) == 0 {
		return nil
	}
	if err := n.publish(nil, victims); err != nil {
		return err
	}
	if n.evictedSinceGC.Add(bytes) >= n.cfg.BudgetBytes/4 &&
		n.gcAuto.CompareAndSwap(false, true) {
		n.evictedSinceGC.Store(0)
		go func() {
			defer n.gcAuto.Store(false)
			if _, err := n.GC(0.5); err != nil && !errors.Is(err, ErrGCRunning) {
				slog.Warn("auto gc", "error", err)
			}
		}()
	}
	return nil
}

// publish merges upserts and deletes into the index and swaps the root,
// objects first, so a crash leaves the previous consistent index.
func (n *Node) publish(upserts []PathInfo, deletes []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	buf := newObjectBuffer()
	root, err := Merge(n.root, upserts, deletes, n.store.Get, buf.emit)
	if err != nil {
		return err
	}
	if err := n.store.WriteBatch(buf.seq()); err != nil {
		return err
	}
	if err := saveRoot(n.path("index-root"), root); err != nil {
		return err
	}
	n.root = root
	return nil
}

func (n *Node) syncLoop(ctx context.Context) {
	n.syncOnce(ctx)
	if n.cfg.SyncEvery <= 0 {
		return
	}
	t := time.NewTicker(n.cfg.SyncEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.syncOnce(ctx)
		}
	}
}

// syncOnce refreshes catalog and peer indexes. A seeder walks the catalog
// only when it changed or the previous pass left failures to retry.
func (n *Node) syncOnce(ctx context.Context) {
	changed := n.SyncCatalog(ctx)
	if err := n.agePass(time.Now()); err != nil {
		fmt.Fprintf(os.Stderr, "nixcache: age pass: %v\n", err)
	}
	n.SyncPeers(ctx)
	if n.cfg.Seed && (changed || n.seedRetry) {
		n.seedRetry = n.SeedPass(ctx) > 0
	}
}

// currentHas reports whether hp is in any of the last fetched
// catalog lists.
func (n *Node) currentHas(hp string) bool {
	n.catalogMu.RLock()
	defer n.catalogMu.RUnlock()
	for _, c := range n.catalogCur {
		if c.Contains(hp) {
			return true
		}
	}
	return false
}

// agePass stamps paths missing from the current catalogs, clears the
// stamp on return, and deletes past CatalogTTL. Skipped unless every
// catalog URL has been fetched, so download failures age nothing.
func (n *Node) agePass(now time.Time) error {
	n.catalogMu.RLock()
	full := len(n.catalogCur) == len(n.cfg.CatalogURLs)
	n.catalogMu.RUnlock()
	if n.cfg.CatalogTTL <= 0 || !full {
		return nil
	}
	root := n.indexRoot()
	if root == (key.Key{}) {
		return nil
	}
	var upserts []PathInfo
	var deletes []string
	for e, err := range indexEntries(root, n.store.Get) {
		if err != nil {
			return err
		}
		pi, err := DecodeRecord(e.LinkTarget)
		if err != nil {
			return err
		}
		hp := string(e.Name)
		cur := n.currentHas(hp)
		if n.evict != nil && n.cfg.Seed {
			if cur {
				n.evict.Pin(hp)
			} else {
				n.evict.Unpin(hp)
			}
		}
		switch {
		case cur && pi.AgedAt != 0:
			pi.AgedAt = 0
			upserts = append(upserts, pi)
		case !cur && pi.AgedAt == 0:
			pi.AgedAt = now.Unix()
			upserts = append(upserts, pi)
		case !cur && now.Unix()-pi.AgedAt >= int64(n.cfg.CatalogTTL.Seconds()):
			deletes = append(deletes, hp)
		}
	}
	n.gcMu.RLock()
	defer n.gcMu.RUnlock()
	if len(upserts) > 0 || len(deletes) > 0 {
		if err := n.publish(upserts, deletes); err != nil {
			return err
		}
	}
	return n.evictOverBudget()
}

// servable gates narinfo misses: the catalog covers upstream, and a synced
// peer index covers paths our catalog has not caught up with.
func (n *Node) servable(hashpart string) bool {
	return n.catalog.Contains(hashpart) || n.peersHave(hashpart)
}

// SyncCatalog fetches all configured catalog URLs once, persists, and
// reports whether any list was re-ingested (vs answered 304).
func (n *Node) SyncCatalog(ctx context.Context) bool {
	changed := false
	for _, url := range n.cfg.CatalogURLs {
		ingested, err := n.addCatalogURL(ctx, url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "nixcache: catalog sync %s: %v\n", url, err)
		}
		changed = changed || ingested
	}
	if changed {
		if err := n.catalog.Save(n.path("catalog")); err != nil {
			fmt.Fprintf(os.Stderr, "nixcache: catalog save: %v\n", err)
		}
	}
	return changed
}

func (n *Node) addCatalogURL(ctx context.Context, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	if tag, ok := n.catalogTags.Load(url); ok {
		req.Header.Set("If-None-Match", tag.(string))
	}
	resp, err := cmp.Or(n.cfg.Client, http.DefaultClient).Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(resp.Status)
	}
	compression := "none"
	if filepath.Ext(url) == ".xz" {
		compression = "xz"
	}
	body, err := decompress(compression, resp.Body)
	if err != nil {
		return false, err
	}
	defer body.Close()
	cur := &Catalog{}
	if _, err := cur.AddList(body); err != nil {
		return false, err
	}
	n.catalog.Merge(cur)
	n.catalogMu.Lock()
	n.catalogCur[url] = cur
	n.catalogMu.Unlock()
	if tag := resp.Header.Get("Etag"); tag != "" {
		n.catalogTags.Store(url, tag)
	}
	return true, nil
}

// objectBuffer holds a fetch's objects until the ingest gate passes.
type objectBuffer struct {
	order []key.Key
	data  map[key.Key][]byte
}

func newObjectBuffer() *objectBuffer {
	return &objectBuffer{data: map[key.Key][]byte{}}
}

func (b *objectBuffer) emit(o fstree.Object) error {
	if _, ok := b.data[o.Key]; !ok {
		b.data[o.Key] = o.Bytes
		b.order = append(b.order, o.Key)
	}
	return nil
}

func (b *objectBuffer) get(k key.Key) ([]byte, error) {
	d, ok := b.data[k]
	if !ok {
		return nil, fmt.Errorf("nixcache: object %s not in fetch buffer", k)
	}
	return d, nil
}

func (b *objectBuffer) seq() func(func(packstore.Object, error) bool) {
	return func(yield func(packstore.Object, error) bool) {
		for _, k := range b.order {
			if !yield(packstore.Object{Key: k, Data: b.data[k]}, nil) {
				return
			}
		}
	}
}

func loadRoot(path string) (key.Key, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return key.Key{}, nil
	}
	if err != nil {
		return key.Key{}, err
	}
	raw, err := hex.DecodeString(string(b))
	if err != nil {
		return key.Key{}, fmt.Errorf("nixcache: index root file: %w", err)
	}
	return key.Parse(raw)
}

func saveRoot(path string, root key.Key) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(hex.EncodeToString(root[:])), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
