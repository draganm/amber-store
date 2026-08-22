package nixcache

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// NodeConfig configures a local-only cache node.
type NodeConfig struct {
	Dir         string   // dedicated store + node state
	Upstream    string   // upstream cache base URL
	TrustedKeys []string // "name:base64" narinfo signing keys
	CatalogURLs []string // store-paths list URLs, synced periodically
	SyncEvery   time.Duration
	BudgetBytes int64 // stop ingesting above this store size; 0: unlimited
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
	n := &Node{cfg: cfg, store: store, catalog: catalog, trusted: trusted, root: root}
	n.server = &Server{
		Store:   store,
		Index:   n.indexRoot,
		Catalog: catalog.Contains,
		Fetch:   n.fetch,
	}
	return n, nil
}

func (n *Node) Close() error { return n.store.Close() }

func (n *Node) path(name string) string { return filepath.Join(n.cfg.Dir, name) }

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

// fetch ingests one catalogued path: fetch+verify into a memory buffer, then
// commit the objects and index the path. A failed gate writes nothing.
func (n *Node) fetch(ctx context.Context, hashpart string) (PathInfo, error) {
	if err := n.checkBudget(); err != nil {
		return PathInfo{}, err
	}
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
	if err := n.store.WriteBatch(buf.seq()); err != nil {
		return PathInfo{}, err
	}
	return pi, n.index(pi)
}

func (n *Node) index(pi PathInfo) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	buf := newObjectBuffer()
	root, err := Merge(n.root, []PathInfo{pi}, nil, n.store.Get, buf.emit)
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

func (n *Node) checkBudget() error {
	if n.cfg.BudgetBytes <= 0 {
		return nil
	}
	var size int64
	entries, err := os.ReadDir(n.path("store"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			size += fi.Size()
		}
	}
	if size > n.cfg.BudgetBytes {
		return fmt.Errorf("nixcache: store size %d exceeds budget %d", size, n.cfg.BudgetBytes)
	}
	return nil
}

func (n *Node) syncLoop(ctx context.Context) {
	n.SyncCatalog(ctx)
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
			n.SyncCatalog(ctx)
		}
	}
}

// SyncCatalog fetches all configured catalog URLs once and persists.
func (n *Node) SyncCatalog(ctx context.Context) {
	for _, url := range n.cfg.CatalogURLs {
		if err := n.addCatalogURL(ctx, url); err != nil {
			fmt.Fprintf(os.Stderr, "nixcache: catalog sync %s: %v\n", url, err)
		}
	}
	if len(n.cfg.CatalogURLs) > 0 {
		if err := n.catalog.Save(n.path("catalog")); err != nil {
			fmt.Fprintf(os.Stderr, "nixcache: catalog save: %v\n", err)
		}
	}
}

func (n *Node) addCatalogURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := cmp.Or(n.cfg.Client, http.DefaultClient).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	compression := "none"
	if filepath.Ext(url) == ".xz" {
		compression = "xz"
	}
	body, err := decompress(compression, resp.Body)
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = n.catalog.AddList(body)
	return err
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
