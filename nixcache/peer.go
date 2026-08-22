package nixcache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/keylist"
	"github.com/draganm/amber-store/narexport"
	"github.com/draganm/amber-store/remotesync"
	"github.com/tmc/go-iroh/iroh"
	ikey "github.com/tmc/go-iroh/key"
)

// PeerSource is remotesync.Source over a peer's swarm protocols. Peers are
// untrusted: Pull re-verifies every object and the caller gates the tree
// against the signed NarHash.
type PeerSource struct {
	Swarm *Swarm
	ID    ikey.EndpointID
}

func (p *PeerSource) request(ctx context.Context, req request) (*iroh.Stream, error) {
	return p.Swarm.open(ctx, p.ID, req)
}

func (p *PeerSource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	s, err := p.request(ctx, request{kind: reqKeys, root: root})
	if err != nil {
		return nil, err
	}
	defer s.CancelRead(0)
	b, err := io.ReadAll(io.LimitReader(s, 64<<20))
	if err != nil {
		return nil, err
	}
	return keylist.Parse(b)
}

func (p *PeerSource) FetchObjects(ctx context.Context, keys []key.Key, onBytes func(n int)) ([]fstree.Object, error) {
	s, counted, err := p.pack(ctx, keys, onBytes)
	if err != nil {
		return nil, err
	}
	defer s.CancelRead(0)
	want := newRequested(keys)
	var objs []fstree.Object
	for o, err := range amberpack.NewReader(counted).All() {
		if err != nil {
			return nil, err
		}
		if err := want.take(o.Key); err != nil {
			return nil, err
		}
		objs = append(objs, o)
	}
	return objs, nil
}

// requested bounds a peer's response to the keys asked for, once each;
// the store verifies content, this bounds volume.
type requested map[key.Key]bool

func (r requested) take(k key.Key) error {
	if !r[k] {
		return fmt.Errorf("nixcache: peer sent unrequested or duplicate record %s", k)
	}
	delete(r, k)
	return nil
}

func newRequested(keys []key.Key) requested {
	r := make(requested, len(keys))
	for _, k := range keys {
		r[k] = true
	}
	return r
}

func (p *PeerSource) pack(ctx context.Context, keys []key.Key, onBytes func(n int)) (*iroh.Stream, io.Reader, error) {
	s, err := p.request(ctx, request{kind: reqObjects, keys: keylist.Flatten(keys)})
	if err != nil {
		return nil, nil, err
	}
	if onBytes == nil {
		return s, s, nil
	}
	return s, &countingReader{r: s, onBytes: onBytes}, nil
}

// StreamRecords hands wire records to emit as they arrive.
func (p *PeerSource) StreamRecords(ctx context.Context, keys []key.Key, onBytes func(n int), emit func(amberpack.RawRecord) error) error {
	s, counted, err := p.pack(ctx, keys, onBytes)
	if err != nil {
		return err
	}
	defer s.CancelRead(0)
	want := newRequested(keys)
	for r, err := range amberpack.NewReader(counted).Records() {
		if err != nil {
			return err
		}
		if err := want.take(r.Key); err != nil {
			amberpack.PutBuf(r.Bytes)
			return err
		}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// probe asks the peer for a path's narinfo, answered from its index alone,
// and verifies it like any upstream narinfo.
func (p *PeerSource) probe(ctx context.Context, hashpart string, trusted trustedKeys) (Narinfo, error) {
	s, err := p.request(ctx, request{kind: reqNarinfo, hashpart: hashpart})
	if err != nil {
		return Narinfo{}, err
	}
	defer s.CancelRead(0)
	doc, err := io.ReadAll(io.LimitReader(s, 1<<20))
	if err != nil {
		return Narinfo{}, err
	}
	return parseVerifiedNarinfo(doc, hashpart, trusted)
}

func (p *PeerSource) indexRoot(ctx context.Context) (key.Key, error) {
	s, err := p.request(ctx, request{kind: reqIndex})
	if err != nil {
		return key.Key{}, err
	}
	defer s.CancelRead(0)
	var root key.Key
	if _, err := io.ReadFull(s, root[:]); err != nil {
		return key.Key{}, err
	}
	if root == (key.Key{}) {
		return key.Key{}, nil
	}
	return key.Parse(root[:])
}

type countingReader struct {
	r       io.Reader
	onBytes func(n int)
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	if n > 0 {
		c.onBytes(n)
	}
	return n, err
}

// peerSources returns a source per known peer.
func (n *Node) peerSources() []remotesync.Source {
	ids := n.peerIDs()
	sources := make([]remotesync.Source, len(ids))
	for i, id := range ids {
		sources[i] = &PeerSource{Swarm: n.cfg.Swarm, ID: id}
	}
	return sources
}

// fetchFromPeers ingests one path from the swarm: synced peer indexes
// answer first, live probes cover the gap, and missing chunks are pulled
// striped across every peer holding the same root.
func (n *Node) fetchFromPeers(ctx context.Context, hashpart string) (PathInfo, error) {
	if pi, sources, ok := n.peerIndexLookup(hashpart); ok {
		if got, err := n.pullPath(ctx, hashpart, pi, sources); err == nil {
			return got, nil
		}
	}
	pi, sources, err := n.probePeers(ctx, hashpart)
	if err != nil {
		return PathInfo{}, err
	}
	return n.pullPath(ctx, hashpart, pi, sources)
}

// swarmOpts sizes batches for striping: the 60MiB bulk-sync default would
// put a whole path in one or two batches and leave every other peer idle.
// Jobs stay within the peers' aggregate admission budget (4 per peer).
func swarmOpts(sources int) remotesync.Opts {
	return remotesync.Opts{BatchBytes: 4 << 20, Jobs: min(4*sources, 16)}
}

// pullPath pulls pi's tree striped across sources and gates it against the
// signed NarHash before indexing: the signature covers NarHash but not
// RootKey, so the round-trip export is what binds the tree to it.
func (n *Node) pullPath(ctx context.Context, hashpart string, pi PathInfo, sources []remotesync.Source) (PathInfo, error) {
	n.gcMu.RLock()
	defer n.gcMu.RUnlock()
	keys := make([]any, len(sources))
	for i, s := range sources {
		keys[i] = s.(*PeerSource).ID
	}
	ms := remotesync.NewMultiSourceKeyed(n.swarmStats, keys, sources)
	if _, err := remotesync.Pull(ctx, n.store, ms, pi.RootKey, swarmOpts(len(sources))); err != nil {
		return PathInfo{}, err
	}
	h := sha256.New()
	// Pull greys only what it wrote; objects already on disk (evicted, not
	// yet collected) must be observed too or a running mark sweeps them.
	get := func(k key.Key) ([]byte, error) {
		n.store.ObserveKeys([]key.Key{k})
		return n.store.Get(k)
	}
	if err := narexport.Export(h, pi.RootKey, get); err != nil {
		return PathInfo{}, err
	}
	if [32]byte(h.Sum(nil)) != pi.NarHash {
		return PathInfo{}, errors.New("nixcache: peer tree does not reproduce the signed NarHash")
	}
	pi.IngestedAt = time.Now().Unix()
	if err := n.publish([]PathInfo{pi}, nil); err != nil {
		return PathInfo{}, err
	}
	return pi, n.enforceBudget(hashpart, pi.NarSize)
}

// probePeers asks every peer concurrently and keeps those on the winning
// root: striping needs byte-identical trees.
func (n *Node) probePeers(ctx context.Context, hashpart string) (PathInfo, []remotesync.Source, error) {
	type hit struct {
		ni   Narinfo
		root key.Key
		id   ikey.EndpointID
	}
	ids := n.peerIDs()
	hits := make([]*hit, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			src := &PeerSource{Swarm: n.cfg.Swarm, ID: id}
			ni, err := src.probe(ctx, hashpart, n.trusted)
			if err != nil {
				return
			}
			root, err := NarURLKey(ni.URL)
			if err != nil {
				return
			}
			hits[i] = &hit{ni: ni, root: root, id: id}
		}()
	}
	wg.Wait()

	votes := map[key.Key]int{}
	for _, h := range hits {
		if h != nil {
			votes[h.root]++
		}
	}
	var root key.Key
	for r, v := range votes {
		if v > votes[root] {
			root = r
		}
	}
	if root == (key.Key{}) {
		return PathInfo{}, nil, fmt.Errorf("nixcache: no peer has %s: %w", hashpart, fstree.ErrNotFound)
	}
	var ni Narinfo
	var sources []remotesync.Source
	for _, h := range hits {
		if h != nil && h.root == root {
			ni = h.ni
			sources = append(sources, &PeerSource{Swarm: n.cfg.Swarm, ID: h.id})
		}
	}
	return ni.pathInfo(root), sources, nil
}

// ensure re-materializes an evicted tree from peers for the NAR route. The
// objects self-verify, and nix checks the NarHash it cached with the URL.
func (n *Node) ensure(ctx context.Context, root key.Key) error {
	n.gcMu.RLock()
	defer n.gcMu.RUnlock()
	sources := n.peerSources()
	if _, err := remotesync.Pull(ctx, n.store, remotesync.NewMultiSource(sources...), root, swarmOpts(len(sources))); err != nil {
		return fmt.Errorf("nixcache: tree %s unavailable from peers: %w", root, err)
	}
	return nil
}
