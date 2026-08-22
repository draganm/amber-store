package nixcache

import (
	"context"
	"log/slog"
	"maps"
	"slices"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remotesync"
	ikey "github.com/tmc/go-iroh/key"
)

// maxPeerIndexBytes caps one peer index sync: peers are unauthenticated,
// and a synced root stays live across GC. About a million records.
const maxPeerIndexBytes = 512 << 20

// SyncPeers pulls each peer's index tree (kilobytes of metadata, never
// NAR payload) into the local store. Peer lookups afterwards are local
// tree walks.
func (n *Node) SyncPeers(ctx context.Context) {
	for _, id := range n.peerIDs() {
		src := &PeerSource{Swarm: n.cfg.Swarm, ID: id}
		root, err := src.indexRoot(ctx)
		if err != nil || root == (key.Key{}) {
			continue
		}
		n.peerMu.Lock()
		known := n.peerRoots[id] == root
		n.peerMu.Unlock()
		if known {
			continue
		}
		n.gcMu.RLock()
		_, err = remotesync.Pull(ctx, n.store, src, root, remotesync.Opts{MaxBytes: maxPeerIndexBytes})
		n.gcMu.RUnlock()
		if err != nil {
			slog.Warn("peer index sync", "peer", id, "err", err)
			continue
		}
		n.peerMu.Lock()
		n.peerRoots[id] = root
		n.peerMu.Unlock()
	}
}

// snapshotPeerRoots returns the synced roots. GC marks them (structure
// only), so peer lookups survive a sweep without re-syncing.
func (n *Node) snapshotPeerRoots() []key.Key {
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	return slices.Collect(maps.Values(n.peerRoots))
}

// peerIndexLookup finds hashpart in the synced peer indexes, verifies the
// record's upstream signature, and returns it with a source per peer on
// the winning root.
func (n *Node) peerIndexLookup(hashpart string) (PathInfo, []remotesync.Source, bool) {
	n.peerMu.Lock()
	roots := maps.Clone(n.peerRoots)
	n.peerMu.Unlock()

	byRoot := map[key.Key][]ikey.EndpointID{}
	records := map[key.Key]PathInfo{}
	for id, root := range roots {
		pi, err := Lookup(root, hashpart, n.store.Get)
		if err != nil {
			continue
		}
		pi, ok := n.peerRecord(pi, hashpart)
		if !ok {
			continue
		}
		byRoot[pi.RootKey] = append(byRoot[pi.RootKey], id)
		records[pi.RootKey] = pi
	}
	var best key.Key
	for r, ids := range byRoot {
		if len(ids) > len(byRoot[best]) {
			best = r
		}
	}
	if len(byRoot[best]) == 0 {
		return PathInfo{}, nil, false
	}
	var sources []remotesync.Source
	for _, id := range byRoot[best] {
		sources = append(sources, &PeerSource{Swarm: n.cfg.Swarm, ID: id})
	}
	return records[best], sources, true
}

// peerRecord accepts a record from an untrusted peer index: it must be
// for hashpart and carry a trusted signature. The signature binds path,
// hash, size and references; everything else is dropped or validated.
func (n *Node) peerRecord(pi PathInfo, hashpart string) (PathInfo, bool) {
	if HashPart(pi.StorePath) != hashpart {
		return PathInfo{}, false
	}
	if pi.Deriver != "" && !validBasename(pi.Deriver) {
		return PathInfo{}, false
	}
	for _, r := range pi.References {
		if !validBasename(r) {
			return PathInfo{}, false
		}
	}
	ni := Narinfo{
		StorePath:  pi.StorePath,
		NarHash:    pi.NarHash,
		NarSize:    pi.NarSize,
		References: pi.References,
	}
	var sigs []string
	for _, sig := range pi.Sigs {
		if ni.VerifySig(sig, n.trusted) {
			sigs = append(sigs, sig)
		}
	}
	if len(sigs) == 0 {
		return PathInfo{}, false
	}
	pi.Sigs = sigs
	return pi, true
}

// validBasename accepts "<hashpart>-<name>" with no whitespace or
// control characters, the only form the narinfo lines may carry.
func validBasename(s string) bool {
	if HashPart(storeDir+s) == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// peersHave widens the catalog gate: a path a peer vouches for with a
// verified upstream signature is servable even if our catalog lags.
func (n *Node) peersHave(hashpart string) bool {
	_, _, ok := n.peerIndexLookup(hashpart)
	return ok
}
