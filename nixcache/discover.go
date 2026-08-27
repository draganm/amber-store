package nixcache

import (
	"context"
	"log/slog"
	"slices"

	ikey "github.com/tmc/go-iroh/key"
)

// maxSwarmPeers caps the peer set. Beyond it discovered peers are
// dropped, striping gains nothing from more sources than jobs.
const maxSwarmPeers = 16

// maxPeerFails consecutive sync failures drop a discovered peer.
const maxPeerFails = 2

type peerState struct {
	static bool
	fails  int
}

func (n *Node) peerIDs() []ikey.EndpointID {
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	ids := make([]ikey.EndpointID, 0, len(n.peerSet))
	for id := range n.peerSet {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, ikey.EndpointID.Compare)
	return ids
}

// addPeer admits id to the peer set.
func (n *Node) addPeer(id ikey.EndpointID, static bool) bool {
	if id == n.cfg.Swarm.ID() {
		return false
	}
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	if _, ok := n.peerSet[id]; ok || len(n.peerSet) >= maxSwarmPeers {
		return false
	}
	n.peerSet[id] = &peerState{static: static}
	return true
}

// peerResult records whether a peer answered; discovered peers that keep
// failing are dropped, discovery re-adds them if they return.
func (n *Node) peerResult(id ikey.EndpointID, err error) {
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	p := n.peerSet[id]
	if p == nil {
		return
	}
	if err == nil {
		p.fails = 0
		return
	}
	p.fails++
	if !p.static && p.fails >= maxPeerFails {
		delete(n.peerSet, id)
		delete(n.peerRoots, id)
		n.cfg.Swarm.ClosePeer(id)
		slog.Info("peer dropped", "peer", id.Short(), "err", err)
	}
}

// discoverLoop admits peers found by gossip and mDNS, each once, so a
// peer dropped by peerResult is not re-added on every update.
func (n *Node) discoverLoop(ctx context.Context) {
	sw := n.cfg.Swarm
	sw.Discover(n.cfg.Peers)
	updated := sw.Updated()
	seen := map[ikey.EndpointID]bool{}
	for {
		for _, id := range sw.Peers() {
			if seen[id] {
				continue
			}
			seen[id] = true
			if n.addPeer(id, false) {
				slog.Info("peer discovered", "peer", id.Short())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-updated:
		}
	}
}
