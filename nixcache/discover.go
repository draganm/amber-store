package nixcache

import (
	"slices"

	ikey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// maxSwarmPeers caps the peer set. Beyond it discovered peers are
// dropped, striping gains nothing from more sources than jobs.
const maxSwarmPeers = 16

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

// addPeer records a peer's address and admits it to the peer set.
func (n *Node) addPeer(a netaddr.EndpointAddr) bool {
	if a.ID == n.cfg.Swarm.ID() {
		return false
	}
	if len(a.Addrs()) > 0 {
		n.cfg.Swarm.AddAddr(a)
	}
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	if _, ok := n.peerSet[a.ID]; ok {
		return false
	}
	if len(n.peerSet) < maxSwarmPeers {
		n.peerSet[a.ID] = struct{}{}
		return true
	}
	return false
}
