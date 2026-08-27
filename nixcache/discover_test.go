package nixcache_test

import (
	"slices"
	"testing"
	"time"

	"github.com/draganm/amber-store/nixcache"
)

// A knows B, C knows B; A must learn C through gossip and reach it.
func TestGossipDiscovery(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	nB, _, swB := newNode(t, u, nil)
	nA, _, swA := newNode(t, u, func(c *nixcache.NodeConfig) { c.Peers = swarmPeers(swB) })
	nC, _, swC := newNode(t, u, func(c *nixcache.NodeConfig) { c.Peers = swarmPeers(swB) })
	for _, n := range []*nixcache.Node{nA, nB, nC} {
		go nixcache.Discover(n, t.Context())
	}
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Contains(nixcache.KnownPeers(nA), swC.ID()) {
		if time.Now().After(deadline) {
			t.Fatalf("A never learned C; knows %v", nixcache.KnownPeers(nA))
		}
		time.Sleep(50 * time.Millisecond)
	}
	src := &nixcache.PeerSource{Swarm: swA, ID: swC.ID()}
	if _, err := nixcache.IndexRoot(src, t.Context()); err != nil {
		t.Fatal("reach C:", err)
	}
	if !slices.Contains(nixcache.KnownPeers(nB), swA.ID()) {
		t.Fatalf("B did not learn A: %v", nixcache.KnownPeers(nB))
	}
}

// Discovered peers that stop answering are dropped so the capped peer
// set does not fill with dead IDs. Static peers stay.
func TestDeadDiscoveredPeerDropped(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	static := testSwarm(t)
	n, _, _ := newNode(t, u, func(c *nixcache.NodeConfig) { c.Peers = swarmPeers(static) })
	gone := testSwarm(t)
	if !nixcache.AddPeer(n, gone.Addr()) {
		t.Fatal("addPeer")
	}
	static.Close()
	gone.Close()
	for range 2 {
		if !slices.Contains(nixcache.KnownPeers(n), gone.ID()) {
			t.Fatal("dropped too early")
		}
		n.SyncPeers(t.Context())
	}
	if slices.Contains(nixcache.KnownPeers(n), gone.ID()) {
		t.Fatal("dead discovered peer kept")
	}
	if !slices.Contains(nixcache.KnownPeers(n), static.ID()) {
		t.Fatal("static peer dropped")
	}
}
