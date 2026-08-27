package nixcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/nixcache"
)

// TestConnectLoopReconnects: a dropped static-peer connection comes back
// without waiting for a fetch.
func TestConnectLoopReconnects(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	remote := testSwarm(t)
	n, _, sw := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.Peers = swarmPeers(remote)
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go nixcache.ConnectLoop(n, ctx)

	deadline := time.Now().Add(5 * time.Second)
	for !sw.Connected(remote.ID()) {
		if time.Now().After(deadline) {
			t.Fatal("never connected")
		}
		time.Sleep(50 * time.Millisecond)
	}
	sw.ClosePeer(remote.ID())
	deadline = time.Now().Add(5 * time.Second)
	for !sw.Connected(remote.ID()) {
		if time.Now().After(deadline) {
			t.Fatal("never reconnected")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestInboundConnReused: a peer that dialed us is reachable back over the
// same connection without knowing its address.
func TestInboundConnReused(t *testing.T) {
	a, b := testSwarm(t), testSwarm(t)
	a.AddAddr(b.Addr())
	if _, err := a.Conn(t.Context(), b.ID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !b.Connected(a.ID()) {
		if time.Now().After(deadline) {
			t.Fatal("inbound conn not registered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	src := &nixcache.PeerSource{Swarm: b, ID: a.ID()}
	(&nixcache.Server{Store: newRecStore(), Index: func() key.Key { return key.Key{} }}).Attach(a)
	if _, err := nixcache.Probe(src, hashPart(1), nil); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("request over inbound conn: %v", err)
	}
}

// TestAddPeer: a discovered peer lands in the peer set once, self never.
func TestAddPeer(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	remote := testSwarm(t)
	n, _, sw := newNode(t, u, nil)
	if nixcache.AddPeer(n, sw.Addr()) {
		t.Fatal("added self")
	}
	if !nixcache.AddPeer(n, remote.Addr()) || nixcache.AddPeer(n, remote.Addr()) {
		t.Fatal("add remote: want true then false")
	}
	if ids := nixcache.KnownPeers(n); len(ids) != 1 || ids[0] != remote.ID() {
		t.Fatalf("peer set = %v", ids)
	}
}
