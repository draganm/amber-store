package nixcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remotesync"
	"github.com/tmc/go-iroh/relay"
	"golang.org/x/sync/errgroup"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

type mapStore map[key.Key][]byte

func (m mapStore) emit(obj fstree.Object) error {
	m[obj.Key] = obj.Bytes
	return nil
}

func (m mapStore) anyKey() key.Key {
	for k := range m {
		return k
	}
	return key.Key{}
}

func (m mapStore) get(k key.Key) ([]byte, error) {
	if b, ok := m[k]; ok {
		return b, nil
	}
	return nil, fstree.ErrNotFound
}

func benchIndex(b *testing.B, n int) (key.Key, mapStore) {
	b.Helper()
	st := mapStore{}
	blob, err := fstree.EncodeBlob([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	pis := make([]PathInfo, n)
	for i := range pis {
		pis[i] = PathInfo{
			StorePath: fmt.Sprintf("/nix/store/%032d-p", i),
			RootKey:   blob.Key,
			NarHash:   [32]byte{1},
			NarSize:   1 << 20,
		}
	}
	root, err := Merge(key.Key{}, pis, nil, st.get, st.emit)
	if err != nil {
		b.Fatal(err)
	}
	return root, st
}

func BenchmarkIndexPublish(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			root, st := benchIndex(b, n)
			pi := PathInfo{
				StorePath: "/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-q",
				RootKey:   st.anyKey(), NarHash: [32]byte{1}, NarSize: 1 << 20,
			}
			b.ResetTimer()
			for b.Loop() {
				if _, err := Merge(root, []PathInfo{pi}, nil, st.get, st.emit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkIndexUnpublish(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			root, st := benchIndex(b, n)
			victim := fmt.Sprintf("%032d", n/2)
			b.ResetTimer()
			for b.Loop() {
				if _, err := Merge(root, nil, []string{victim}, st.get, st.emit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEvictChurn(b *testing.B) {
	p := newEvictPolicy(100_000 << 20) // 100k one-MiB paths resident
	i := 0
	for ; int64(i)<<20 < p.budget; i++ {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
	}
	b.ResetTimer()
	for b.Loop() {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
		i++
		p.Evict()
	}
}

func BenchmarkEvictTouch(b *testing.B) {
	p := newEvictPolicy(1_000 << 20)
	for i := range 1000 {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
	}
	b.ResetTimer()
	for b.Loop() {
		p.Touch("00000000000000000000000000000500")
	}
}

// treeCollector records emitted objects, in order, for layout-faithful
// store writes.
type treeCollector struct {
	mapStore
	order *[]fstree.Object
}

func newTreeCollector() treeCollector {
	return treeCollector{mapStore: mapStore{}, order: new([]fstree.Object)}
}

func (s treeCollector) emit(o fstree.Object) error {
	*s.order = append(*s.order, o)
	return s.mapStore.emit(o)
}

func swarmTree(b *testing.B, st treeCollector, size int) key.Key {
	b.Helper()
	content := make([]byte, size)
	rand.Read(content)
	ib := fstree.NewFileIndexBuilder(chunkers.NewItemChunker(7))
	err := chunkers.SplitBytes(bytes.NewReader(content), nil, func(c []byte) error {
		o, err := fstree.EncodeBlob(c)
		if err != nil {
			return err
		}
		if err := st.emit(o); err != nil {
			return err
		}
		return ib.AddChild(st.emit, o.Key, nil)
	})
	if err != nil {
		b.Fatal(err)
	}
	root, err := ib.Finish(st.emit)
	if err != nil {
		b.Fatal(err)
	}
	return root
}

// swarmHosts builds one client swarm and n server swarms on loopback.
func swarmHosts(b *testing.B, n int) (*Swarm, []*Swarm) {
	newHost := func() *Swarm {
		sw, err := NewSwarm(context.Background(), SwarmOpts{
			KeyPath: b.TempDir() + "/p2p.key",
			Bind:    netip.MustParseAddrPort("127.0.0.1:0"),
			Relay:   relay.ModeDisabled(),
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { sw.Close() })
		return sw
	}
	client := newHost()
	servers := make([]*Swarm, n)
	for i := range servers {
		servers[i] = newHost()
		client.AddAddr(servers[i].Addr())
	}
	return client, servers
}

// delaySource simulates WAN latency: every request loses one RTT.
type delaySource struct {
	src *PeerSource
	rtt time.Duration
}

func (d delaySource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	time.Sleep(d.rtt)
	return d.src.ReachableKeys(ctx, root)
}

func (d delaySource) FetchObjects(ctx context.Context, keys []key.Key, onBytes func(int)) ([]fstree.Object, error) {
	time.Sleep(d.rtt)
	return d.src.FetchObjects(ctx, keys, onBytes)
}

func (d delaySource) StreamRecords(ctx context.Context, keys []key.Key, onBytes func(int), emit func(amberpack.RawRecord) error) error {
	time.Sleep(d.rtt)
	return d.src.StreamRecords(ctx, keys, onBytes, emit)
}

type swarmBench struct {
	peers   int
	rate    int64         // per-peer uplink cap, 0 = uncapped
	rates   []int64       // per-peer caps; overrides rate
	rtt     time.Duration // simulated per-request latency
	size    int
	prewarm bool // local store already holds the tree
}

func benchSwarmPull(b *testing.B, cfg swarmBench) {
	mem := newTreeCollector()
	root := swarmTree(b, mem, cfg.size)
	// Peers serve from a real packstore so span-coalesced serving is
	// exercised, not a test double.
	st, err := packstore.Open(b.TempDir(), packstore.WithSync(false))
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	// One writer keeps disk layout in tree order, like a store filled by an
	// in-order pull; parallel writers would scatter records and understate
	// span coalescing.
	seq := func(yield func(packstore.Object, error) bool) {
		for _, o := range *mem.order {
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	if _, err := st.WriteParallel(seq, packstore.WriteOpts{Writers: 1}); err != nil {
		b.Fatal(err)
	}
	client, servers := swarmHosts(b, cfg.peers)
	var sources []remotesync.Source
	for i, sh := range servers {
		rate := cfg.rate
		if cfg.rates != nil {
			rate = cfg.rates[i]
		}
		srv := &Server{
			Store:        st,
			Index:        func() key.Key { return key.Key{} },
			PeerByteRate: rate,
		}
		srv.Attach(sh)
		var src remotesync.Source = &PeerSource{Swarm: client, ID: sh.ID()}
		if cfg.rtt > 0 {
			src = delaySource{src: src.(*PeerSource), rtt: cfg.rtt}
		}
		sources = append(sources, src)
	}
	local, err2 := packstore.Open(b.TempDir(), packstore.WithSync(false))
	if err2 != nil {
		b.Fatal(err2)
	}
	defer local.Close()
	// Stats persist across iterations, as a node persists them across pulls.
	ms := remotesync.NewMultiSource(sources...)
	pull := func() {
		if _, err := remotesync.Pull(context.Background(), local, ms, root, swarmOpts(cfg.peers)); err != nil {
			b.Fatal(err)
		}
	}
	if cfg.prewarm {
		pull()
	}
	b.SetBytes(int64(cfg.size))
	b.ResetTimer()
	for b.Loop() {
		pull()
		if !cfg.prewarm {
			b.StopTimer()
			if err := local.Wipe(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}

// BenchmarkSwarmPull measures one node pulling a tree from N peers whose
// uplinks are capped at 8 MiB/s each: the BitTorrent claim is that
// throughput scales with the number of holders.
func BenchmarkSwarmPull(b *testing.B) {
	for _, peers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("peers=%d", peers), func(b *testing.B) {
			benchSwarmPull(b, swarmBench{peers: peers, rate: 8 << 20, size: 64 << 20})
		})
	}
}

// BenchmarkSwarmPullAsym mixes uplink speeds; scheduling should keep the
// pull near the fast peers' aggregate instead of the slowest peer's pace.
func BenchmarkSwarmPullAsym(b *testing.B) {
	benchSwarmPull(b, swarmBench{peers: 4, rates: []int64{32 << 20, 8 << 20, 2 << 20, 512 << 10}, size: 64 << 20})
}

// BenchmarkSwarmPullUncapped exposes the pull path's own CPU cost.
func BenchmarkSwarmPullUncapped(b *testing.B) {
	benchSwarmPull(b, swarmBench{peers: 4, size: 64 << 20})
}

// BenchmarkSwarmPullWAN checks the round-trip budget at 100ms latency:
// a large cold pull hides RTTs behind transfer, a small cold path costs
// the 2-RTT floor, and a warm re-ensure must not touch the network.
func BenchmarkSwarmPullWAN(b *testing.B) {
	const rtt = 100 * time.Millisecond
	b.Run("cold-64MiB", func(b *testing.B) {
		benchSwarmPull(b, swarmBench{peers: 4, rate: 8 << 20, rtt: rtt, size: 64 << 20})
	})
	b.Run("cold-4KiB", func(b *testing.B) {
		benchSwarmPull(b, swarmBench{peers: 1, rate: 8 << 20, rtt: rtt, size: 4 << 10})
	})
	b.Run("warm-64MiB", func(b *testing.B) {
		benchSwarmPull(b, swarmBench{peers: 4, rate: 8 << 20, rtt: rtt, size: 64 << 20, prewarm: true})
	})
}

// BenchmarkSwarmServe measures one peer feeding several concurrent pullers:
// the seeder role, where per-byte serving cost is capacity.
func BenchmarkSwarmServe(b *testing.B) {
	const pullers = 4
	mem := newTreeCollector()
	root := swarmTree(b, mem, 64<<20)
	st, err := packstore.Open(b.TempDir(), packstore.WithSync(false))
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	seq := func(yield func(packstore.Object, error) bool) {
		for _, o := range *mem.order {
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	if _, err := st.WriteParallel(seq, packstore.WriteOpts{Writers: 1}); err != nil {
		b.Fatal(err)
	}
	_, seeder := swarmHosts(b, 1)
	(&Server{
		Store:           st,
		Index:           func() key.Key { return key.Key{} },
		PeerConcurrency: pullers * 4,
	}).Attach(seeder[0])
	locals := make([]*packstore.Store, pullers)
	pullSources := make([]*PeerSource, pullers)
	for i := range locals {
		if locals[i], err = packstore.Open(b.TempDir(), packstore.WithSync(false)); err != nil {
			b.Fatal(err)
		}
		defer locals[i].Close()
		client, _ := swarmHosts(b, 0)
		client.AddAddr(seeder[0].Addr())
		pullSources[i] = &PeerSource{Swarm: client, ID: seeder[0].ID()}
	}
	b.SetBytes(64 << 20 * pullers)
	for b.Loop() {
		var eg errgroup.Group
		for i := range locals {
			eg.Go(func() error {
				_, err := remotesync.Pull(context.Background(), locals[i], pullSources[i], root, swarmOpts(1))
				return err
			})
		}
		if err := eg.Wait(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for _, l := range locals {
			if err := l.Wipe(); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
}
