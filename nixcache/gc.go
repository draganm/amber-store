package nixcache

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// Liveness marks and returns the per-segment liveness breakdown: the GC dry
// run. It quiesces writers like GC so the mark is exact.
func (n *Node) Liveness() ([]packstore.SegmentLiveness, error) {
	n.gcMu.Lock()
	defer n.gcMu.Unlock()
	live, err := n.markLive(n.indexRoot(), n.snapshotPeerRoots())
	if err != nil {
		return nil, err
	}
	return n.store.Liveness(live.Contains)
}

// ErrGCRunning is returned by GC while another cycle is in progress.
var ErrGCRunning = errors.New("nixcache: gc cycle already running")

// GC marks from the published index and compacts segments past
// minDeadRatio. Writers stall only for the snapshot and the sweep
// (specs/gc.qnt). The mark runs concurrently with ingests.
// Cycles never overlap: the store has one write barrier, so a second
// BeginBarrier mid-mark would lose the keys the first cycle captured.
func (n *Node) GC(minDeadRatio float64) (packstore.CompactStats, error) {
	if !n.cycleMu.TryLock() {
		return packstore.CompactStats{}, ErrGCRunning
	}
	defer n.cycleMu.Unlock()
	n.gcMu.Lock()
	n.store.BeginBarrier()
	root := n.indexRoot()
	peerRoots := n.snapshotPeerRoots()
	n.gcMu.Unlock()

	live, err := n.markLive(root, peerRoots)
	if n.midMark != nil {
		n.midMark()
	}
	if err != nil {
		n.store.AbortBarrier()
		return packstore.CompactStats{}, err
	}

	n.gcMu.Lock()
	defer n.gcMu.Unlock()
	start := time.Now()
	stats, err := n.store.Compact(live.Contains, packstore.CompactOpts{MinDeadRatio: minDeadRatio})
	if err == nil {
		n.metrics.gcRuns.Add(1)
		n.metrics.gcFreedBytes.Add(stats.BytesFreed)
	}
	if err == nil && stats.SegmentsCompacted > 0 {
		slog.Info("gc", "segments", stats.SegmentsCompacted, "freed", stats.BytesFreed,
			"copied", stats.BytesCopied, "dur", time.Since(start).Round(time.Millisecond))
	}
	return stats, err
}

// markLive walks the index at root, every indexed tree, and each synced
// peer index (structure only). The roots must be snapshots taken under
// gcMu. The walk touches only snapshot-reachable objects.
func (n *Node) markLive(root key.Key, peerRoots []key.Key) (*packstore.MarkSet, error) {
	live := n.store.NewMarkSet()
	for _, pr := range peerRoots {
		if err := n.markFrom(live, pr); err != nil {
			return nil, err
		}
	}
	if root == (key.Key{}) {
		return live, nil
	}
	if err := n.markFrom(live, root); err != nil {
		return nil, err
	}
	for e, err := range indexEntries(root, n.store.Get) {
		if err != nil {
			return nil, err
		}
		pi, err := DecodeRecord(e.LinkTarget)
		if err != nil {
			return nil, err
		}
		if err := n.markFrom(live, pi.RootKey); err != nil {
			return nil, err
		}
	}
	return live, nil
}

// markFrom prunes at already-marked keys, so shared subtrees are walked once.
func (n *Node) markFrom(live *packstore.MarkSet, root key.Key) error {
	stack := []key.Key{root}
	for len(stack) > 0 {
		k := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		newly, present := live.Mark(k)
		if !present {
			return fmt.Errorf("nixcache: mark: object %s missing from store", k)
		}
		if !newly {
			continue
		}
		if k.Type() == key.Blob || k.Type() == key.XattrSet {
			continue
		}
		data, err := n.store.Get(k)
		if err != nil {
			return err
		}
		children, err := fstree.ChildKeys(k, data)
		if err != nil {
			return err
		}
		stack = append(stack, children...)
	}
	return nil
}
