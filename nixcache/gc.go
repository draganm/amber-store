package nixcache

import (
	"fmt"

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

// GC marks from the published index and compacts segments past
// minDeadRatio. Writers stall only for the snapshot and the sweep
// (specs/gc.qnt). The mark runs concurrently with ingests.
func (n *Node) GC(minDeadRatio float64) (packstore.CompactStats, error) {
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
	return n.store.Compact(live.Contains, packstore.CompactOpts{MinDeadRatio: minDeadRatio})
}

// markLive walks the index tree at root, every indexed path's object
// tree, and each synced peer index (structure only: peer records point at
// trees we may not hold). The roots must be snapshots taken under gcMu.
// The walk may run concurrently with ingests: it only touches objects
// reachable from the snapshots, which no writer mutates.
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
