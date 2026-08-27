package nixcache

import (
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
)

// agePass must publish evictions under gcMu like every other writer.
func TestAgePassEvictsUnderGCMu(t *testing.T) {
	n, err := OpenNode(NodeConfig{Dir: t.TempDir(), Upstream: "http://unreachable.invalid", BudgetBytes: 100, CatalogTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	hp := strings.Repeat("a", 32)
	root, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.store.Put(root.Key, root.Bytes); err != nil {
		t.Fatal(err)
	}
	pi := PathInfo{StorePath: storeDir + hp + "-x", RootKey: root.Key, NarSize: 1000, AgedAt: time.Now().Unix()}
	if err := n.publish([]PathInfo{pi}, nil); err != nil {
		t.Fatal(err)
	}
	n.evict.Admit(hp, pi.NarSize)
	before := n.indexRoot()

	n.gcMu.Lock()
	done := make(chan error, 1)
	go func() { done <- n.agePass(time.Now()) }()
	select {
	case err := <-done:
		t.Fatalf("agePass finished while gc held gcMu (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	if n.indexRoot() != before {
		t.Fatal("index published during gc sweep")
	}
	n.gcMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n.indexRoot() == before {
		t.Fatal("eviction not published")
	}
}
