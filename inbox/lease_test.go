package inbox

import (
	"bytes"
	"sync"
	"testing"

	"github.com/draganm/amber-store/key"
)

// fakeLease records its lifecycle for assertions.
type fakeLease struct {
	mu        sync.Mutex
	refreshes int
	released  bool
}

func (l *fakeLease) Refresh() { l.mu.Lock(); l.refreshes++; l.mu.Unlock() }
func (l *fakeLease) Release() { l.mu.Lock(); l.released = true; l.mu.Unlock() }

type fakeLeaser struct {
	mu     sync.Mutex
	leases map[key.Key]*fakeLease
}

func (fl *fakeLeaser) lease(root key.Key) Lease {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.leases == nil {
		fl.leases = map[key.Key]*fakeLease{}
	}
	l := &fakeLease{}
	fl.leases[root] = l
	return l
}

func (fl *fakeLeaser) get(root key.Key) *fakeLease {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.leases[root]
}

// stageAndCommit pushes one pack for root through the inbox.
func stageAndCommit(t *testing.T, ib *Inbox, root key.Key, body []byte) {
	t.Helper()
	tmp, h, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ib.Commit(tmp, h, root); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseTakenRefreshedReleased(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(t.TempDir(), store, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	fl := &fakeLeaser{}
	ib.SetLeaser(fl.lease)

	obj1 := blobObject(t, []byte("lease-one"))
	obj2 := blobObject(t, []byte("lease-two"))
	root := obj1.Key
	stageAndCommit(t, ib, root, packBody(t, obj1))
	l := fl.get(root)
	if l == nil {
		t.Fatal("no lease taken on first pack")
	}
	stageAndCommit(t, ib, root, packBody(t, obj2))
	l.mu.Lock()
	refreshes, released := l.refreshes, l.released
	l.mu.Unlock()
	if refreshes != 1 || released {
		t.Fatalf("after second pack: refreshes=%d released=%v, want 1 refresh, not released", refreshes, released)
	}

	// The reference PUT releases; a second release is a no-op.
	ib.ReleaseLease(root)
	ib.ReleaseLease(root)
	l.mu.Lock()
	released = l.released
	l.mu.Unlock()
	if !released {
		t.Fatal("lease not released by ReleaseLease")
	}

	// A pack after the release starts a fresh lease.
	obj3 := blobObject(t, []byte("lease-three"))
	stageAndCommit(t, ib, root, packBody(t, obj3))
	if fresh := fl.get(root); fresh == l || fresh == nil {
		t.Fatal("no fresh lease after release")
	}
	if err := ib.Close(); err != nil {
		t.Fatal(err)
	}
	if fresh := fl.get(root); !fresh.released {
		t.Fatal("Close left the lease held")
	}
}

// TestSetLeaserCoversPendingEntries pins the recovery contract: SetLeaser
// takes a lease for every root already pending at install time (entries a
// previous run committed but had not processed). The pending group is
// planted directly — recovered entries drain too fast to catch reliably.
func TestSetLeaserCoversPendingEntries(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(t.TempDir(), store, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ib.Close()
	root := blobObject(t, []byte("recovered")).Key
	ib.mu.Lock()
	ib.groups[root]++
	ib.mu.Unlock()
	fl := &fakeLeaser{}
	ib.SetLeaser(fl.lease)
	if fl.get(root) == nil {
		t.Fatal("SetLeaser took no lease for the pending root")
	}
	ib.mu.Lock()
	ib.groups[root]--
	ib.mu.Unlock()
}
