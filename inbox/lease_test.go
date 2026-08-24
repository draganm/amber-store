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

func (l *fakeLease) state() (refreshes int, released bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refreshes, l.released
}

type fakeLeaser struct {
	mu     sync.Mutex
	leases map[key.Key]*fakeLease
}

func (fl *fakeLeaser) lease(root key.Key) *fakeLease {
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
	fl := &fakeLeaser{}
	ib, err := Open(t.TempDir(), store, 1, nil, WithLeaser(LeaserOf(fl.lease)))
	if err != nil {
		t.Fatal(err)
	}
	if !ib.Leased() {
		t.Fatal("Leased() = false with a leaser configured")
	}

	obj1 := blobObject(t, []byte("lease-one"))
	obj2 := blobObject(t, []byte("lease-two"))
	root := obj1.Key
	stageAndCommit(t, ib, root, packBody(t, obj1))
	l := fl.get(root)
	if l == nil {
		t.Fatal("no lease taken on first pack")
	}
	stageAndCommit(t, ib, root, packBody(t, obj2))
	if refreshes, released := l.state(); refreshes != 1 || released {
		t.Fatalf("after second pack: refreshes=%d released=%v, want 1 refresh, not released", refreshes, released)
	}
	// A duplicate pack (a resumed push) refreshes too.
	stageAndCommit(t, ib, root, packBody(t, obj2))
	if refreshes, _ := l.state(); refreshes != 2 {
		t.Fatalf("after duplicate pack: refreshes=%d, want 2", refreshes)
	}

	// The reference PUT releases; a second release is a no-op.
	ib.ReleaseLease(root)
	ib.ReleaseLease(root)
	if _, released := l.state(); !released {
		t.Fatal("lease not released by ReleaseLease")
	}

	// A pack after the release starts a fresh lease.
	obj3 := blobObject(t, []byte("lease-three"))
	stageAndCommit(t, ib, root, packBody(t, obj3))
	fresh := fl.get(root)
	if fresh == l || fresh == nil {
		t.Fatal("no fresh lease after release")
	}
	if err := ib.Close(); err != nil {
		t.Fatal(err)
	}
	if _, released := fresh.state(); !released {
		t.Fatal("Close left the lease held")
	}
}

// TestRecoveredEntriesLeasedBeforeWorkersRun pins the restart contract:
// an entry committed by a previous run is leased inside Open, before the
// worker pool can process it — so the lease is observable the moment Open
// returns, however fast the worker drains the entry afterwards, and it
// survives the processing (only the reference PUT releases it).
func TestRecoveredEntriesLeasedBeforeWorkersRun(t *testing.T) {
	store := newTestStore(t)
	dir := t.TempDir()
	obj := blobObject(t, []byte("recovered"))
	root := obj.Key

	// Leave a committed entry on disk: stop the first inbox's workers before
	// committing so nothing processes it.
	first, err := Open(dir, store, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stageAndCommit(t, first, root, packBody(t, obj))

	fl := &fakeLeaser{}
	ib, err := Open(dir, store, 1, nil, WithLeaser(LeaserOf(fl.lease)))
	if err != nil {
		t.Fatal(err)
	}
	defer ib.Close()
	l := fl.get(root)
	if l == nil {
		t.Fatal("Open took no lease for the recovered root")
	}
	ib.WaitFor(root)
	if has, err := store.Has(root); err != nil || !has {
		t.Fatalf("recovered entry not processed: has=%v err=%v", has, err)
	}
	if _, released := l.state(); released {
		t.Fatal("processing the recovered entry released its lease; only the reference PUT may")
	}
}
