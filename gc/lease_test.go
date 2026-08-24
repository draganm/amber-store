package gc

import (
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/packstore"
)

func TestHorizonGraceOnly(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	now := time.Now()
	h := c.horizon(now)
	if want := now.Add(-time.Hour); !h.Equal(want) {
		t.Errorf("horizon = %v, want %v", h, want)
	}
}

func TestHorizonLease(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	root, _ := storeTree(t, ts.objects, "ls", 2)
	l := c.Lease(root)
	now := time.Now().Add(2 * time.Hour) // pretend hours passed
	// The lease is idle past grace by then: expired, ignored.
	if h := c.horizon(now); !h.Equal(now.Add(-time.Hour)) {
		t.Errorf("expired lease still pins the horizon: %v", h)
	}
	// A fresh lease pins the horizon at its start.
	l2 := c.Lease(root)
	if h := c.horizon(time.Now()); h.After(l2.start) {
		t.Errorf("horizon %v after lease start %v", h, l2.start)
	}
	l2.Release()
	l.Release()
	if h := c.horizon(time.Now().Add(time.Minute)); h.Before(time.Now().Add(-2 * time.Hour)) {
		t.Errorf("released leases still pin the horizon: %v", h)
	}
}

func TestHorizonRefreshKeepsLeaseAlive(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{Grace: 50 * time.Millisecond})
	root, _ := storeTree(t, ts.objects, "rf", 2)
	l := c.Lease(root)
	time.Sleep(30 * time.Millisecond)
	l.Refresh()
	// 30ms after the refresh the lease is still within grace, so the
	// horizon sits at its start.
	time.Sleep(30 * time.Millisecond)
	if h := c.horizon(time.Now()); h.After(l.start) {
		t.Error("refreshed lease not honored")
	}
	// Once idle past grace it expires.
	time.Sleep(60 * time.Millisecond)
	if h := c.horizon(time.Now()); h.Before(time.Now().Add(-time.Second)) {
		t.Error("expired lease still honored")
	}
	l.Release()
}

func TestHorizonInflightWrite(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{Grace: time.Millisecond})
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		seq := func(yield func(packstore.Object, error) bool) {
			o, err := fstree.EncodeBlob([]byte("held"))
			if err != nil || !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
			close(started)
			<-release
		}
		ts.objects.WriteBatch(seq)
	}()
	<-started
	h := c.horizon(time.Now().Add(time.Hour))
	if h.After(time.Now()) {
		t.Errorf("horizon %v ignores the in-flight write", h)
	}
	close(release)
}
