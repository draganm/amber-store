package remotesync

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"golang.org/x/sync/errgroup"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

type fakeSource struct {
	mu      sync.Mutex
	calls   int
	fail    bool
	delay   time.Duration
	block   chan struct{} // when non-nil, FetchObjects waits on it
	started atomic.Int64
}

func (f *fakeSource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	if f.fail {
		return nil, errors.New("down")
	}
	return []key.Key{root}, nil
}

func (f *fakeSource) FetchObjects(ctx context.Context, keys []key.Key, onBytes func(int)) ([]fstree.Object, error) {
	f.started.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.fail {
		return nil, errors.New("down")
	}
	return make([]fstree.Object, len(keys)), nil
}

func TestMultiSourceFailover(t *testing.T) {
	dead, live := &fakeSource{fail: true}, &fakeSource{}
	m := NewMultiSource(dead, live)
	if _, err := m.ReachableKeys(t.Context(), key.Key{}); err != nil {
		t.Fatal(err)
	}
	objs, err := m.FetchObjects(t.Context(), make([]key.Key, 3), nil)
	if err != nil || len(objs) != 3 {
		t.Fatalf("failover: %v, %d objs", err, len(objs))
	}
	if dead.calls != 1 || live.calls != 1 {
		t.Fatalf("calls: dead=%d live=%d", dead.calls, live.calls)
	}
}

func TestMultiSourceAllDown(t *testing.T) {
	m := NewMultiSource(&fakeSource{fail: true}, &fakeSource{fail: true})
	if _, err := m.FetchObjects(t.Context(), nil, nil); err == nil {
		t.Fatal("want error when every source is down")
	}
}

func TestMultiSourceSpreadsLoad(t *testing.T) {
	// Source a blocks mid-fetch; a concurrent fetch must go to b.
	a := &fakeSource{block: make(chan struct{})}
	b := &fakeSource{}
	m := NewMultiSource(a, b)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.FetchObjects(context.Background(), nil, nil)
	}()
	for a.started.Load() == 0 {
		runtime.Gosched()
	}
	if _, err := m.FetchObjects(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if b.calls != 1 {
		t.Fatalf("concurrent fetch not routed to idle source: b.calls=%d", b.calls)
	}
	close(a.block)
	<-done
}

type countingSource struct {
	fakeSource
	keyCalls atomic.Int64
}

func (c *countingSource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	c.keyCalls.Add(1)
	return c.fakeSource.ReachableKeys(ctx, root)
}

func TestPullCompleteTreeNoRoundTrip(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	obj, err := fstree.EncodeBlob([]byte("already here"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(obj.Key, obj.Bytes); err != nil {
		t.Fatal(err)
	}
	src := &countingSource{}
	stats, err := Pull(t.Context(), store, src, obj.Key, Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 0 || src.keyCalls.Load() != 0 {
		t.Fatalf("complete tree still hit the network: %+v, keyCalls=%d", stats, src.keyCalls.Load())
	}
}

func TestMultiSourceAvoidsSlowSource(t *testing.T) {
	slow := &fakeSource{delay: 100 * time.Millisecond}
	fast := &fakeSource{delay: time.Millisecond}
	m := NewMultiSource(slow, fast)

	var eg errgroup.Group
	eg.SetLimit(4)
	for range 32 {
		eg.Go(func() error {
			_, err := m.FetchObjects(context.Background(), nil, nil)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	if slow.calls > 8 {
		t.Fatalf("slow source got %d of 32 batches", slow.calls)
	}
}

func TestAcquireHonorsContextCancel(t *testing.T) {
	s := NewStats()
	s.claim("a") // unmeasured and busy: nothing eligible
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, _, err := s.acquire(ctx, []any{"a"}); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire ignored cancellation")
	}
}

func TestAcquireWakesWhenBackoffExpires(t *testing.T) {
	s := NewStats()
	s.release("a", s.claim("a"), 0, time.Second, true) // one failure: 1s backoff
	start := time.Now()
	if i, _, err := s.acquire(t.Context(), []any{"a"}); err != nil || i != 0 {
		t.Fatalf("acquire: %d, %v", i, err)
	}
	if d := time.Since(start); d < 500*time.Millisecond || d > 3*time.Second {
		t.Fatalf("acquired after %v, want about the 1s backoff", d)
	}
}

func TestPullNoSourcesFails(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	obj, err := fstree.EncodeBlob([]byte("absent"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := Pull(ctx, store, NewMultiSource(), obj.Key, Opts{}); err == nil || ctx.Err() != nil {
		t.Fatalf("pull with no sources: err=%v, ctx=%v", err, ctx.Err())
	}
}
