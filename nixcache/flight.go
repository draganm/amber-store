package nixcache

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
)

// flights coalesces concurrent work per key. The work runs under a context
// that lives as long as any caller is still waiting, so one client
// disconnecting neither fails nor leaks the fetch the others share.
type flights struct {
	sf singleflight.Group
	mu sync.Mutex
	m  map[string]*flight
}

type flight struct {
	ctx     context.Context
	cancel  context.CancelFunc
	waiters int
}

func (f *flights) join(k string) *flight {
	f.mu.Lock()
	defer f.mu.Unlock()
	fl := f.m[k]
	if fl == nil {
		if f.m == nil {
			f.m = map[string]*flight{}
		}
		fl = &flight{}
		fl.ctx, fl.cancel = context.WithCancel(context.Background())
		f.m[k] = fl
	}
	fl.waiters++
	return fl
}

func (f *flights) leave(k string, fl *flight) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fl.waiters--
	if fl.waiters == 0 {
		fl.cancel()
		if f.m[k] == fl {
			delete(f.m, k)
			f.sf.Forget(k)
		}
	}
}

func inFlight[T any](ctx context.Context, f *flights, k string, fn func(context.Context) (T, error)) (T, error) {
	fl := f.join(k)
	defer f.leave(k, fl)
	ch := f.sf.DoChan(k, func() (any, error) { return fn(fl.ctx) })
	select {
	case r := <-ch:
		v, _ := r.Val.(T)
		return v, r.Err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
