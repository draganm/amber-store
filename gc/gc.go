// Package gc implements the mark-and-sweep collector of
// architecture/mark-sweep-gc.md, a port of Mic92's bitmap GC
// (draganm/amber-store#9): a cycle marks every key reachable from the
// references' roots into a packstore.MarkSet — one bit per sealed record,
// slotted by the packs' own footer indexes — and sweeps by rewriting the
// packs whose dead ratio crosses the line (packstore.Compact). Nothing is
// persisted between cycles: no closure files, no union, no refcounts. A
// write barrier (packstore.BeginBarrier) keeps ingests running while the
// mark walks; reference publication and the sweep serialize on the
// collector's reference lock. Wire protocol and pack formats are
// unchanged.
package gc

import (
	"runtime"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// DefaultGrace is the minimum age of a sealed pack before it can be
	// reaped.
	DefaultGrace = time.Hour
	// DefaultGarbage is the selection line: an eligible pack with at
	// least this fraction of garbage is reaped.
	DefaultGarbage = 0.5
	// minFreeGarbage is the selection line under free-space pressure.
	minFreeGarbage = 0.1
)

// Options configure a Collector. The zero value means: 1 h grace, 0.5
// garbage line, min-free at 5 % of the filesystem, unlimited copy rate, no
// background cycles, GOMAXPROCS walk parallelism.
type Options struct {
	Grace    time.Duration // pack eligibility age
	Garbage  float64       // reap packs with at least this fraction of garbage
	MinFree  uint64        // free-space floor in bytes; 0 = 5 % of the filesystem
	Rate     int64         // copier bandwidth cap in bytes/s; 0 = unlimited
	Interval time.Duration // time between background cycles; 0 = none
	Jobs     int           // PrepareRef walk parallelism; <= 0 = GOMAXPROCS
}

func (o Options) withDefaults() Options {
	if o.Grace <= 0 {
		o.Grace = DefaultGrace
	}
	if o.Garbage <= 0 {
		o.Garbage = DefaultGarbage
	}
	if o.Jobs <= 0 {
		o.Jobs = runtime.GOMAXPROCS(0)
	}
	return o
}

// throttle paces the copier to rate bytes/s; zero rate never sleeps. Compact
// calls pace from its single append loop, so it bounds the aggregate.
type throttle struct {
	rate  int64
	start time.Time
	mu    sync.Mutex
	bytes int64
}

func newThrottle(rate int64) *throttle {
	return &throttle{rate: rate, start: time.Now()}
}

func (t *throttle) pace(n int) {
	if t.rate <= 0 {
		return
	}
	t.mu.Lock()
	t.bytes += int64(n)
	ahead := throttleOwed(t.bytes, t.rate) - time.Since(t.start)
	t.mu.Unlock()
	if ahead > 0 {
		time.Sleep(ahead)
	}
}

// throttleOwed is the total time a copy of n bytes at rate bytes/s should
// have taken. Divide-before-multiply: n*1e9 overflows int64 past ~8.6 GiB.
func throttleOwed(n, rate int64) time.Duration {
	return time.Duration(n/rate)*time.Second + time.Duration((n%rate)*int64(time.Second)/rate)
}

// freeBelow reports whether the filesystem holding path has less than min
// bytes free; min 0 means 5 % of the filesystem.
func freeBelow(path string, min uint64) bool {
	var st unix.Statfs_t
	if unix.Statfs(path, &st) != nil {
		return false
	}
	if min == 0 {
		min = uint64(st.Blocks) * uint64(st.Bsize) / 20
	}
	return uint64(st.Bavail)*uint64(st.Bsize) < min
}
