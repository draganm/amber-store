// One cycle: snapshot the union pointer and the horizon; score every pack's
// footer index against the union, in parallel; reap every eligible pack
// above the garbage line, most garbage first; per victim, copy the live
// records and delete the pack under the removal lock. Cycles never overlap.

package gc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// ErrCycleRunning reports an overlapping Run; cycles never overlap.
var ErrCycleRunning = errors.New("gc: a cycle is already running")

// PackStatus is one pack's score against a union and horizon.
type PackStatus struct {
	ID       uint64
	Sealed   time.Time
	Body     int64 // record bytes
	Keys     uint64
	Live     int64 // Σ (46 + slen) over entries whose tail is in the union
	Garbage  float64
	Eligible bool // sealed before the horizon
}

// CycleStats describes one cycle.
type CycleStats struct {
	Start         time.Time
	Duration      time.Duration
	Threshold     float64
	Scored        int
	Reaped        []uint64 // victim ids, most garbage first
	CopiedRecords int
	CopiedBytes   int64
	FreedBytes    int64
	Skipped       bool
}

// Run scores every pack and reaps the eligible ones above the line:
// garbage >= 0 forces that line, garbage < 0 uses policy — 0.5, or 0.1
// under min-free pressure. A cycle that would repeat the previous one
// (union unchanged, same eligible packs, same line) is skipped.
func (c *Collector) Run(ctx context.Context, garbage float64) (CycleStats, error) {
	if !c.cycleMu.TryLock() {
		return CycleStats{}, ErrCycleRunning
	}
	defer c.cycleMu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancelCycle = cancel
	c.mu.Unlock()
	defer func() {
		cancel()
		c.mu.Lock()
		c.cancelCycle = nil
		c.mu.Unlock()
	}()
	stats, err := c.cycle(ctx, garbage)
	c.mu.Lock()
	c.last = &stats
	c.lastErr = err
	c.mu.Unlock()
	return stats, err
}

func (c *Collector) cycle(ctx context.Context, garbage float64) (stats CycleStats, err error) {
	stats.Start = time.Now()
	defer func() { stats.Duration = time.Since(stats.Start) }()

	// Ground truth first: every named root has a closure before any cycle.
	if err := c.ensureClosures(ctx); err != nil {
		return stats, err
	}

	// snapshot: union pointer; horizon.
	u := c.union.Load()
	horizon := c.horizon(time.Now())

	threshold := garbage
	if threshold < 0 {
		threshold = c.opts.Garbage
		if freeBelow(c.d.path, c.opts.MinFree) {
			threshold = minFreeGarbage
		}
	}
	stats.Threshold = threshold

	scores, err := c.score(ctx, u, horizon)
	if err != nil {
		return stats, err
	}
	stats.Scored = len(scores)

	var eligible []uint64
	for _, sc := range scores {
		if sc.Eligible {
			eligible = append(eligible, sc.ID)
		}
	}
	slices.Sort(eligible)
	c.mu.Lock()
	unchanged := c.haveLast && c.lastGen == u.gen &&
		slices.Equal(c.lastEligible, eligible) && c.lastThreshold == threshold
	c.mu.Unlock()
	if unchanged {
		stats.Skipped = true
		return stats, nil
	}

	victims := make([]PackStatus, 0)
	for _, sc := range scores {
		if sc.Eligible && sc.Garbage > threshold {
			victims = append(victims, sc)
		}
	}
	slices.SortFunc(victims, func(a, b PackStatus) int {
		return cmp.Compare(b.Garbage, a.Garbage) // most garbage first
	})

	th := newThrottle(c.opts.Rate)
	reaped := make(map[uint64]bool)
	for _, v := range victims {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		records, bytes, err := c.reap(ctx, u, v.ID, th)
		if err != nil {
			return stats, err
		}
		extra, err := c.deletePack(v.ID)
		if err != nil {
			return stats, err
		}
		reaped[v.ID] = true
		stats.Reaped = append(stats.Reaped, v.ID)
		stats.CopiedRecords += records + extra.records
		stats.CopiedBytes += bytes + extra.bytes
		stats.FreedBytes += v.Body
	}

	remaining := slices.DeleteFunc(eligible, func(id uint64) bool { return reaped[id] })
	c.mu.Lock()
	c.haveLast = true
	c.lastGen = u.gen // the snapshot this cycle scored against, not the live union
	c.lastEligible = remaining
	c.lastThreshold = threshold
	c.mu.Unlock()
	return stats, nil
}

// ensureClosures walks every named root that has no valid closure — at
// first start on an existing store, before any cycle runs. A root that
// cannot be walked (its reference already dangles: pre-collector damage)
// aborts the cycle with the error naming it. The walk holds the same
// walking[root] reservation PrepareRef does, so a concurrent ReleaseRef
// dropping the root's last name mid-walk cannot leave a stale closure file
// on disk (a later PrepareRef reading it would skip its own walk and admit
// a reference to objects a cycle may since have reaped).
func (c *Collector) ensureClosures(ctx context.Context) error {
	c.mu.Lock()
	roots := slices.Collect(maps.Keys(c.pending))
	c.mu.Unlock()
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if !c.pending[root] { // resolved or released while we walked others
			c.mu.Unlock()
			continue
		}
		c.walking[root]++
		c.mu.Unlock()

		tails, err := c.ensureClosure(root)

		c.mu.Lock()
		c.walking[root]--
		if c.walking[root] == 0 {
			delete(c.walking, root)
		}
		switch {
		case err != nil:
			c.mu.Unlock()
			return err
		case c.roots[root] == 0:
			// Released while we walked: the closure must not outlive the
			// last name — a stale file would let a later PUT skip its walk.
			// A failed removal breaks that invariant, so no cycle may run
			// past it.
			if c.walking[root] == 0 {
				if rerr := c.d.remove(root); rerr != nil {
					delete(c.pending, root)
					c.mu.Unlock()
					return fmt.Errorf("gc: removing stale closure for root %s: %w", root, rerr)
				}
			}
			delete(c.pending, root)
		case c.pending[root]:
			c.union.Store(c.union.Load().merge(tails, int32(c.roots[root])))
			delete(c.pending, root)
		}
		c.mu.Unlock()
	}
	return nil
}

// score walks every pack's footer index against the union, packs in
// parallel: an entry is live iff its tail is in the union;
// garbage = 1 − Σ (46 + slen) / body. No pack body is read.
func (c *Collector) score(ctx context.Context, u *union, horizon time.Time) ([]PackStatus, error) {
	segs, err := c.objects.Segments()
	if err != nil {
		return nil, err
	}
	out := make([]PackStatus, len(segs))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(c.opts.Jobs)
	for i, seg := range segs {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var live int64
			err := c.objects.ScanIndex(seg.ID, func(k key.Key, off uint64, slen uint32) {
				if u.contains(Tail(k)) {
					live += int64(amberpack.RecHeaderSize) + int64(slen)
				}
			})
			if errors.Is(err, packstore.ErrUnknownSegment) {
				return nil // removed while scoring (a concurrent Status); skip
			}
			if err != nil {
				return err
			}
			ps := PackStatus{ID: seg.ID, Sealed: seg.Sealed, Body: seg.Body, Keys: seg.Keys,
				Live: live, Eligible: seg.Sealed.Before(horizon)}
			if seg.Body > 0 {
				ps.Garbage = 1 - float64(live)/float64(seg.Body)
			}
			out[i] = ps
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(out, func(p PackStatus) bool { return p.ID == 0 }), nil
}

// liveEntry is one record to copy, snapshotted from the footer index.
type liveEntry struct {
	k    key.Key
	off  uint64
	slen uint32
}

// liveIn lists the victim's live entries against u, in file order.
func (c *Collector) liveIn(id uint64, u *union) ([]liveEntry, error) {
	var live []liveEntry
	err := c.objects.ScanIndex(id, func(k key.Key, off uint64, slen uint32) {
		if u.contains(Tail(k)) {
			live = append(live, liveEntry{k, off, slen})
		}
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(live, func(a, b liveEntry) int { return cmp.Compare(a.off, b.off) })
	return live, nil
}

// copyLive re-appends the live records not already stored outside the
// victim: read in file order, CRC-checked, appended raw. Batches share one
// fsync; the throttle caps bytes/s.
func (c *Collector) copyLive(ctx context.Context, id uint64, live []liveEntry, th *throttle) (int, int64, error) {
	const batchBytes = 8 << 20
	var records int
	var copied, inBatch int64
	for _, e := range live {
		if err := ctx.Err(); err != nil {
			return records, copied, err
		}
		outside, err := c.objects.HasOutside(id, e.k)
		if err != nil {
			return records, copied, err
		}
		if outside {
			continue
		}
		raw, err := c.objects.Record(id, e.off)
		if err != nil {
			return records, copied, err
		}
		if err := c.objects.AppendRecord(e.k, raw); err != nil {
			return records, copied, err
		}
		records++
		copied += int64(len(raw))
		inBatch += int64(len(raw))
		th.pace(len(raw))
		if inBatch >= batchBytes {
			if err := c.objects.Sync(); err != nil {
				return records, copied, err
			}
			inBatch = 0
		}
	}
	if err := c.objects.Sync(); err != nil {
		return records, copied, err
	}
	return records, copied, nil
}

// reap copies a victim's live records against the snapshot union; the
// victim is immutable and stays readable throughout. A pack with no live
// bytes is deleted without copying.
func (c *Collector) reap(ctx context.Context, u *union, id uint64, th *throttle) (int, int64, error) {
	live, err := c.liveIn(id, u)
	if err != nil {
		return 0, 0, err
	}
	if len(live) == 0 {
		return 0, 0, nil
	}
	return c.copyLive(ctx, id, live, th)
}

type copyStats struct {
	records int
	bytes   int64
}

// deletePack removes a fully-reaped victim. Reaping used the snapshot
// union; references written since may name more of it. Under the removal
// lock held exclusively the victim's index is re-tested against the
// current union and the new hits copied; then the pack leaves the probe
// list, is unmapped after in-flight scrubs, unlinked, and the directory
// fsynced.
func (c *Collector) deletePack(id uint64) (copyStats, error) {
	c.removal.Lock()
	defer c.removal.Unlock()
	live, err := c.liveIn(id, c.union.Load())
	if err != nil {
		return copyStats{}, err
	}
	// HasOutside skips everything the reap already copied.
	records, bytes, err := c.copyLive(context.Background(), id, live, newThrottle(0))
	if err != nil {
		return copyStats{records, bytes}, err
	}
	if err := c.objects.Remove(id); err != nil {
		return copyStats{records, bytes}, err
	}
	return copyStats{records, bytes}, nil
}

// throttle paces the copier to rate bytes/s; zero rate never sleeps.
type throttle struct {
	rate  int64
	start time.Time
	bytes int64
}

func newThrottle(rate int64) *throttle {
	return &throttle{rate: rate, start: time.Now()}
}

func (t *throttle) pace(n int) {
	if t.rate <= 0 {
		return
	}
	t.bytes += int64(n)
	ahead := throttleOwed(t.bytes, t.rate) - time.Since(t.start)
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
