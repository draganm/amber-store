package gc

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
)

// Status is the gc report: per-pack scores against the current union and
// horizon, totals, closure and union sizes, the last cycle. While Pending
// is nonzero (named roots not yet walked) the scores overstate garbage;
// the first cycle resolves it.
type Status struct {
	Packs        []PackStatus
	LiveBytes    int64
	GarbageBytes int64
	Refs         int // reference names
	Closures     int // closure files on disk
	Pending      int // named roots without a valid closure yet
	Union        int // live tails
	Last         *CycleStats
	LastError    string
}

// Status scores every pack against the current union — seconds on a large
// store, no pack body read.
func (c *Collector) Status(ctx context.Context) (Status, error) {
	u := c.union.Load()
	scores, err := c.score(ctx, u, c.horizon(time.Now()))
	if err != nil {
		return Status{}, err
	}
	st := Status{Packs: scores, Union: u.size()}
	for _, p := range scores {
		st.LiveBytes += p.Live
		st.GarbageBytes += p.Body - p.Live
	}
	onDisk, err := c.d.list()
	if err != nil {
		return Status{}, err
	}
	st.Closures = len(onDisk)
	c.mu.Lock()
	for _, n := range c.roots {
		st.Refs += n
	}
	st.Pending = len(c.pending)
	st.Last = c.last
	if c.lastErr != nil {
		st.LastError = c.lastErr.Error()
	}
	c.mu.Unlock()
	return st, nil
}

// Why returns the sorted names of the references whose closure holds k's
// tail — why the object is alive.
func (c *Collector) Why(k key.Key) ([]string, error) {
	recs, err := c.refs.All()
	if err != nil {
		return nil, err
	}
	t := Tail(k)
	byRoot := make(map[key.Key][]string)
	for _, r := range recs {
		ref, err := reference.Decode(r.Data)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		root, err := key.Parse(ref.Key)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		byRoot[root] = append(byRoot[root], r.Name)
	}
	var names []string
	for root, ns := range byRoot {
		tails, ok := c.d.read(root)
		if !ok {
			continue
		}
		if _, found := slices.BinarySearch(tails, t); found {
			names = append(names, ns...)
		}
	}
	slices.Sort(names)
	return names, nil
}
