// The union of all closures: every live tail and the number of references
// reaching it, ≈12 B per live object plus a fixed 256 KiB fanout per union.
// Immutable; merges build a fresh union that is swapped in. Membership is a
// binary search under a 64 Ki-entry fanout on the top tail bits. Fanout
// offsets are uint32, so a union holds at most 2^32 entries (~51 GB —
// unreachable in practice).

package gc

import (
	"cmp"
	"slices"
)

const unionFanout = 1 << 16

type union struct {
	tails  []uint64
	counts []uint32
	fanout []uint32 // unionFanout+1 cumulative bucket starts on tail>>48
	gen    uint64   // bumped by every merge; cycle-skip bookkeeping
}

type tailCount struct {
	tail  uint64
	count uint32
}

// buildUnion aggregates (tail, count) pairs — any order, duplicate tails
// summed — into a fresh union.
func buildUnion(pairs []tailCount) *union {
	slices.SortFunc(pairs, func(a, b tailCount) int { return cmp.Compare(a.tail, b.tail) })
	u := &union{}
	for _, p := range pairs {
		if n := len(u.tails); n > 0 && u.tails[n-1] == p.tail {
			u.counts[n-1] += p.count
		} else {
			u.tails = append(u.tails, p.tail)
			u.counts = append(u.counts, p.count)
		}
	}
	u.index()
	return u
}

func (u *union) index() {
	u.fanout = make([]uint32, unionFanout+1)
	for _, t := range u.tails {
		u.fanout[(t>>48)+1]++
	}
	for i := 1; i <= unionFanout; i++ {
		u.fanout[i] += u.fanout[i-1]
	}
}

// contains is the liveness test: one or two cache misses, exact.
func (u *union) contains(t uint64) bool {
	lo, hi := u.fanout[t>>48], u.fanout[(t>>48)+1]
	_, ok := slices.BinarySearch(u.tails[lo:hi], t)
	return ok
}

// merge returns a fresh union with delta applied to every tail in tails
// (sorted, deduplicated): one sequential pass over the union, plus a fixed
// fanout rebuild (65 536 buckets, ~256 KiB) — merges cost the same for tiny
// unions as the pass itself does for big ones. Counts reaching zero drop
// out; a negative delta on an absent tail is ignored.
func (u *union) merge(tails []uint64, delta int32) *union {
	nu := &union{
		tails:  make([]uint64, 0, len(u.tails)+len(tails)),
		counts: make([]uint32, 0, len(u.tails)+len(tails)),
		gen:    u.gen + 1,
	}
	i, j := 0, 0
	for i < len(u.tails) || j < len(tails) {
		switch {
		case j == len(tails) || (i < len(u.tails) && u.tails[i] < tails[j]):
			nu.tails = append(nu.tails, u.tails[i])
			nu.counts = append(nu.counts, u.counts[i])
			i++
		case i == len(u.tails) || tails[j] < u.tails[i]:
			if delta > 0 {
				nu.tails = append(nu.tails, tails[j])
				nu.counts = append(nu.counts, uint32(delta))
			}
			j++
		default:
			if c := int64(u.counts[i]) + int64(delta); c > 0 {
				nu.tails = append(nu.tails, u.tails[i])
				nu.counts = append(nu.counts, uint32(c))
			}
			i++
			j++
		}
	}
	nu.index()
	return nu
}

func (u *union) size() int {
	return len(u.tails)
}
