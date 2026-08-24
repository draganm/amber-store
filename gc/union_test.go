package gc

import (
	"math/rand/v2"
	"slices"
	"testing"
)

func TestUnionBuildAndContains(t *testing.T) {
	u := buildUnion([]tailCount{{9, 1}, {3, 2}, {9, 1}, {1 << 50, 1}})
	if u.size() != 3 {
		t.Fatalf("size = %d, want 3", u.size())
	}
	for _, tail := range []uint64{3, 9, 1 << 50} {
		if !u.contains(tail) {
			t.Errorf("missing tail %d", tail)
		}
	}
	for _, tail := range []uint64{0, 4, 1<<50 + 1, ^uint64(0)} {
		if u.contains(tail) {
			t.Errorf("phantom tail %d", tail)
		}
	}
	if empty := buildUnion(nil); empty.size() != 0 || empty.contains(0) {
		t.Error("empty union misbehaves")
	}
}

func TestUnionMerge(t *testing.T) {
	u := buildUnion([]tailCount{{5, 1}, {7, 2}})
	u2 := u.merge([]uint64{5, 6}, 1)
	if u2.gen <= u.gen {
		t.Error("merge did not bump gen")
	}
	if !u2.contains(6) || !u2.contains(5) || !u2.contains(7) {
		t.Error("merge-in lost tails")
	}
	if u.contains(6) {
		t.Error("merge mutated the original")
	}
	u3 := u2.merge([]uint64{5, 6}, -1)
	if u3.contains(6) {
		t.Error("count-0 tail survives")
	}
	if !u3.contains(5) {
		t.Error("tail 5 (count 2-1=1... built with 1, +1, -1 = 1) must survive")
	}
	u4 := u3.merge([]uint64{5}, -1)
	if u4.contains(5) {
		t.Error("tail 5 should be gone")
	}
	if !u4.contains(7) {
		t.Error("untouched tail lost")
	}
	// Negative delta on an absent tail is ignored, not underflowed.
	u5 := u4.merge([]uint64{123456}, -1)
	if u5.contains(123456) {
		t.Error("absent tail materialized")
	}
}

func TestUnionMergeModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	model := map[uint64]int64{}
	u := buildUnion(nil)
	for round := 0; round < 200; round++ {
		n := rng.IntN(8) + 1
		tails := make([]uint64, 0, n)
		for i := 0; i < n; i++ {
			tails = append(tails, uint64(rng.IntN(64))<<48|uint64(rng.IntN(16)))
		}
		slices.Sort(tails)
		tails = slices.Compact(tails)
		delta := int32(1)
		if rng.IntN(2) == 0 {
			delta = -1
		}
		for _, tail := range tails {
			model[tail] += int64(delta)
			if model[tail] <= 0 {
				delete(model, tail) // −1 on an absent tail nets to a no-op, like merge
			}
		}
		u = u.merge(tails, delta)
		if u.size() != len(model) {
			t.Fatalf("round %d: size %d, model %d", round, u.size(), len(model))
		}
		for tail := range model {
			if !u.contains(tail) {
				t.Fatalf("round %d: missing %d", round, tail)
			}
		}
	}
}
