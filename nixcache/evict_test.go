package nixcache

import (
	"fmt"
	"slices"
	"testing"
)

func admitN(p *evictPolicy, n int, size uint64) []string {
	hps := make([]string, n)
	for i := range hps {
		hps[i] = fmt.Sprintf("path%03d", i)
		p.Admit(hps[i], size)
	}
	return hps
}

func TestEvictOneHitWonders(t *testing.T) {
	p := newEvictPolicy(1000)
	hps := admitN(p, 12, 100) // 1200 used, all probationary
	victims, _ := p.Evict()
	if len(victims) != 2 {
		t.Fatalf("victims: %v", victims)
	}
	for _, v := range victims {
		if !slices.Contains(hps[:2], v) {
			t.Fatalf("evicted %s, want oldest of %v", v, hps[:2])
		}
	}
	if p.used != 1000 {
		t.Fatalf("used: %d", p.used)
	}
}

func TestEvictTouchedSurvives(t *testing.T) {
	p := newEvictPolicy(1000)
	hps := admitN(p, 12, 100)
	p.Touch(hps[0]) // oldest, but hit: promotes to main
	victims, _ := p.Evict()
	if slices.Contains(victims, hps[0]) {
		t.Fatalf("touched path evicted: %v", victims)
	}
	if len(victims) != 2 {
		t.Fatalf("victims: %v", victims)
	}
}

func TestEvictGhostReadmitsToMain(t *testing.T) {
	p := newEvictPolicy(1000)
	hps := admitN(p, 12, 100)
	victims, _ := p.Evict()
	back := victims[0]
	p.Admit(back, 100) // ghost hit: straight to main
	// A fresh probationary path must lose to protect the readmitted one.
	p.Admit("fresh", 100)
	victims, _ = p.Evict()
	if slices.Contains(victims, back) {
		t.Fatalf("ghost-readmitted path evicted: %v", victims)
	}
	_ = hps
}

func TestEvictPinnedNeverEvicted(t *testing.T) {
	p := newEvictPolicy(300)
	hps := admitN(p, 4, 100)
	for _, hp := range hps {
		p.Pin(hp)
	}
	if victims, _ := p.Evict(); victims != nil {
		t.Fatalf("pinned paths evicted: %v", victims)
	}
	p.Unpin(hps[1])
	if victims, _ := p.Evict(); !slices.Equal(victims, []string{hps[1]}) {
		t.Fatalf("victims: %v, want only unpinned %s", victims, hps[1])
	}
}

func TestEvictHotMainRotates(t *testing.T) {
	p := newEvictPolicy(1000)
	hps := admitN(p, 10, 100)
	for _, hp := range hps {
		p.Touch(hp)
	}
	p.Evict() // promotes into main
	for _, hp := range hps {
		for range 3 {
			p.Touch(hp)
		}
	}
	p.Admit("newcomer", 500)
	victims, _ := p.Evict()
	if len(victims) == 0 {
		t.Fatal("hot main starved eviction")
	}
	if p.used > 1000 {
		t.Fatalf("used %d over budget", p.used)
	}
}

func TestEvictPinnedRotationDoesNotGrow(t *testing.T) {
	p := newEvictPolicy(1000)
	hps := admitN(p, 100, 100)
	for _, hp := range hps {
		p.Pin(hp)
	}
	for range 1000 {
		if victims, _ := p.Evict(); len(victims) != 0 {
			t.Fatalf("pinned path evicted: %v", victims)
		}
	}
	if c := cap(p.small.q) + cap(p.main.q); c > 4*len(hps) {
		t.Fatalf("queues grew to cap %d for %d live entries", c, len(hps))
	}
}
