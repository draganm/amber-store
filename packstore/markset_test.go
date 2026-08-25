package packstore

import "testing"

func TestMarkSet(t *testing.T) {
	s, objs := compactStore(t)
	m := s.NewMarkSet()
	for i, o := range objs {
		if m.Contains(o.Key) {
			t.Fatalf("object %d marked before Mark", i)
		}
		newly, present := m.Mark(o.Key)
		if !newly || !present {
			t.Fatalf("first Mark of object %d: newly=%v present=%v", i, newly, present)
		}
		newly, present = m.Mark(o.Key)
		if newly || !present {
			t.Fatalf("second Mark of object %d: newly=%v present=%v", i, newly, present)
		}
		if !m.Contains(o.Key) {
			t.Fatalf("object %d not marked after Mark", i)
		}
	}
	if m.Marked() != len(objs) {
		t.Fatalf("Marked() = %d, want %d", m.Marked(), len(objs))
	}
	absent := blobObj(t, []byte("never stored"))
	if newly, present := m.Mark(absent.Key); newly || present {
		t.Fatalf("absent key: newly=%v present=%v", newly, present)
	}
	if m.Contains(absent.Key) {
		t.Fatal("absent key marked")
	}
}

func TestMarkSetDrivesCompact(t *testing.T) {
	s, objs := compactStore(t)
	m := s.NewMarkSet()
	for _, i := range []int{0, 2, 4} {
		m.Mark(objs[i].Key)
	}
	stats, err := s.Compact(m.Contains, CompactOpts{MinDeadRatio: 0.4})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsCompacted != 2 || stats.RecordsCopied != 2 {
		t.Fatalf("stats: %+v", stats)
	}
}
