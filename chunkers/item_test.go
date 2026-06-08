package chunkers

import "testing"

func TestNewItemChunker_DerivesBounds(t *testing.T) {
	c := NewItemChunker(7) // avg 128
	if c.MinRun != 32 || c.MaxRun != 512 {
		t.Errorf("bounds = min %d max %d, want 32/512", c.MinRun, c.MaxRun)
	}
}

func TestIsBoundary_BelowMinNeverBoundary(t *testing.T) {
	c := NewItemChunker(7)
	for runLen := 1; runLen < c.MinRun; runLen++ {
		if c.IsBoundary([]byte("anything"), runLen) {
			t.Fatalf("boundary at runLen %d below MinRun %d", runLen, c.MinRun)
		}
	}
}

func TestIsBoundary_AtMaxAlwaysBoundary(t *testing.T) {
	c := NewItemChunker(7)
	if !c.IsBoundary([]byte("x"), c.MaxRun) {
		t.Fatalf("no forced boundary at MaxRun %d", c.MaxRun)
	}
}

func TestIsBoundary_Deterministic(t *testing.T) {
	c := NewItemChunker(5)
	enc := []byte("some item encoding")
	first := c.IsBoundary(enc, 100)
	for range 5 {
		if c.IsBoundary(enc, 100) != first {
			t.Fatal("IsBoundary not deterministic")
		}
	}
}

func TestIsBoundary_HitsOnLowBitsZero(t *testing.T) {
	// With k=0 the mask is 0, so every item at or above MinRun is a boundary.
	c := NewItemChunker(0)
	if !c.IsBoundary([]byte("x"), c.MinRun) {
		t.Fatal("k=0 should make every item (>= MinRun) a boundary")
	}
	if !c.IsBoundary([]byte("anything else"), c.MinRun+5) {
		t.Fatal("k=0 should make every item (>= MinRun) a boundary")
	}
}
