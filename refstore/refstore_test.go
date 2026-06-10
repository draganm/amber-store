package refstore_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/refstore"
)

func open(t *testing.T, dir string) *refstore.Store {
	t.Helper()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := open(t, t.TempDir())

	if _, err := s.Get("missing"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := s.Put("a/b", []byte("rec1")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec1")) {
		t.Fatalf("Get = %q, want rec1", got)
	}
	// Overwrite is unconditional.
	if err := s.Put("a/b", []byte("rec2")); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec2")) {
		t.Fatalf("Get after overwrite = %q, want rec2", got)
	}
	if err := s.Delete("a/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Delete(absent) = %v, want ErrNotFound", err)
	}
}

func TestAllSortedByName(t *testing.T) {
	s := open(t, t.TempDir())
	for _, n := range []string{"zeta", "alpha", "mid/dle"} {
		if err := s.Put(n, []byte("v-"+n)); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"alpha", "mid/dle", "zeta"}
	if len(recs) != len(wantNames) {
		t.Fatalf("All returned %d records, want %d", len(recs), len(wantNames))
	}
	for i, want := range wantNames {
		if recs[i].Name != want {
			t.Fatalf("recs[%d].Name = %q, want %q", i, recs[i].Name, want)
		}
		if !bytes.Equal(recs[i].Data, []byte("v-"+want)) {
			t.Fatalf("recs[%d].Data = %q", i, recs[i].Data)
		}
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, dir)
	got, err := s2.Get("keep")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen = %q, want v", got)
	}
}
