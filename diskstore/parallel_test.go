package diskstore_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/diskstore"
)

func TestWriteParallel_MixedRetrievable(t *testing.T) {
	s := openTemp(t)

	small1 := []byte("small one")
	small2 := []byte("small two")
	large := bytes.Repeat([]byte("L"), diskstore.DefaultInlineThreshold+1)
	objs := []diskstore.Object{
		{Key: mkKey(t, small1), Data: small1},
		{Key: mkKey(t, large), Data: large},
		{Key: mkKey(t, small2), Data: small2},
	}

	if err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 4}); err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}

	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
		if !bytes.Equal(got, o.Data) {
			t.Fatalf("Get(%s) = %d bytes, want %d", o.Key, len(got), len(o.Data))
		}
	}
}

func TestWriteParallel_DedupLargeWritesOneFile(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir, diskstore.WithSync(false))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	large := bytes.Repeat([]byte("D"), diskstore.DefaultInlineThreshold+1)
	k := mkKey(t, large)
	dup := func(yield func(diskstore.Object, error) bool) {
		for range 3 {
			if !yield(diskstore.Object{Key: k, Data: large}, nil) {
				return
			}
		}
	}
	if err := s.WriteParallel(dup, diskstore.WriteOpts{Writers: 4}); err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}
	if n := countBlobFiles(t, dir); n != 1 {
		t.Fatalf("blob files after 3 identical objects = %d, want 1", n)
	}
}

func TestWriteParallel_SurfacesIteratorError(t *testing.T) {
	s := openTemp(t)
	boom := errors.New("producer blew up")
	d := []byte("first")
	seq := func(yield func(diskstore.Object, error) bool) {
		if !yield(diskstore.Object{Key: mkKey(t, d), Data: d}, nil) {
			return
		}
		yield(diskstore.Object{}, boom)
	}
	err := s.WriteParallel(seq, diskstore.WriteOpts{Writers: 2})
	if !errors.Is(err, boom) {
		t.Fatalf("WriteParallel err = %v, want %v", err, boom)
	}
}
