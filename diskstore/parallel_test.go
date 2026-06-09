package diskstore_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
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

	if _, err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 4}); err != nil {
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
	if _, err := s.WriteParallel(dup, diskstore.WriteOpts{Writers: 4}); err != nil {
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
	_, err := s.WriteParallel(seq, diskstore.WriteOpts{Writers: 2})
	if !errors.Is(err, boom) {
		t.Fatalf("WriteParallel err = %v, want %v", err, boom)
	}
}

func TestWriteParallel_Stats(t *testing.T) {
	s := openTemp(t)
	a := []byte("aaaa")
	b := []byte("bbbbbbbb")
	objs := []diskstore.Object{
		{Key: mkKey(t, a), Data: a},
		{Key: mkKey(t, b), Data: b},
		{Key: mkKey(t, a), Data: a}, // duplicate within the stream
	}
	stats, err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 1})
	if err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}
	if stats.Stored != 2 || stats.Deduped != 1 {
		t.Fatalf("stats = %+v, want Stored=2 Deduped=1", stats)
	}
	if stats.BytesStored != int64(len(a)+len(b)) {
		t.Fatalf("BytesStored = %d, want %d", stats.BytesStored, len(a)+len(b))
	}

	// Re-writing the same objects dedups everything.
	stats2, err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 1})
	if err != nil {
		t.Fatalf("WriteParallel (re-run): %v", err)
	}
	if stats2.Stored != 0 || stats2.Deduped != 3 {
		t.Fatalf("re-run stats = %+v, want Stored=0 Deduped=3", stats2)
	}
}

func TestWriteParallel_VerifyRejectsTamperedKey(t *testing.T) {
	s := openTemp(t)
	data := []byte("honest payload")
	good := mkKey(t, data)
	// Flip one hash byte to make a key that does not match the payload.
	bad := good
	bad[key.Size-1] ^= 0xFF
	seq := seqOf(diskstore.Object{Key: bad, Data: data})

	_, err := s.WriteParallel(seq, diskstore.WriteOpts{Writers: 1, Verify: true})
	if !errors.Is(err, diskstore.ErrVerify) {
		t.Fatalf("err = %v, want ErrVerify", err)
	}
	has, _ := s.Has(bad)
	if has {
		t.Fatalf("tampered object must not be stored")
	}
}

func TestWriteParallel_VerifyAcceptsHonestObjects(t *testing.T) {
	s := openTemp(t)
	data := []byte("honest payload")
	_, err := s.WriteParallel(seqOf(diskstore.Object{Key: mkKey(t, data), Data: data}),
		diskstore.WriteOpts{Writers: 1, Verify: true})
	if err != nil {
		t.Fatalf("WriteParallel verify: %v", err)
	}
}
