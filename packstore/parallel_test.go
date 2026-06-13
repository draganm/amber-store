package packstore

import (
	"bytes"
	"errors"
	"testing"
)

func TestWriteParallelStoresAll(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSegmentSize(32<<10))
	objs := testObjects(t, 300)
	batch := append(append([]Object{}, objs...), objs[:50]...) // 50 in-stream dups
	stats, err := s.WriteParallel(objSeq(batch, -1), WriteOpts{Writers: 4, BatchSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != len(objs) {
		t.Fatalf("Stored = %d, want %d", stats.Stored, len(objs))
	}
	if stats.Deduped != 50 {
		t.Fatalf("Deduped = %d, want 50", stats.Deduped)
	}
	var wantBytes int64
	for _, o := range objs {
		wantBytes += int64(len(o.Data))
	}
	if stats.BytesStored != wantBytes {
		t.Fatalf("BytesStored = %d, want %d", stats.BytesStored, wantBytes)
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
}

func TestWriteParallelSkipsExisting(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 20)
	for _, o := range objs[:10] {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 10 || stats.Deduped != 10 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestWriteParallelVerifyCatchesMismatch(t *testing.T) {
	s := openStore(t, t.TempDir())
	good := testObjects(t, 5)
	bad := good[2]
	bad.Data = append(bytes.Clone(bad.Data), 0xFF) // payload no longer matches the key
	objs := append(append([]Object{}, good[:2]...), bad)
	_, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{Verify: true})
	if !errors.Is(err, ErrVerify) {
		t.Fatalf("err = %v, want ErrVerify", err)
	}
}

func TestWriteParallelIteratorError(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 10)
	_, err := s.WriteParallel(objSeq(objs, 7), WriteOpts{Writers: 2})
	if err == nil {
		t.Fatal("want iterator error")
	}
}
