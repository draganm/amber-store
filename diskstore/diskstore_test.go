package diskstore_test

import (
	"bytes"
	"errors"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
)

// seqOf yields objs in order with no error.
func seqOf(objs ...diskstore.Object) iter.Seq2[diskstore.Object, error] {
	return func(yield func(diskstore.Object, error) bool) {
		for _, o := range objs {
			if !yield(o, nil) {
				return
			}
		}
	}
}

// countBlobFiles returns the number of regular files under dir/blobs.
func countBlobFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	root := filepath.Join(dir, "blobs")
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk %s: %v", root, err)
	}
	return n
}

// mkKey builds a canonical Blob key for data.
func mkKey(t *testing.T, data []byte) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatalf("key.New: %v", err)
	}
	return k
}

func openTemp(t *testing.T, opts ...diskstore.Option) *diskstore.Store {
	t.Helper()
	s, err := diskstore.Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetSmallObject(t *testing.T) {
	s := openTemp(t)
	data := []byte("hello, amber")
	k := mkKey(t, data)

	if err := s.Put(k, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
}

func TestPutGetLargeObject(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	data := bytes.Repeat([]byte("x"), diskstore.DefaultInlineThreshold+1) // > threshold -> external
	k := mkKey(t, data)

	if err := s.Put(k, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(data))
	}
	if n := countBlobFiles(t, dir); n != 1 {
		t.Fatalf("blob files under blobs/ = %d, want 1", n)
	}
}

func TestWriteBatchMixedRetrievable(t *testing.T) {
	s := openTemp(t)

	small1 := []byte("small one")
	small2 := []byte("small two")
	large := bytes.Repeat([]byte("L"), diskstore.DefaultInlineThreshold+1)
	objs := []diskstore.Object{
		{Key: mkKey(t, small1), Data: small1},
		{Key: mkKey(t, large), Data: large},
		{Key: mkKey(t, small2), Data: small2},
	}

	if err := s.WriteBatch(seqOf(objs...)); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
		if !bytes.Equal(got, o.Data) {
			t.Fatalf("Get(%s) = %d bytes, want %d", o.Key, len(got), len(o.Data))
		}
		has, err := s.Has(o.Key)
		if err != nil || !has {
			t.Fatalf("Has(%s) = %v, %v; want true, nil", o.Key, has, err)
		}
	}
}

func TestWriteBatchErrorRollsBack(t *testing.T) {
	s := openTemp(t)

	d1 := []byte("first")
	d2 := []byte("second")
	k1 := mkKey(t, d1)
	k2 := mkKey(t, d2)
	boom := errors.New("producer blew up")

	seq := func(yield func(diskstore.Object, error) bool) {
		if !yield(diskstore.Object{Key: k1, Data: d1}, nil) {
			return
		}
		if !yield(diskstore.Object{Key: k2, Data: d2}, nil) {
			return
		}
		yield(diskstore.Object{}, boom)
	}

	err := s.WriteBatch(seq)
	if !errors.Is(err, boom) {
		t.Fatalf("WriteBatch err = %v, want %v", err, boom)
	}

	for _, k := range []key.Key{k1, k2} {
		has, err := s.Has(k)
		if err != nil {
			t.Fatalf("Has(%s): %v", k, err)
		}
		if has {
			t.Fatalf("Has(%s) = true after rollback; want false", k)
		}
	}
}

func TestThresholdBoundaryStaysInline(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	data := bytes.Repeat([]byte("b"), diskstore.DefaultInlineThreshold) // == threshold -> inline
	k := mkKey(t, data)
	if err := s.Put(k, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(data))
	}
	if n := countBlobFiles(t, dir); n != 0 {
		t.Fatalf("blob files at threshold = %d, want 0 (inline)", n)
	}
}

func TestHasMissingAndPresent(t *testing.T) {
	s := openTemp(t)
	data := []byte("present")
	k := mkKey(t, data)

	if has, err := s.Has(k); err != nil || has {
		t.Fatalf("Has(missing) = %v, %v; want false, nil", has, err)
	}
	if err := s.Put(k, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if has, err := s.Has(k); err != nil || !has {
		t.Fatalf("Has(present) = %v, %v; want true, nil", has, err)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	s := openTemp(t)
	_, err := s.Get(mkKey(t, []byte("never stored")))
	if !errors.Is(err, diskstore.ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
}

func TestDedupLargeObjectWritesOneFile(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	data := bytes.Repeat([]byte("D"), diskstore.DefaultInlineThreshold+1)
	k := mkKey(t, data)
	for i := range 3 {
		if err := s.Put(k, data); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}
	if n := countBlobFiles(t, dir); n != 1 {
		t.Fatalf("blob files after 3 identical Puts = %d, want 1", n)
	}
}

func TestEmptyData(t *testing.T) {
	s := openTemp(t)
	k := mkKey(t, []byte{})
	if err := s.Put(k, []byte{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get returned %d bytes, want 0", len(got))
	}
}

func TestWithInlineThresholdControlsSpill(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir, diskstore.WithInlineThreshold(8))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	inline := []byte("12345678") // == threshold -> inline
	spill := []byte("123456789") // > threshold -> external
	ki := mkKey(t, inline)
	ks := mkKey(t, spill)
	if err := s.Put(ki, inline); err != nil {
		t.Fatalf("Put inline: %v", err)
	}
	if n := countBlobFiles(t, dir); n != 0 {
		t.Fatalf("blob files after inline Put = %d, want 0", n)
	}
	if err := s.Put(ks, spill); err != nil {
		t.Fatalf("Put spill: %v", err)
	}
	if n := countBlobFiles(t, dir); n != 1 {
		t.Fatalf("blob files after spill Put = %d, want 1", n)
	}

	for _, tc := range []struct {
		k    key.Key
		want []byte
	}{{ki, inline}, {ks, spill}} {
		got, err := s.Get(tc.k)
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("Get(%s) = %q, %v; want %q", tc.k, got, err, tc.want)
		}
	}
}

func TestWithSyncDisabledRoundTrips(t *testing.T) {
	s := openTemp(t, diskstore.WithSync(false))
	small := []byte("no fsync small")
	large := bytes.Repeat([]byte("N"), diskstore.DefaultInlineThreshold+1)
	for _, data := range [][]byte{small, large} {
		k := mkKey(t, data)
		if err := s.Put(k, data); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := s.Get(k)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("Get(%s) = %d bytes, %v; want %d", k, len(got), err, len(data))
		}
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	small := []byte("durable small")
	large := bytes.Repeat([]byte("P"), diskstore.DefaultInlineThreshold+1)
	ks := mkKey(t, small)
	kl := mkKey(t, large)
	if err := s.Put(ks, small); err != nil {
		t.Fatalf("Put small: %v", err)
	}
	if err := s.Put(kl, large); err != nil {
		t.Fatalf("Put large: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := diskstore.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	for _, tc := range []struct {
		k    key.Key
		want []byte
	}{{ks, small}, {kl, large}} {
		got, err := s2.Get(tc.k)
		if err != nil {
			t.Fatalf("Get(%s) after reopen: %v", tc.k, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("Get(%s) after reopen = %d bytes, want %d", tc.k, len(got), len(tc.want))
		}
	}
}
