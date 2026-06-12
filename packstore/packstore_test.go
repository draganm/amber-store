package packstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openStore(t *testing.T, dir string, opts ...Option) *Store {
	t.Helper()
	s, err := Open(dir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetHasRoundTrip(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 50)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range objs {
		has, err := s.Has(o.Key)
		if err != nil || !has {
			t.Fatalf("Has(%s) = %v, %v", o.Key, has, err)
		}
		data, err := s.Get(o.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): payload mismatch", o.Key)
		}
	}
}

func TestGetAbsentReturnsErrNotFound(t *testing.T) {
	s := openStore(t, t.TempDir())
	_, err := s.Get(blobObj(t, []byte("nope")).Key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	has, err := s.Has(blobObj(t, []byte("nope")).Key)
	if err != nil || has {
		t.Fatalf("Has = %v, %v", has, err)
	}
}

func TestPutIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	o := blobObj(t, incompressible(1000))
	for range 3 {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	// Three identical Puts must append exactly one record.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.seg.active"))
	if err != nil || len(files) != 1 {
		t.Fatalf("glob: %v %v", files, err)
	}
	st, err := os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	rec, err := encodeRecord(o.Key, o.Data)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(magicHeader) + len(rec)); st.Size() != want {
		t.Fatalf("active size = %d, want %d", st.Size(), want)
	}
}

func TestReopenResumesActiveSegment(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	first := testObjects(t, 10)
	for _, o := range first {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, dir)
	for _, o := range first {
		data, err := s2.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("after reopen Get(%s): %v", o.Key, err)
		}
	}
	more := blobObj(t, []byte("written after reopen"))
	if err := s2.Put(more.Key, more.Data); err != nil {
		t.Fatal(err)
	}
	if data, err := s2.Get(more.Key); err != nil || !bytes.Equal(data, more.Data) {
		t.Fatalf("Get after resume append: %v", err)
	}
	// Still exactly one active segment file, no sealed ones.
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	actives, _ := filepath.Glob(filepath.Join(dir, "*.seg.active"))
	if len(segs) != 0 || len(actives) != 1 {
		t.Fatalf("segs=%v actives=%v", segs, actives)
	}
}

func TestReopenTruncatesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 5)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a torn write: append garbage to the active file.
	actives, _ := filepath.Glob(filepath.Join(dir, "*.seg.active"))
	f, err := os.OpenFile(actives[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x55, 0x44, 0x33}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2 := openStore(t, dir)
	for _, o := range objs {
		if data, err := s2.Get(o.Key); err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s) after torn-tail recovery: %v", o.Key, err)
		}
	}
	// New writes land cleanly after the truncated tail.
	o := blobObj(t, []byte("post-recovery write"))
	if err := s2.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3 := openStore(t, dir)
	if data, err := s3.Get(o.Key); err != nil || !bytes.Equal(data, o.Data) {
		t.Fatalf("Get after second reopen: %v", err)
	}
}

func TestSecondOpenFails(t *testing.T) {
	dir := t.TempDir()
	_ = openStore(t, dir)
	if _, err := Open(dir); err == nil {
		t.Fatal("second Open must fail while the first holds the flock")
	}
}

func TestMultipleActiveFilesFailOpen(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"0000000000000001.seg.active", "0000000000000002.seg.active"} {
		if err := os.WriteFile(filepath.Join(dir, name), magicHeader, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("want error for two active segments")
	}
}

func TestClosedStoreErrors(t *testing.T) {
	s := openStore(t, t.TempDir())
	o := blobObj(t, []byte("x"))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(o.Key); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close: %v", err)
	}
	if err := s.Put(o.Key, o.Data); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: %v", err)
	}
	if _, err := s.Has(o.Key); !errors.Is(err, ErrClosed) {
		t.Fatalf("Has after Close: %v", err)
	}
}

func TestWithSyncFalse(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSync(false))
	o := blobObj(t, incompressible(100))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if data, err := s.Get(o.Key); err != nil || !bytes.Equal(data, o.Data) {
		t.Fatalf("Get: %v", err)
	}
}
