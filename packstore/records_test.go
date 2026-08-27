package packstore

import (
	"errors"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
)

func recordSeq(t *testing.T, objs []Object) []amberpack.RawRecord {
	t.Helper()
	var out []amberpack.RawRecord
	for _, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, amberpack.RawRecord{Key: o.Key, Bytes: rec})
	}
	return out
}

func yieldAll(recs []amberpack.RawRecord) func(func(amberpack.RawRecord, error) bool) {
	return func(yield func(amberpack.RawRecord, error) bool) {
		for _, r := range recs {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func TestWriteRecords(t *testing.T) {
	s, err := Open(t.TempDir(), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	objs := []Object{blobObj(t, incompressible(4<<10)), blobObj(t, compressible(4<<10))}
	recs := recordSeq(t, objs)

	stats, err := s.WriteRecords(yieldAll(recs), WriteOpts{}, nil)
	if err != nil || stats.Stored != 2 {
		t.Fatalf("stats %+v, err %v", stats, err)
	}
	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil || !slicesEqual(got, o.Data) {
			t.Fatalf("round trip: %v", err)
		}
	}
	// Idempotent: a re-run dedups everything.
	stats, err = s.WriteRecords(yieldAll(recs), WriteOpts{}, nil)
	if err != nil || stats.Stored != 0 || stats.Deduped != 2 {
		t.Fatalf("re-run stats %+v, err %v", stats, err)
	}
}

func TestWriteRecordsRejectsCorrupt(t *testing.T) {
	s, err := Open(t.TempDir(), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recs := recordSeq(t, []Object{blobObj(t, incompressible(4<<10))})
	recs[0].Bytes[amberpack.RecHeaderSize] ^= 1 // flip a payload byte

	if _, err := s.WriteRecords(yieldAll(recs), WriteOpts{}, nil); err == nil {
		t.Fatal("corrupt record accepted")
	}
	if has, _ := s.Has(recs[0].Key); has {
		t.Fatal("corrupt record stored")
	}
}

func TestWriteRecordsRejectsWrongKey(t *testing.T) {
	s, err := Open(t.TempDir(), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := blobObj(t, incompressible(4<<10))
	b := blobObj(t, incompressible(8<<10))
	recs := recordSeq(t, []Object{a})
	recs[0].Key = b.Key // record body still holds a

	if _, err := s.WriteRecords(yieldAll(recs), WriteOpts{}, nil); !errors.Is(err, ErrVerify) {
		t.Fatalf("want ErrVerify, got %v", err)
	}
}

func slicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestViewRecordSpans(t *testing.T) {
	s, err := Open(t.TempDir(), WithSegmentSize(32<<10), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var keys []key.Key
	for i := range 20 {
		data := incompressible(4 << 10)
		data[0] = byte(i)
		o := blobObj(t, data)
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, o.Key)
	}

	var spans int
	var got []byte
	err = s.ViewRecordSpans(keys, 1<<20, func(b []byte) error {
		spans++
		got = append(got, b...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if spans >= len(keys)/2 {
		t.Fatalf("expected coalesced spans, got %d for %d keys", spans, len(keys))
	}
	seen := map[key.Key]bool{}
	for i := 0; i < len(got); {
		rec, err := amberpack.ParseRecord(got[i:])
		if err != nil {
			t.Fatal(err)
		}
		seen[rec.Key] = true
		i += amberpack.RecHeaderSize + int(rec.Slen)
	}
	for _, k := range keys {
		if !seen[k] {
			t.Fatalf("key %s missing from spans", k)
		}
	}

	absent := blobObj(t, []byte("absent")).Key
	err = s.ViewRecordSpans([]key.Key{absent}, 1<<20, func([]byte) error { return nil })
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// fn writes to a network peer; a stalled peer must not hold the store
// lock and block every other reader and writer.
func TestViewCallbacksDoNotHoldStoreLock(t *testing.T) {
	s, err := Open(t.TempDir(), WithSegmentSize(32<<10), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var keys []key.Key
	for i := range 10 { // enough to seal a segment, so both paths are sealed reads
		data := incompressible(4 << 10)
		data[0] = byte(i)
		o := blobObj(t, data)
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, o.Key)
	}
	views := []func(fn func([]byte) error) error{
		func(fn func([]byte) error) error { return s.ViewRecord(keys[0], fn) },
		func(fn func([]byte) error) error { return s.ViewRecordSpans(keys, 1<<20, fn) },
	}
	for i, view := range views {
		stalled, release := make(chan struct{}), make(chan struct{})
		go func() {
			view(func([]byte) error {
				close(stalled)
				<-release
				return nil
			})
		}()
		<-stalled
		done := make(chan error, 1)
		go func() {
			o := blobObj(t, []byte{byte(i), 'n', 'e', 'w'})
			if err := s.Put(o.Key, o.Data); err != nil {
				done <- err
				return
			}
			_, err := s.Get(keys[1])
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatalf("view %d: store wedged behind a stalled callback", i)
		}
		close(release)
	}
}
