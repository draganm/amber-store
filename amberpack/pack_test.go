package amberpack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// mkObj builds a canonical Blob object from data (Blob length == byte length).
func mkObj(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// wirePack prepends the pack magic to raw body bytes. Negative tests pass
// crafted bodies (records and/or markers) to exercise the framing branches.
func wirePack(body []byte) []byte {
	return append([]byte(packMagic), body...)
}

func collect(t *testing.T, r *Reader) ([]fstree.Object, error) {
	t.Helper()
	var out []fstree.Object
	for o, err := range r.All() {
		if err != nil {
			return out, err
		}
		out = append(out, o)
	}
	return out, nil
}

func TestWriterReader_RoundTrip(t *testing.T) {
	objs := []fstree.Object{
		mkObj(t, []byte("alpha")),
		mkObj(t, []byte("")),
		mkObj(t, bytes.Repeat([]byte("x"), 5000)),
	}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := collect(t, NewReader(&buf))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(objs) {
		t.Fatalf("read %d objects, want %d", len(got), len(objs))
	}
	for i, o := range objs {
		if got[i].Key != o.Key || !bytes.Equal(got[i].Bytes, o.Bytes) {
			t.Errorf("object %d mismatch", i)
		}
	}
}

func TestRoundTrip_Compressed(t *testing.T) {
	// A large, highly compressible payload: its per-record zstd makes the pack
	// clearly smaller than the raw bytes, proving compression is applied.
	big := bytes.Repeat([]byte("amber"), 50_000) // 250 KB, very compressible
	objs := []fstree.Object{
		mkObj(t, []byte("alpha")),
		mkObj(t, big),
	}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	out := buf.Bytes()
	if len(out) < len(packMagic) || string(out[:len(packMagic)]) != packMagic {
		t.Fatalf("magic = %q, want %q", out[:min(len(out), len(packMagic))], packMagic)
	}
	if len(out) >= len(big) {
		t.Fatalf("output %d bytes not smaller than raw payload %d; compression not applied", len(out), len(big))
	}
	got, err := collect(t, NewReader(&buf))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(objs) {
		t.Fatalf("read %d objects, want %d", len(got), len(objs))
	}
	for i, o := range objs {
		if got[i].Key != o.Key || !bytes.Equal(got[i].Bytes, o.Bytes) {
			t.Errorf("object %d mismatch", i)
		}
	}
}

func TestReader_RejectsLegacyVersions(t *testing.T) {
	for _, magic := range []string{"AMBERPK\x01", "AMBERPK\x02"} {
		var buf bytes.Buffer
		buf.WriteString(magic)
		buf.WriteByte(tagEnd)
		if _, err := collect(t, NewReader(&buf)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("magic %q: err = %v, want ErrMalformed", magic, err)
		}
	}
}

func TestWriterReader_EmptyStreamIsValid(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := collect(t, NewReader(&buf))
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("read %d objects from empty stream, want 0", len(got))
	}
}

func TestReader_BadMagic(t *testing.T) {
	_, err := collect(t, NewReader(bytes.NewReader([]byte("NOTAMBER..."))))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
}

func TestReader_TruncatedMissingEndMarker(t *testing.T) {
	// One complete record but no end marker: the loop reads the record, then
	// hits EOF where the end marker (or next tag) should be.
	o := mkObj(t, []byte("data"))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(rec)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (missing end marker)", err)
	}
}

func TestReader_NonCanonicalKeyRejected(t *testing.T) {
	// A record whose key has the reserved type nibble set, so key.Parse fails in
	// ParseRecord. EncodeRecord writes the key as given without validating it.
	o := mkObj(t, []byte("payload"))
	var k key.Key
	copy(k[:], o.Key[:])
	k[0] = 0xF0 // reserved type nibble -> key.Parse fails
	rec, err := EncodeRecord(k, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	body := append(rec, tagEnd)
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(body)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (bad key)", err)
	}
}

func TestReader_BadRecordTag(t *testing.T) {
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack([]byte{0x42})))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (bad record tag)", err)
	}
}

func TestReader_TruncatedPayload(t *testing.T) {
	// A record header claims a 100-byte payload but the stream ends after 5.
	o := mkObj(t, incompressible(100)) // incompressible -> stored raw, slen = 100
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	truncated := rec[:RecHeaderSize+5] // header + only 5 of 100 payload bytes
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(truncated)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (truncated payload)", err)
	}
}

func TestReader_RecordCRCMismatch(t *testing.T) {
	// A flipped payload byte fails the record CRC inside ParseRecord.
	o := mkObj(t, incompressible(64))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	rec[len(rec)-1] ^= 0x01
	body := append(rec, tagEnd)
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(body)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (record CRC mismatch)", err)
	}
}

func TestReader_OversizedPayloadRejected(t *testing.T) {
	// A header claiming a payload above maxWirePayload is rejected before any
	// allocation (and before the CRC check).
	o := mkObj(t, []byte("x"))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	// Only the header follows the magic — no payload bytes — so the size guard
	// must fire before the payload ReadFull, not after.
	hdr := bytes.Clone(rec[:RecHeaderSize])
	binary.BigEndian.PutUint32(hdr[38:42], maxWirePayload+1)
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(hdr)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (oversized payload)", err)
	}
}
