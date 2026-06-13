package amberpack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/klauspost/compress/zstd"
)

// pack wraps crafted record bytes into a structurally valid container: the
// plaintext magic followed by a single zstd frame holding body. It lets the
// negative tests below exercise the real framing branches (bad key, bad tag,
// truncated payload) rather than tripping the corrupt-frame path.
func pack(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(magic)
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// mkObj builds a canonical Blob object from data (Blob length == byte length).
func mkObj(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	return o
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

func TestRoundTrip_CompressedV2(t *testing.T) {
	// A large, highly compressible payload so the encoded frame is clearly smaller
	// than the raw bytes — proof that compression is actually applied — and a small
	// object alongside it to exercise the framing inside the compressed stream.
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
	if want := "AMBERPK\x02"; len(out) < len(want) || string(out[:len(want)]) != want {
		t.Fatalf("magic = %q, want %q", out[:min(len(out), len(want))], want)
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

func TestReader_RejectsLegacyV1(t *testing.T) {
	// A structurally valid OLD (uncompressed) stream: "AMBERPK\x01" + raw record +
	// end marker. The compressed reader must reject the legacy version outright.
	var buf bytes.Buffer
	buf.WriteString("AMBERPK\x01")
	buf.WriteByte(tagObject)
	o := mkObj(t, []byte("legacy"))
	buf.Write(o.Key[:])
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(o.Bytes)))
	buf.Write(lb[:n])
	buf.Write(o.Bytes)
	buf.WriteByte(tagEnd)

	_, err := collect(t, NewReader(&buf))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (legacy v1 rejected)", err)
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
	// A valid zstd frame holding one complete record but no end marker: the loop
	// reads the record, then hits EOF where the end marker should be.
	o := mkObj(t, []byte("data"))
	var body bytes.Buffer
	body.WriteByte(tagObject)
	body.Write(o.Key[:])
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(o.Bytes)))
	body.Write(lb[:n])
	body.Write(o.Bytes)
	// no tagEnd

	_, err := collect(t, NewReader(bytes.NewReader(pack(t, body.Bytes()))))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (missing end marker)", err)
	}
}

func TestReader_NonCanonicalKeyRejected(t *testing.T) {
	// A record whose key has the reserved bit set, so key.Parse fails. (type 0,
	// length-size 1, length 0 would otherwise be canonical.)
	var body bytes.Buffer
	body.WriteByte(tagObject)
	var k [key.Size]byte
	k[0] = 0x08 // reserved bit set -> key.Parse fails
	body.Write(k[:])
	body.WriteByte(0) // uvarint payload length 0

	_, err := collect(t, NewReader(bytes.NewReader(pack(t, body.Bytes()))))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (bad key)", err)
	}
}

func TestReader_BadRecordTag(t *testing.T) {
	_, err := collect(t, NewReader(bytes.NewReader(pack(t, []byte{0x42})))) // unknown record tag
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (bad record tag)", err)
	}
}

func TestReader_TruncatedPayload(t *testing.T) {
	// Claim a 100-byte payload but provide only 5 bytes.
	o := mkObj(t, []byte("hello"))
	var body bytes.Buffer
	body.WriteByte(tagObject)
	body.Write(o.Key[:])
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], 100)
	body.Write(lb[:n])
	body.WriteString("short") // only 5 bytes, not 100

	_, err := collect(t, NewReader(bytes.NewReader(pack(t, body.Bytes()))))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (truncated payload)", err)
	}
}

func TestReader_CorruptZstdBody(t *testing.T) {
	// Valid magic but the body is not a zstd frame at all.
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteString("this is not a valid zstd frame")
	_, err := collect(t, NewReader(&buf))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (corrupt zstd body)", err)
	}
}
