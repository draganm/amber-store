# Remote Sync Redesign — Phase 2: wire pack format

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `amberpack`'s old `AMBERPK\x02` whole-stream-zstd wire format with a **record-stream pack** built on the Phase 1a record codec — `magic + records + end-marker`, each record self-describing and per-record-zstd'd — so the wire and the on-disk store share one record framing.

**Architecture:** The `amberpack` `Writer`/`Reader` public API is unchanged (`NewWriter`/`Add`/`Close`, `NewReader`/`All`, `ErrMalformed`), so the two callers — `server/objects.go` and `remoteclient/objects.go` — need **no code changes**; only the bytes on the wire change. Each object is encoded with `amberpack.EncodeRecord` (tag `0x01` + 46-byte header + payload) and the stream ends with a `0x00` marker. The `Reader` reassembles each record and validates it with `amberpack.ParseRecord`, surfacing all framing/record failures as `ErrMalformed`.

**Tech Stack:** Go, the Phase 1a record codec (`amberpack.EncodeRecord`/`ParseRecord`/`DecodePayload`/`RecHeaderSize`), `bufio`, big-endian framing.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — phase 2.

---

## Roadmap context

Phase 1a (record-codec extraction) is **done**. Phase 1b (footer/sealed-reader move) was **dropped** — the footer stays in `packstore`. This is **Phase 2**. Remaining after this: Phase 3 (parallel reachable walk), Phase 4 (negotiation redesign + `POST /v1/objects/reachable`), Phase 5 (single push/pull operation).

## Why the callers don't change

- `server/objects.go`: `postObjects` reads with `amberpack.NewReader(...).All()`; `postObjectsGet` writes with `amberpack.NewWriter(...)` + `Add`/`Close`. Its error mapping already keys on `errors.Is(err, amberpack.ErrMalformed)` → `422`, which the new `Reader` still returns.
- `remoteclient/objects.go`: `PushPack` writes with `NewWriter`/`Add`/`Close`; `FetchObjects` reads with `NewReader(tee).All()` then drains `tee` to feed the trailer hash. The new `Reader` wraps the body in `bufio` and consumes exactly to the end marker, so the tee/hasher still sees the whole body and the trailer check is unaffected.

## File structure (Phase 2)

**Rewritten:**
- `amberpack/pack.go` — new record-stream `Writer`/`Reader` + package doc; old `AMBERPK\x02` framing (`magic`, `tagObject`, the zstd-frame Writer/Reader, `maxPayloadBytes`) removed.
- `amberpack/pack_test.go` — tests rewritten for the new format.

**Unchanged (verified, not edited):** `server/objects.go`, `remoteclient/objects.go`, and their behavior. Any *test* elsewhere that asserts the literal `AMBERPK\x02` magic is fixed in this phase.

---

## Task 1: Swap the wire pack format to record-stream framing

**Files:**
- Rewrite: `amberpack/pack.go`
- Rewrite: `amberpack/pack_test.go`
- Possibly fix: any test asserting the old wire magic (found via grep in Step 4)

- [ ] **Step 1: Rewrite `amberpack/pack.go`**

Replace the ENTIRE file with:

```go
// Package amberpack defines the Amber-Store pack format. The content-addressed
// record codec (record.go) is shared by packstore's on-disk segments and the
// remote-sync wire packs defined here.
//
// A wire pack is a possibly-partial, unordered set of CAS objects (like a git
// pack) carrying no root key. Layout:
//
//	Magic    "AMBERPK\x03"   8 bytes  (plaintext)
//	Records  repeat: one EncodeRecord output each (tag 0x01 + 46-byte header + payload)
//	End      0x00
//
// Each record is the same self-describing, CRC-protected, per-record-zstd unit
// packstore writes on disk (see record.go); a wire pack is just those records
// framed by a magic and an explicit end marker, so a truncated stream is
// detected rather than read as a clean EOF. The Reader validates framing, CRC,
// and key canonicality and decodes each payload; it does NOT verify the payload
// hash — that happens in the storage path (packstore WriteParallel with Verify).
//
// Versions 1 and 2 ("AMBERPK\x01" / "AMBERPK\x02") were the older uncompressed
// and whole-stream-zstd stream formats; they are no longer produced and are
// rejected by the Reader.
package amberpack

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/draganm/amber-store/fstree"
)

// packMagic identifies the wire pack format and its version (the trailing byte).
const packMagic = "AMBERPK\x03"

// tagEnd marks the end of the record stream. A record begins with tagChunk
// (0x01, written by EncodeRecord), so the two are distinguished on the first byte.
const tagEnd byte = 0x00

// ErrMalformed wraps every error from a structurally invalid wire pack (bad or
// legacy magic, truncation, bad record framing, or a corrupt record). Callers
// distinguish it with errors.Is to map to a client error.
var ErrMalformed = errors.New("amberpack: malformed pack stream")

// maxWirePayload bounds a single record's stored payload so a hostile or corrupt
// stream cannot trigger an unbounded allocation. It is far above any real CAS
// object (the chunker MaxSize is on the order of a few hundred KiB).
const maxWirePayload = 256 << 20 // 256 MiB

// Writer serializes fstree.Objects into the wire pack format. It is not safe for
// concurrent use; a client wanting parallel uploads creates one Writer per pack.
type Writer struct {
	bw          *bufio.Writer
	wroteHeader bool
}

// NewWriter returns a Writer emitting to w. The caller owns w and must close it;
// Writer.Close only writes the end marker and flushes.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) ensureHeader() error {
	if w.wroteHeader {
		return nil
	}
	if _, err := w.bw.WriteString(packMagic); err != nil {
		return err
	}
	w.wroteHeader = true
	return nil
}

// Add appends one object record.
func (w *Writer) Add(o fstree.Object) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		return err
	}
	_, err = w.bw.Write(rec)
	return err
}

// Close writes the header (if no object was added) and the end marker, then
// flushes. It does not close the underlying writer.
func (w *Writer) Close() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagEnd); err != nil {
		return err
	}
	return w.bw.Flush()
}

// Reader decodes a wire pack stream.
type Reader struct {
	r io.Reader
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// All iterates over the objects in the stream. It yields exactly one error (and
// stops) on any structural problem; on a clean stream it yields every object and
// returns after the end marker. All must be called at most once per Reader
// because the underlying stream position is not reset between calls.
func (r *Reader) All() iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		br := bufio.NewReader(r.r)
		var magic [len(packMagic)]byte
		if _, err := io.ReadFull(br, magic[:]); err != nil {
			yield(fstree.Object{}, fmt.Errorf("%w: reading magic: %v", ErrMalformed, err))
			return
		}
		if string(magic[:]) != packMagic {
			yield(fstree.Object{}, fmt.Errorf("%w: bad magic", ErrMalformed))
			return
		}
		for {
			tag, err := br.ReadByte()
			if err != nil {
				yield(fstree.Object{}, fmt.Errorf("%w: truncated before end marker: %v", ErrMalformed, err))
				return
			}
			switch tag {
			case tagEnd:
				return
			case tagChunk:
				// Reassemble the full record — tag + remaining 45 header bytes +
				// slen payload bytes — then validate it with ParseRecord.
				var hdr [RecHeaderSize]byte
				hdr[0] = tag
				if _, err := io.ReadFull(br, hdr[1:]); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: truncated record header: %v", ErrMalformed, err))
					return
				}
				slen := binary.BigEndian.Uint32(hdr[38:42]) // stored-payload length field
				if slen > maxWirePayload {
					yield(fstree.Object{}, fmt.Errorf("%w: record payload %d exceeds limit %d", ErrMalformed, slen, maxWirePayload))
					return
				}
				full := make([]byte, RecHeaderSize+int(slen))
				copy(full, hdr[:])
				if _, err := io.ReadFull(br, full[RecHeaderSize:]); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: truncated record payload: %v", ErrMalformed, err))
					return
				}
				rec, err := ParseRecord(full)
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: %v", ErrMalformed, err))
					return
				}
				payload, err := DecodePayload(rec.Flags, rec.Ulen, full[RecHeaderSize:])
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: %v", ErrMalformed, err))
					return
				}
				if !yield(fstree.Object{Key: rec.Key, Bytes: payload}, nil) {
					return
				}
			default:
				yield(fstree.Object{}, fmt.Errorf("%w: bad record tag %#x", ErrMalformed, tag))
				return
			}
		}
	}
}
```

- [ ] **Step 2: Rewrite `amberpack/pack_test.go`**

Replace the ENTIRE file with (uses `mkObj` defined here and `incompressible` from `record_test.go`, same package):

```go
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
	hdr := bytes.Clone(rec[:RecHeaderSize])
	binary.BigEndian.PutUint32(hdr[38:42], maxWirePayload+1)
	if _, err := collect(t, NewReader(bytes.NewReader(wirePack(hdr)))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (oversized payload)", err)
	}
}
```

- [ ] **Step 3: Run amberpack tests**

Run: `go test ./amberpack/ -v`
Expected: PASS — the record-codec tests from Phase 1a still pass, and every `TestWriterReader_*`/`TestReader_*`/`TestRoundTrip_*` above passes against the new format.

- [ ] **Step 4: Find and fix any other test asserting the old wire magic**

Run: `grep -rn 'AMBERPK' --include='*.go' . | grep -v '/amberpack/'`
For any hit in a TEST that asserts the literal `AMBERPK\x02` (or `\x01`) wire bytes, update it to `AMBERPK\x03` / the new framing. Production code must NOT reference the magic (only `amberpack` does). If a non-test, non-amberpack file references `AMBERPK`, STOP and report it — that would be an unexpected coupling.
(Expected: likely no hits outside `amberpack/`, since callers use the `Writer`/`Reader` API, not raw bytes.)

- [ ] **Step 5: Run the full suite**

Run: `go build ./... && go vet ./amberpack/ && go test ./...`
Expected: every package PASSES. In particular `server` and `remoteclient` round-trip packs through the new format unchanged (`server` upload/verify, `remoteclient` push/fetch with the trailer-signed stream). If `server` or `remoteclient` tests fail, investigate: a genuine regression must be fixed in `amberpack` (do not change the callers' behavior). If you cannot resolve a failure, STOP and report BLOCKED with the exact output.

- [ ] **Step 6: Commit**

```bash
git add amberpack/pack.go amberpack/pack_test.go
git commit -m "amberpack: record-stream wire pack format (replaces AMBERPK v2)"
```
(If Step 4 fixed a test in another package, `git add` that file too and name it in the commit body.)

---

## Self-review

**Spec coverage:** Implements spec phase 2 ("Wire pack format"): the `amberpack` pack `Writer`/`Reader` over `header + records + end-marker` on the record codec; the old `AMBERPK` stream deleted; `POST /v1/objects` and `POST /v1/objects/get` now carry the new bytes (via the unchanged `server`/`remoteclient` callers). The end marker gives the spec's truncation-detection property.

**Placeholder scan:** None — full file contents and exact commands with expected output.

**Type/name consistency:** Reader/Writer keep their Phase-0 public API; `ErrMalformed` retained as the wire sentinel (server's `422` mapping unchanged). New internals: `packMagic`, `tagEnd`, `maxWirePayload`; uses Phase 1a's `EncodeRecord`/`ParseRecord`/`DecodePayload`/`RecHeaderSize`/`tagChunk`. The `slen` field is read at header offset `38:42` (matches the record layout `EncodeRecord` writes).

**Behavior/compat:** Coordinated daemon+server change — no on-wire backward compatibility is needed or kept; the Reader explicitly rejects `AMBERPK\x01`/`\x02`. The `bufio`-wrapped Reader consumes exactly to the end marker, so `remoteclient.FetchObjects`'s tee-hash + trailer-signature check still covers the whole body.
