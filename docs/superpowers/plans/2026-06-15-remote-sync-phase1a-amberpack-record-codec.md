# Remote Sync Redesign — Phase 1a: amberpack record-codec extraction

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the content-addressed record codec out of `packstore` into `amberpack` (exported), so the on-disk segment records and the future remote-sync wire packs share one framing — without changing any behavior.

**Architecture:** `amberpack` becomes the pack-format library; `packstore` builds on it. This first increment relocates only the *record codec* (encode/parse/decode + framing constants + CRC + zstd coders). The footer/index/filter/sealed-reader relocation and the wire-format swap come in later phases. The move is behavior-preserving: the existing test suites are the safety net, and they move or re-point with the code.

**Tech Stack:** Go, `github.com/klauspost/compress/zstd`, `hash/crc32` (Castagnoli), big-endian binary framing.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md)

---

## Full roadmap (this plan is Phase 1a)

Each phase produces working, tested software and is planned in its own document against the real code left by the previous phase (later phases depend on APIs earlier ones create, so planning them speculatively would be guesswork).

- **Phase 1a — record-codec extraction (THIS PLAN).** Codec → `amberpack`; `packstore` re-points; wire untouched.
- **Phase 1b — footer & sealed-reader extraction.** Move the footer/index/fuse-filter format and a new `amberpack.SealedView` (parse, `Get`/`Has`, record iteration, index bytes); `packstore.sealedSegment` wraps it; scrub policy stays in `packstore`.
- **Phase 2 — wire pack format.** Add `amberpack` pack `Writer`/`Reader` (`header + records + end-marker`, no footer) on the moved codec; delete the old `AMBERPK` stream; swap `POST /v1/objects` and `POST /v1/objects/get` bodies; update `server`/`remoteclient` + tests.
- **Phase 3 — parallel reachable walk.** Parallelize `fstree.ReachableKeys` (buffered slice, unordered); update its contract and callers.
- **Phase 4 — negotiation redesign.** Add `POST /v1/objects/reachable`; rewrite `remotesync.Push` (phased whole-set missing-check) and `remotesync.Pull` (list → fetch → local completeness gate); delete the 3-stage pipeline and the interleaved BFS.
- **Phase 5 — single operation.** Merge daemon endpoints (`/v1/remote/push`, `/v1/remote/pull`) and CLI (`push`, `pull`, refs-only); rewrite `architecture/remote.md`.

---

## File structure (Phase 1a)

**Created:**
- `amberpack/record.go` — the exported record codec (moved from `packstore/record.go`).
- `amberpack/record_test.go` — the codec unit tests (moved from `packstore/record_test.go`), with `amberpack`-local helpers.
- `packstore/helpers_test.go` — the shared test helpers `blobObj` / `incompressible` / `compressible` that currently live in `packstore/record_test.go` and are used by many other `packstore` tests.

**Modified:**
- `packstore/record.go` → renamed to `packstore/segment.go`, gutted to segment-level constants (`magicHeader`, `magicTrailer`, `tagSeal`, `tagDelete`), the footer CRC table (`castagnoli`), the `Object` type, and `ErrCorrupt` (now an alias of `amberpack.ErrCorrupt`).
- `packstore/packstore.go`, `packstore/parallel.go`, `packstore/recover.go`, `packstore/verify.go`, `packstore/footer.go` — re-point codec calls to `amberpack.*`.
- `packstore/recover_test.go`, `packstore/verify_test.go`, `packstore/footer_test.go`, `packstore/packstore_test.go` — re-point direct codec calls to `amberpack.*`.

**Deleted:**
- `packstore/record_test.go` (its codec tests move to `amberpack`; its shared helpers move to `packstore/helpers_test.go`).

---

## Task 1: Add the record codec to `amberpack` (additive — `packstore` untouched)

This task only adds files to `amberpack`. `packstore` still has its own private codec; both compile independently (different packages, no symbol collision).

**Files:**
- Create: `amberpack/record.go`
- Create: `amberpack/record_test.go`

- [ ] **Step 1: Create `amberpack/record.go`**

Exact full content (this is `packstore/record.go`'s codec, renamed for export — `encodeRecord`→`EncodeRecord`, `parseRecord`→`ParseRecord`, `decodePayload`→`DecodePayload`, `record`→`Record` with exported fields, `recHeaderSize`→`RecHeaderSize`, and `ErrCorrupt` reworded for the broader pack scope):

```go
package amberpack

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/draganm/amber-store/key"
	"github.com/klauspost/compress/zstd"
)

const (
	// RecHeaderSize is the fixed record-header length:
	// tag(1) + key(32) + flags(1) + ulen(4) + slen(4) + crc(4). Payload follows.
	RecHeaderSize = 46

	tagChunk byte = 0x01
	flagZstd byte = 0x01

	maxPayload = math.MaxUint32 // u32 length fields cap one object at 4 GiB
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	zero4      [4]byte
)

// ErrCorrupt wraps every record-level structural-corruption error (bad framing,
// bad flags, non-canonical key, CRC mismatch). Callers distinguish it with
// errors.Is.
var ErrCorrupt = errors.New("amberpack: corrupt pack data")

// Record describes a parsed record header. The payload lives at
// [RecHeaderSize : RecHeaderSize+Slen] within the record's bytes.
type Record struct {
	Key   key.Key
	Flags byte
	Ulen  uint32
	Slen  uint32
}

// Shared zstd coders; EncodeAll/DecodeAll are safe for concurrent use.
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	if zstdEnc, err = zstd.NewWriter(nil); err != nil {
		panic(err)
	}
	if zstdDec, err = zstd.NewReader(nil); err != nil {
		panic(err)
	}
}

// EncodeRecord serializes (k, data) into a complete record, compressing the
// payload with zstd when that makes it strictly smaller. k is written as given;
// canonical-form validation happens on the read side.
func EncodeRecord(k key.Key, data []byte) ([]byte, error) {
	if !payloadFits(len(data)) {
		return nil, fmt.Errorf("amberpack: object %s too large: %d bytes", k, len(data))
	}
	payload := data
	flags := byte(0)
	if comp := zstdEnc.EncodeAll(data, make([]byte, 0, len(data))); len(comp) < len(data) {
		payload = comp
		flags = flagZstd
	}
	rec := make([]byte, RecHeaderSize+len(payload))
	rec[0] = tagChunk
	copy(rec[1:33], k[:])
	rec[33] = flags
	binary.BigEndian.PutUint32(rec[34:38], uint32(len(data)))
	binary.BigEndian.PutUint32(rec[38:42], uint32(len(payload)))
	copy(rec[RecHeaderSize:], payload)
	// CRC over the whole record; the crc field itself is still zero here.
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
	return rec, nil
}

// payloadFits reports whether a payload of n bytes fits the format's u32 length
// fields. Split out of EncodeRecord so the bound is testable without allocating
// 4 GiB.
func payloadFits(n int) bool {
	return uint64(n) <= maxPayload
}

// ParseRecord validates the record at the start of b (which may extend past it)
// and returns its header. It checks framing, flags, key canonicality, and the
// CRC, without mutating b (b may be a read-only mmap).
func ParseRecord(b []byte) (Record, error) {
	if len(b) < RecHeaderSize {
		return Record{}, fmt.Errorf("%w: truncated record header", ErrCorrupt)
	}
	if b[0] != tagChunk {
		return Record{}, fmt.Errorf("%w: unexpected record tag %#x", ErrCorrupt, b[0])
	}
	flags := b[33]
	if flags&^flagZstd != 0 {
		return Record{}, fmt.Errorf("%w: unknown record flags %#x", ErrCorrupt, flags)
	}
	ulen := binary.BigEndian.Uint32(b[34:38])
	slen := binary.BigEndian.Uint32(b[38:42])
	if int64(len(b)) < RecHeaderSize+int64(slen) {
		return Record{}, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	if flags&flagZstd == 0 && ulen != slen {
		return Record{}, fmt.Errorf("%w: raw record with ulen %d != slen %d", ErrCorrupt, ulen, slen)
	}
	if flags&flagZstd != 0 && slen >= ulen {
		return Record{}, fmt.Errorf("%w: compressed record with slen %d >= ulen %d", ErrCorrupt, slen, ulen)
	}
	c := crc32.Update(0, castagnoli, b[:42])
	c = crc32.Update(c, castagnoli, zero4[:])
	c = crc32.Update(c, castagnoli, b[RecHeaderSize:RecHeaderSize+int(slen)])
	if c != binary.BigEndian.Uint32(b[42:46]) {
		return Record{}, fmt.Errorf("%w: record CRC mismatch", ErrCorrupt)
	}
	k, err := key.Parse(b[1:33])
	if err != nil {
		return Record{}, fmt.Errorf("%w: record key: %v", ErrCorrupt, err)
	}
	return Record{Key: k, Flags: flags, Ulen: ulen, Slen: slen}, nil
}

// DecodePayload returns caller-owned payload bytes from a record's stored
// payload. stored may be a read-only mmap slice and is never retained.
func DecodePayload(flags byte, ulen uint32, stored []byte) ([]byte, error) {
	if flags&flagZstd == 0 {
		out := make([]byte, len(stored))
		copy(out, stored)
		return out, nil
	}
	out, err := zstdDec.DecodeAll(stored, make([]byte, 0, ulen))
	if err != nil {
		return nil, fmt.Errorf("%w: zstd: %v", ErrCorrupt, err)
	}
	if uint32(len(out)) != ulen {
		return nil, fmt.Errorf("%w: decompressed to %d bytes, header says %d", ErrCorrupt, len(out), ulen)
	}
	return out, nil
}
```

- [ ] **Step 2: Create `amberpack/record_test.go`**

These are `packstore/record_test.go`'s codec tests, in `package amberpack`, re-pointed to the exported names. Canonical blobs come from `mkObj` (already defined in `amberpack/pack_test.go`, same package), which returns `fstree.Object{Key, Bytes}`. The shared `incompressible`/`compressible`/`r0ulen`/`fixCRC` helpers are local to this file.

```go
package amberpack

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/draganm/amber-store/key"
)

// incompressible returns n deterministic pseudo-random bytes (zstd cannot shrink them).
func incompressible(n int) []byte {
	r := rand.New(rand.NewPCG(42, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// compressible returns n highly repetitive bytes (zstd shrinks them a lot).
func compressible(n int) []byte {
	return bytes.Repeat([]byte("abcdefgh"), n/8+1)[:n]
}

// r0ulen reads the ulen field of a record.
func r0ulen(rec []byte) uint32 { return binary.BigEndian.Uint32(rec[34:38]) }

// fixCRC recomputes a record's CRC after test tampering.
func fixCRC(rec []byte) {
	binary.BigEndian.PutUint32(rec[42:46], 0)
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
}

func TestRecordRoundTripRaw(t *testing.T) {
	o := mkObj(t, incompressible(4096))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Key != o.Key {
		t.Fatalf("key mismatch: %s != %s", r.Key, o.Key)
	}
	if r.Flags != 0 {
		t.Fatalf("random data must be stored raw, got flags %#x", r.Flags)
	}
	if r.Ulen != r.Slen || int(r.Slen) != len(o.Bytes) {
		t.Fatalf("raw lens: ulen=%d slen=%d want %d", r.Ulen, r.Slen, len(o.Bytes))
	}
	got, err := DecodePayload(r.Flags, r.Ulen, rec[RecHeaderSize:RecHeaderSize+int(r.Slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, o.Bytes) {
		t.Fatal("payload mismatch")
	}
}

func TestRecordRoundTripCompressed(t *testing.T) {
	o := mkObj(t, compressible(64<<10))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Flags != flagZstd {
		t.Fatalf("repetitive data must compress, got flags %#x", r.Flags)
	}
	if r.Slen >= r.Ulen {
		t.Fatalf("compressed slen=%d must be < ulen=%d", r.Slen, r.Ulen)
	}
	got, err := DecodePayload(r.Flags, r.Ulen, rec[RecHeaderSize:RecHeaderSize+int(r.Slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, o.Bytes) {
		t.Fatal("payload mismatch after decompression")
	}
}

func TestRecordEmptyPayload(t *testing.T) {
	o := mkObj(t, nil)
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.Ulen != 0 || r.Slen != 0 || r.Flags != 0 {
		t.Fatalf("empty payload: ulen=%d slen=%d flags=%#x", r.Ulen, r.Slen, r.Flags)
	}
}

func TestRecordTooLarge(t *testing.T) {
	if !payloadFits(math.MaxUint32) {
		t.Fatal("payloadFits must accept exactly 4 GiB - 1")
	}
	if payloadFits(math.MaxUint32 + 1) {
		t.Fatal("payloadFits must reject > 4 GiB - 1")
	}
	if !payloadFits(0) {
		t.Fatal("payloadFits must accept empty payloads")
	}
}

func TestParseRecordRejectsCorruption(t *testing.T) {
	o := mkObj(t, incompressible(1024))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated header", func(t *testing.T) {
		if _, err := ParseRecord(rec[:RecHeaderSize-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("truncated payload", func(t *testing.T) {
		if _, err := ParseRecord(rec[:len(rec)-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad tag", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[0] = 0x7F
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad flags", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[33] = 0x80
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped payload byte fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[len(bad)-1] ^= 0x01
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped length fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[39] ^= 0x01 // inside slen, keeps record long enough to parse
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("non-canonical key", func(t *testing.T) {
		var k key.Key
		copy(k[:], o.Key[:])
		k[0] = 0xF0 // type 15: reserved
		bad, err := EncodeRecord(k, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("raw ulen != slen", func(t *testing.T) {
		bad := bytes.Clone(rec)
		binary.BigEndian.PutUint32(bad[34:38], r0ulen(bad)+1)
		fixCRC(bad)
		if _, err := ParseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestParseRecordIgnoresTrailingBytes(t *testing.T) {
	o := mkObj(t, incompressible(512))
	rec, err := EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	extended := append(bytes.Clone(rec), incompressible(1000)...)
	r, err := ParseRecord(extended)
	if err != nil {
		t.Fatal(err)
	}
	if r.Key != o.Key || int(r.Slen) != len(o.Bytes) {
		t.Fatalf("parse over extended buffer: %+v", r)
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	t.Run("bad zstd frame", func(t *testing.T) {
		if _, err := DecodePayload(flagZstd, 100, []byte("not a zstd frame")); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("ulen mismatch", func(t *testing.T) {
		comp := zstdEnc.EncodeAll([]byte("hello world"), nil)
		if _, err := DecodePayload(flagZstd, 5, comp); err == nil {
			t.Fatal("want error for wrong ulen")
		}
	})
}

func TestDecodePayloadRawDoesNotAlias(t *testing.T) {
	stored := []byte{1, 2, 3, 4}
	out, err := DecodePayload(0, 4, stored)
	if err != nil {
		t.Fatal(err)
	}
	out[0] = 99
	if stored[0] != 1 {
		t.Fatal("DecodePayload raw path must copy, not alias")
	}
}
```

- [ ] **Step 3: Run the amberpack tests**

Run: `go test ./amberpack/ -v`
Expected: PASS, including the new `TestRecord*`, `TestParseRecord*`, `TestDecodePayload*` plus the pre-existing `TestWriterReader_*` / `TestReader_*` (the old AMBERPK stream tests still pass — untouched this phase).

- [ ] **Step 4: Confirm `packstore` still builds and passes (unchanged this task)**

Run: `go build ./... && go test ./packstore/`
Expected: PASS (packstore still uses its own private codec; nothing re-pointed yet).

- [ ] **Step 5: Commit**

```bash
git add amberpack/record.go amberpack/record_test.go
git commit -m "amberpack: add exported record codec (moved from packstore)"
```

---

## Task 2: Re-point `packstore` onto the `amberpack` codec and delete its copy

One atomic flip: the intermediate steps will not compile until the re-points are all in place; the task ends green with a single commit.

**Files:**
- Create: `packstore/helpers_test.go`
- Delete: `packstore/record_test.go`
- Rename + edit: `packstore/record.go` → `packstore/segment.go`
- Modify: `packstore/packstore.go`, `packstore/parallel.go`, `packstore/recover.go`, `packstore/verify.go`, `packstore/footer.go`
- Modify (tests): `packstore/recover_test.go`, `packstore/verify_test.go`, `packstore/footer_test.go`, `packstore/packstore_test.go`

- [ ] **Step 1: Preserve the shared test helpers**

`blobObj`, `incompressible`, and `compressible` live in `packstore/record_test.go` but are used across `oracle_test.go`, `verify_test.go`, `footer_test.go`, and `packstore_test.go`. Create `packstore/helpers_test.go` with exactly these three (lifted verbatim from `packstore/record_test.go`):

```go
package packstore

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/draganm/amber-store/key"
)

// blobObj builds a canonical Blob object for data.
func blobObj(t *testing.T, data []byte) Object {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	return Object{Key: k, Data: data}
}

// incompressible returns n deterministic pseudo-random bytes (zstd cannot shrink them).
func incompressible(n int) []byte {
	r := rand.New(rand.NewPCG(42, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// compressible returns n highly repetitive bytes (zstd shrinks them a lot).
func compressible(n int) []byte {
	return bytes.Repeat([]byte("abcdefgh"), n/8+1)[:n]
}
```

- [ ] **Step 2: Delete the old codec test file**

```bash
git rm packstore/record_test.go
```
(Its codec tests now live in `amberpack/record_test.go`; its helpers in `packstore/helpers_test.go`.)

- [ ] **Step 3: Rename `record.go` → `segment.go` and gut it to segment-level definitions**

```bash
git mv packstore/record.go packstore/segment.go
```

Replace the entire contents of `packstore/segment.go` with (keeps the package doc, the segment header/trailer magics, the seal/delete tags, the footer CRC table, the `Object` type, and `ErrCorrupt` as an alias of the amberpack sentinel so every existing `errors.Is(err, ErrCorrupt)` and `%w` wrap keeps working):

```go
// Package packstore persists Amber-Store CAS objects in log-structured,
// append-only segment (pack) files. The store directory contains only segment
// files: sealed segments are immutable, mmap'd whole, and self-indexed by a
// footer (fanout index on the last key byte + binary fuse filter + fixed
// trailer); the single active segment is recovered by a tail-scan. There is no
// global index. All format integers are big-endian. Record framing lives in the
// amberpack package. See docs/superpowers/specs/2026-06-13-packstore-design.md.
package packstore

import (
	"hash/crc32"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
)

const (
	tagSeal   byte = 0xF0 // first byte of the footer
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
)

var (
	magicHeader  = []byte("AMBERSG\x01")
	magicTrailer = []byte("AMBERSGF")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli) // footer CRC; record CRC lives in amberpack
)

// ErrCorrupt wraps every structural-corruption error (bad record framing, bad
// footer, scrub findings). It aliases amberpack's record-corruption sentinel so
// a single errors.Is target covers both record- and footer-level corruption.
var ErrCorrupt = amberpack.ErrCorrupt

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}
```

Note: `tagChunk` is gone from `packstore`; the active-segment append path (Step 4) keeps reading the record header it just built from `amberpack.EncodeRecord` at fixed offsets, which is fine — those offsets mirror the amberpack record layout.

- [ ] **Step 4: Re-point `packstore/packstore.go`**

Add `"github.com/draganm/amber-store/amberpack"` to its import block. Then:

- Line ~433 (`WriteBatch`): `rec, err := encodeRecord(obj.Key, obj.Data)` → `rec, err := amberpack.EncodeRecord(obj.Key, obj.Data)`
- Line ~462 (`Put`): `rec, err := encodeRecord(k, data)` → `rec, err := amberpack.EncodeRecord(k, data)`
- Line ~480 (`Get`, active read): `s.active.f.ReadAt(stored, loc.off+recHeaderSize)` → `... loc.off+amberpack.RecHeaderSize)`
- Line ~483 (`Get`, active read): `return decodePayload(loc.flags, loc.ulen, stored)` → `return amberpack.DecodePayload(loc.flags, loc.ulen, stored)`

Leave the `append()` body (the `rec[33]`, `rec[34:38]`, `rec[38:42]` reads that fill `activeLoc`) unchanged.

- [ ] **Step 5: Re-point `packstore/parallel.go`**

Add the `amberpack` import. Then line ~144 (`runWriter`): `rec, err := encodeRecord(obj.Key, obj.Data)` → `rec, err := amberpack.EncodeRecord(obj.Key, obj.Data)`.

- [ ] **Step 6: Re-point `packstore/recover.go`**

Add the `amberpack` import. Then in `scanActive`:

- `rec, err := parseRecord(b[off:])` → `rec, err := amberpack.ParseRecord(b[off:])`
- `res.index[rec.key] = activeLoc{off: off, flags: rec.flags, ulen: rec.ulen, slen: rec.slen}` → `res.index[rec.Key] = activeLoc{off: off, flags: rec.Flags, ulen: rec.Ulen, slen: rec.Slen}`
- `off += recHeaderSize + int64(rec.slen)` → `off += amberpack.RecHeaderSize + int64(rec.Slen)`

(`magicHeader` and `tagSeal` remain `packstore` identifiers — unchanged.)

- [ ] **Step 7: Re-point `packstore/verify.go`**

Add the `amberpack` import. Then in `(*sealedSegment).verify`:

- `rec, err := parseRecord(g.mm[off:g.fv.bodyLen])` → `rec, err := amberpack.ParseRecord(g.mm[off:g.fv.bodyLen])`
- `payload, err := decodePayload(rec.flags, rec.ulen, g.mm[off+recHeaderSize:off+recHeaderSize+int64(rec.slen)])` → `payload, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, g.mm[off+amberpack.RecHeaderSize:off+amberpack.RecHeaderSize+int64(rec.Slen)])`
- `verifyObject(Object{Key: rec.key, Data: payload})` → `verifyObject(Object{Key: rec.Key, Data: payload})`
- `g.fv.filter.Contains(filterKey(rec.key))` → `g.fv.filter.Contains(filterKey(rec.Key))`
- `... filter missing key %s (offset %d)", ErrCorrupt, g.path, rec.key, off)` → `... rec.Key, off)`
- `entries = append(entries, indexEntry{k: rec.key, off: uint64(off), slen: rec.slen})` → `indexEntry{k: rec.Key, off: uint64(off), slen: rec.Slen}`
- `off += recHeaderSize + int64(rec.slen)` → `off += amberpack.RecHeaderSize + int64(rec.Slen)`

(`verifyObject` and `ErrCorrupt` stay in `packstore`.)

- [ ] **Step 8: Re-point `packstore/footer.go`**

Add the `amberpack` import. In `(*footerView).get` (around lines 368–375):

- `uint64(recHeaderSize)+uint64(slen) > bodyLen-off` → `uint64(amberpack.RecHeaderSize)+uint64(slen) > bodyLen-off`
- `end := off + recHeaderSize + uint64(slen)` → `end := off + amberpack.RecHeaderSize + uint64(slen)`
- `h := g.mm[off : off+recHeaderSize]` → `h := g.mm[off : off+amberpack.RecHeaderSize]`
- `data, err := decodePayload(flags, ulen, g.mm[off+recHeaderSize:end])` → `data, err := amberpack.DecodePayload(flags, ulen, g.mm[off+amberpack.RecHeaderSize:end])`

(`castagnoli` references in `buildFooter`/`parseFooter` stay — that's the footer CRC table kept in `segment.go`.)

- [ ] **Step 9: Re-point the `packstore` tests that call the codec directly**

Add `"github.com/draganm/amber-store/amberpack"` imports where needed and change the calls:

- `packstore/recover_test.go` — `buildBody`: `rec, err := encodeRecord(o.Key, o.Data)` → `rec, err := amberpack.EncodeRecord(o.Key, o.Data)`.
- `packstore/verify_test.go` — replace every `encodeRecord(` with `amberpack.EncodeRecord(`, `parseRecord(` with `amberpack.ParseRecord(`, `decodePayload(` with `amberpack.DecodePayload(`, and any `recHeaderSize` with `amberpack.RecHeaderSize`. If a parsed record's fields are read, use the exported `.Key/.Flags/.Ulen/.Slen`.
- `packstore/footer_test.go` — same mechanical replacement in `writeSealedFile` and any other codec call.
- `packstore/packstore_test.go` — same mechanical replacement.

(Use `grep -n 'encodeRecord\|parseRecord\|decodePayload\|recHeaderSize' packstore/*_test.go` to find every site; `blobObj`/`incompressible`/`compressible` are unchanged — they resolve to `helpers_test.go`.)

- [ ] **Step 10: Build and vet**

Run: `go build ./... && go vet ./packstore/ ./amberpack/`
Expected: no errors. If `go vet` reports an unused `binary` import in any edited `packstore` file, remove it; if it reports a missing `amberpack` import, add it.

- [ ] **Step 11: Run the full test suite**

Run: `go test ./...`
Expected: PASS across all packages — `packstore` behavior is unchanged (same on-disk bytes: the record codec is byte-for-byte identical, only its package moved), and downstream packages (`server`, `daemon`, `remotesync`, `cmd/...`) are unaffected.

- [ ] **Step 12: Commit**

```bash
git add -A packstore/
git commit -m "packstore: build on amberpack record codec, drop private copy"
```

---

## Self-review

**Spec coverage (Phase 1a slice):** The spec's `amberpack` decision lists "record codec (`encodeRecord`/`parseRecord`/`decodePayload`)" as moving to `amberpack`. Task 1 creates it there (exported); Task 2 makes `packstore` consume it and deletes the duplicate. The footer/filter/sealed-reader items from the same spec section are explicitly deferred to Phase 1b (see roadmap) — they are store-internal and the wire never uses them, so this slice stands alone.

**Placeholder scan:** None — every step gives exact file content or exact old→new edits and a runnable command with expected output.

**Type/name consistency:** Exported surface used consistently everywhere — `amberpack.EncodeRecord`, `amberpack.ParseRecord`, `amberpack.DecodePayload`, `amberpack.Record{Key,Flags,Ulen,Slen}`, `amberpack.RecHeaderSize`, `amberpack.ErrCorrupt`. `packstore.ErrCorrupt` is retained as an alias so no `errors.Is` target changes. `tagSeal`/`tagDelete`/`magicHeader`/`magicTrailer`/`castagnoli`/`Object` remain `packstore` identifiers in `segment.go`.

**Behavior preservation:** The codec bytes are unchanged (same constants, CRC, zstd, offsets), so existing on-disk segments and the existing `AMBERPK` wire format both keep working; the move is purely organizational. The safety net is the unchanged `packstore` suite plus the relocated codec tests.
