# Packstore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `packstore` — a log-structured segment store parallel to `diskstore`, per the approved spec `docs/superpowers/specs/2026-06-13-packstore-design.md`.

**Architecture:** Objects append as self-framing CRC'd records into one active segment file; at 256 MiB the segment seals with a footer (fanout index on the last key byte + binary fuse filter + fixed 64-byte trailer) and becomes an immutable mmap'd file. There is no global index: RAM holds per-segment fuse filters, lookups binary-search mmap'd footer indexes, and crash recovery tail-scans the single active segment. All format integers are big-endian.

**Tech Stack:** Go, `github.com/FastFilter/xorfilter` (binary fuse filters), `github.com/klauspost/compress/zstd` (per-record compression), `golang.org/x/sys/unix` (mmap, flock), `github.com/zeebo/blake3` (verify), stdlib `hash/crc32` (Castagnoli).

**Read the spec first.** Every byte-layout decision (record header, footer sections, trailer fields, endianness) is normative there. This plan repeats the layouts where code needs them, but the spec wins on conflict.

---

## File structure

```
packstore/
  record.go         record encode/parse, zstd store-if-smaller, decodePayload, shared zstd/crc state
  footer.go         index section build/search, filter section build/parse, footer assemble/parse,
                    sealedSegment (mmap open/lookup/get/close)
  recover.go        active-segment tail-scan (scanActive)
  packstore.go      Store: Open/Put/Get/Has/WriteBatch/Close, options, flock, active segment,
                    append/seal/rotation
  missing.go        Missing (mirrors diskstore/missing.go)
  parallel.go       WriteParallel + seenSet (mirrors diskstore/parallel.go)
  verify.go         write-time verifyObject + Verify(ctx) scrub
  record_test.go, footer_test.go, recover_test.go, packstore_test.go,
  missing_test.go, parallel_test.go, verify_test.go
```

Conventions used throughout (defined once in Task 1, reused everywhere):

```go
// record.go
const (
	recHeaderSize = 46

	tagChunk  byte = 0x01
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
	tagSeal   byte = 0xF0 // first byte of the footer

	flagZstd byte = 0x01
)

var (
	magicHeader  = []byte("AMBERSG\x01") // first 8 bytes of every segment
	magicTrailer = []byte("AMBERSGF")    // last 8 bytes of every sealed segment
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)
```

Record layout (offsets within a record; all integers big-endian):

```
[0]      tag      (tagChunk)
[1:33]   key      (32 bytes, opaque key.Key)
[33]     flags    (flagZstd or 0)
[34:38]  ulen     u32 — uncompressed payload length
[38:42]  slen     u32 — stored payload length (== ulen when raw)
[42:46]  crc      u32 — CRC-32C of the whole record with this field zeroed
[46:]    payload  (slen bytes)
```

Run all tests with `go test ./packstore/ -count=1`. The repo uses a nix flake +
direnv; the `go` toolchain (1.26) is already on PATH inside the repo.

---

### Task 1: Record format (encode, parse, payload decode)

**Files:**
- Modify: `go.mod`, `go.sum` (new deps)
- Create: `packstore/record.go`
- Test: `packstore/record_test.go`

- [ ] **Step 1: Add dependencies**

```bash
cd /Users/dragan/draganm/amber-store
go get github.com/klauspost/compress@v1.17.11
```

Expected: klauspost/compress (already an indirect dependency via Pebble) moves to the direct require block once imported; `go mod tidy` in Step 6 settles it. (`github.com/FastFilter/xorfilter` is fetched in Task 3, where it is first imported — fetching it earlier would just get dropped by tidy.)

- [ ] **Step 2: Write the failing tests**

Create `packstore/record_test.go`:

```go
package packstore

import (
	"bytes"
	"encoding/binary"
	"math"
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

func TestRecordRoundTripRaw(t *testing.T) {
	obj := blobObj(t, incompressible(4096))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.key != obj.Key {
		t.Fatalf("key mismatch: %s != %s", r.key, obj.Key)
	}
	if r.flags != 0 {
		t.Fatalf("random data must be stored raw, got flags %#x", r.flags)
	}
	if r.ulen != r.slen || int(r.slen) != len(obj.Data) {
		t.Fatalf("raw lens: ulen=%d slen=%d want %d", r.ulen, r.slen, len(obj.Data))
	}
	got, err := decodePayload(r.flags, r.ulen, rec[recHeaderSize:recHeaderSize+int(r.slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, obj.Data) {
		t.Fatal("payload mismatch")
	}
}

func TestRecordRoundTripCompressed(t *testing.T) {
	obj := blobObj(t, compressible(64<<10))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.flags != flagZstd {
		t.Fatalf("repetitive data must compress, got flags %#x", r.flags)
	}
	if r.slen >= r.ulen {
		t.Fatalf("compressed slen=%d must be < ulen=%d", r.slen, r.ulen)
	}
	got, err := decodePayload(r.flags, r.ulen, rec[recHeaderSize:recHeaderSize+int(r.slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, obj.Data) {
		t.Fatal("payload mismatch after decompression")
	}
}

func TestRecordEmptyPayload(t *testing.T) {
	obj := blobObj(t, nil)
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.ulen != 0 || r.slen != 0 || r.flags != 0 {
		t.Fatalf("empty payload: ulen=%d slen=%d flags=%#x", r.ulen, r.slen, r.flags)
	}
}

func TestRecordTooLarge(t *testing.T) {
	// Do not allocate 4 GiB: only the length check matters, so trip it via a
	// fake huge slice header is not possible safely — instead check the guard
	// constant is enforced by calling with a length-limited wrapper.
	if maxPayload != math.MaxUint32 {
		t.Fatalf("maxPayload = %d, want %d", maxPayload, uint64(math.MaxUint32))
	}
}

func TestParseRecordRejectsCorruption(t *testing.T) {
	obj := blobObj(t, incompressible(1024))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated header", func(t *testing.T) {
		if _, err := parseRecord(rec[:recHeaderSize-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("truncated payload", func(t *testing.T) {
		if _, err := parseRecord(rec[:len(rec)-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad tag", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[0] = 0x7F
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad flags", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[33] = 0x80
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped payload byte fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[len(bad)-1] ^= 0x01
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped length fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[39] ^= 0x01 // inside slen, keeps record long enough to parse
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("non-canonical key", func(t *testing.T) {
		// Encode with an invalid type nibble; encodeRecord does not validate
		// keys (callers supply canonical keys), parseRecord must.
		var k key.Key
		copy(k[:], obj.Key[:])
		k[0] = 0xF0 // type 15: reserved
		bad, err := encodeRecord(k, obj.Data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("raw ulen != slen", func(t *testing.T) {
		bad := bytes.Clone(rec)
		binary.BigEndian.PutUint32(bad[34:38], r0ulen(bad)+1)
		fixCRC(bad)
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
}

// r0ulen reads the ulen field of a record.
func r0ulen(rec []byte) uint32 { return binary.BigEndian.Uint32(rec[34:38]) }

// fixCRC recomputes a record's CRC after test tampering.
func fixCRC(rec []byte) {
	binary.BigEndian.PutUint32(rec[42:46], 0)
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
}
```

Add `"hash/crc32"` to the test imports.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `encodeRecord`, `parseRecord`, `decodePayload`, `Object`, constants undefined.

- [ ] **Step 4: Implement `packstore/record.go`**

```go
// Package packstore persists Amber-Store CAS objects in log-structured,
// append-only segment (pack) files. The store directory contains only segment
// files: sealed segments are immutable, mmap'd whole, and self-indexed by a
// footer (fanout index on the last key byte + binary fuse filter + fixed
// trailer); the single active segment is recovered by a tail-scan. There is no
// global index. All format integers are big-endian. See
// docs/superpowers/specs/2026-06-13-packstore-design.md.
package packstore

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
	recHeaderSize = 46

	tagChunk  byte = 0x01
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
	tagSeal   byte = 0xF0 // first byte of the footer

	flagZstd byte = 0x01

	maxPayload = math.MaxUint32 // u32 length fields cap one object at 4 GiB
)

var (
	magicHeader  = []byte("AMBERSG\x01")
	magicTrailer = []byte("AMBERSGF")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)

// ErrCorrupt wraps every structural-corruption error (bad record framing, bad
// footer, scrub findings). Callers distinguish it with errors.Is.
var ErrCorrupt = errors.New("packstore: corrupt segment")

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
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

// record describes a parsed record header. The payload lives at
// [recHeaderSize : recHeaderSize+slen] within the record's bytes.
type record struct {
	key   key.Key
	flags byte
	ulen  uint32
	slen  uint32
}

// encodeRecord serializes (k, data) into a complete record, compressing the
// payload with zstd when that makes it strictly smaller. k is written as
// given; canonical-form validation happens on the read side.
func encodeRecord(k key.Key, data []byte) ([]byte, error) {
	if uint64(len(data)) > maxPayload {
		return nil, fmt.Errorf("packstore: object %s too large: %d bytes", k, len(data))
	}
	payload := data
	flags := byte(0)
	if comp := zstdEnc.EncodeAll(data, make([]byte, 0, len(data))); len(comp) < len(data) {
		payload = comp
		flags = flagZstd
	}
	rec := make([]byte, recHeaderSize+len(payload))
	rec[0] = tagChunk
	copy(rec[1:33], k[:])
	rec[33] = flags
	binary.BigEndian.PutUint32(rec[34:38], uint32(len(data)))
	binary.BigEndian.PutUint32(rec[38:42], uint32(len(payload)))
	copy(rec[recHeaderSize:], payload)
	// CRC over the whole record; the crc field itself is still zero here.
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
	return rec, nil
}

var zero4 [4]byte

// parseRecord validates the record at the start of b (which may extend past
// it) and returns its header. It checks framing, flags, key canonicality, and
// the CRC, without mutating b (b may be a read-only mmap).
func parseRecord(b []byte) (record, error) {
	if len(b) < recHeaderSize {
		return record{}, fmt.Errorf("%w: truncated record header", ErrCorrupt)
	}
	if b[0] != tagChunk {
		return record{}, fmt.Errorf("%w: unexpected record tag %#x", ErrCorrupt, b[0])
	}
	flags := b[33]
	if flags&^flagZstd != 0 {
		return record{}, fmt.Errorf("%w: unknown record flags %#x", ErrCorrupt, flags)
	}
	ulen := binary.BigEndian.Uint32(b[34:38])
	slen := binary.BigEndian.Uint32(b[38:42])
	if int64(len(b)) < recHeaderSize+int64(slen) {
		return record{}, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	if flags&flagZstd == 0 && ulen != slen {
		return record{}, fmt.Errorf("%w: raw record with ulen %d != slen %d", ErrCorrupt, ulen, slen)
	}
	if flags&flagZstd != 0 && slen >= ulen {
		return record{}, fmt.Errorf("%w: compressed record with slen %d >= ulen %d", ErrCorrupt, slen, ulen)
	}
	c := crc32.Update(0, castagnoli, b[:42])
	c = crc32.Update(c, castagnoli, zero4[:])
	c = crc32.Update(c, castagnoli, b[recHeaderSize:recHeaderSize+int(slen)])
	if c != binary.BigEndian.Uint32(b[42:46]) {
		return record{}, fmt.Errorf("%w: record CRC mismatch", ErrCorrupt)
	}
	k, err := key.Parse(b[1:33])
	if err != nil {
		return record{}, fmt.Errorf("%w: record key: %v", ErrCorrupt, err)
	}
	return record{key: k, flags: flags, ulen: ulen, slen: slen}, nil
}

// decodePayload returns caller-owned payload bytes from a record's stored
// payload. stored may be a read-only mmap slice and is never retained.
func decodePayload(flags byte, ulen uint32, stored []byte) ([]byte, error) {
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

Note: `tagDelete`, `magicHeader`, `magicTrailer` are unused until later tasks;
Go only rejects unused *imports/locals*, so this compiles.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS (all `TestRecord*` tests).

- [ ] **Step 6: Tidy and commit**

```bash
go mod tidy
go test ./... > /dev/null && echo OK
git add go.mod go.sum packstore/record.go packstore/record_test.go
git commit -m "feat(packstore): record format — encode, parse, zstd store-if-smaller"
```

Expected: `OK`, then a clean commit with `github.com/klauspost/compress` direct in `go.mod`.

---

### Task 2: Index section — fanout on the last key byte + sorted entries

**Files:**
- Create: `packstore/footer.go`
- Test: `packstore/footer_test.go`

The index section is `1024 bytes of fanout (256 × u32 BE, cumulative counts of
entries whose key's LAST byte ≤ b)` followed by `keyCount × 44-byte entries`
(`key[32] | off u64 BE | slen u32 BE`), sorted by `(key[31], full key)`.
Fanout is on the last byte because key byte 0 is type/length-size (clusters);
the hash tail is uniform.

- [ ] **Step 1: Write the failing tests**

Create `packstore/footer_test.go`:

```go
package packstore

import (
	"slices"
	"testing"

	"github.com/draganm/amber-store/key"
)

// testEntries builds n index entries with distinct keys and synthetic offsets.
func testEntries(t *testing.T, n int) []indexEntry {
	t.Helper()
	entries := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		data := append(incompressible(64), byte(i), byte(i>>8), byte(i>>16))
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{k: k, off: uint64(8 + i*100), slen: uint32(i + 1)})
	}
	return entries
}

func TestIndexSectionLookup(t *testing.T) {
	entries := testEntries(t, 1000)
	idx := buildIndexSection(entries)
	if len(idx) != fanoutSize+len(entries)*indexEntrySize {
		t.Fatalf("index length %d", len(idx))
	}
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok {
			t.Fatalf("key %s not found", e.k)
		}
		if off != e.off || slen != e.slen {
			t.Fatalf("key %s: got (%d,%d) want (%d,%d)", e.k, off, slen, e.off, e.slen)
		}
	}
}

func TestIndexSectionAbsentKey(t *testing.T) {
	entries := testEntries(t, 100)
	idx := buildIndexSection(entries)
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	absent := blobObj(t, []byte("definitely not stored")).Key
	if _, _, ok := searchIndex(fanout, entryBytes, absent); ok {
		t.Fatal("absent key reported present")
	}
}

func TestIndexSectionSingleEntryAndEdgeBuckets(t *testing.T) {
	// Force last bytes 0x00 and 0xFF to cover the b==0 lower bound and the
	// final bucket.
	for _, last := range []byte{0x00, 0xFF, 0x80} {
		e := testEntries(t, 1)[0]
		e.k[31] = last
		idx := buildIndexSection([]indexEntry{e})
		fanout, entryBytes, err := parseIndexSection(idx, 1)
		if err != nil {
			t.Fatal(err)
		}
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok || off != e.off || slen != e.slen {
			t.Fatalf("last=%#x: ok=%v off=%d slen=%d", last, ok, off, slen)
		}
		miss := e.k
		miss[30] ^= 0xFF
		if _, _, ok := searchIndex(fanout, entryBytes, miss); ok {
			t.Fatalf("last=%#x: absent key found", last)
		}
	}
}

func TestIndexSectionDoesNotMutateInput(t *testing.T) {
	entries := testEntries(t, 50)
	orig := slices.Clone(entries)
	_ = buildIndexSection(entries)
	if !slices.Equal(entries, orig) {
		t.Fatal("buildIndexSection mutated its input")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `indexEntry`, `buildIndexSection`, `parseIndexSection`, `searchIndex`, `fanoutSize`, `indexEntrySize` undefined.

- [ ] **Step 3: Implement the index section in `packstore/footer.go`**

```go
package packstore

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"

	"github.com/draganm/amber-store/key"
)

const (
	fanoutSize     = 256 * 4 // 256 cumulative u32 counts on the key's last byte
	indexEntrySize = 32 + 8 + 4
	trailerSize    = 64
)

// indexEntry is one sealed-segment index row: a key and where its record
// starts, plus the stored payload length as a readahead hint.
type indexEntry struct {
	k    key.Key
	off  uint64 // file offset of the record header
	slen uint32
}

// compareEntries orders by (last key byte, full key): the fanout is on the
// last byte because byte 0 is type/length-size and clusters, while the hash
// tail is uniformly distributed.
func compareEntries(a, b indexEntry) int {
	if c := cmp.Compare(a.k[key.Size-1], b.k[key.Size-1]); c != 0 {
		return c
	}
	return bytes.Compare(a.k[:], b.k[:])
}

// buildIndexSection serializes the index section (fanout + sorted entries).
// It does not mutate entries.
func buildIndexSection(entries []indexEntry) []byte {
	es := slices.Clone(entries)
	slices.SortFunc(es, compareEntries)

	out := make([]byte, fanoutSize+len(es)*indexEntrySize)
	var counts [256]uint32
	for _, e := range es {
		counts[e.k[key.Size-1]]++
	}
	cum := uint32(0)
	for b := 0; b < 256; b++ {
		cum += counts[b]
		binary.BigEndian.PutUint32(out[b*4:], cum)
	}
	off := fanoutSize
	for _, e := range es {
		copy(out[off:off+32], e.k[:])
		binary.BigEndian.PutUint64(out[off+32:], e.off)
		binary.BigEndian.PutUint32(out[off+40:], e.slen)
		off += indexEntrySize
	}
	return out
}

// parseIndexSection splits an index section into a decoded fanout table and
// the raw entry bytes, validating lengths and fanout monotonicity.
func parseIndexSection(b []byte, keyCount uint64) (*[256]uint32, []byte, error) {
	want := fanoutSize + int(keyCount)*indexEntrySize
	if len(b) != want {
		return nil, nil, fmt.Errorf("%w: index section is %d bytes, want %d", ErrCorrupt, len(b), want)
	}
	var fanout [256]uint32
	prev := uint32(0)
	for i := 0; i < 256; i++ {
		fanout[i] = binary.BigEndian.Uint32(b[i*4:])
		if fanout[i] < prev {
			return nil, nil, fmt.Errorf("%w: fanout not monotonic at byte %#x", ErrCorrupt, i)
		}
		prev = fanout[i]
	}
	if uint64(fanout[255]) != keyCount {
		return nil, nil, fmt.Errorf("%w: fanout total %d != key count %d", ErrCorrupt, fanout[255], keyCount)
	}
	return &fanout, b[fanoutSize:], nil
}

// searchIndex finds k in a parsed index section: fanout bucket on the last
// byte, then binary search on the full key within the bucket.
func searchIndex(fanout *[256]uint32, entries []byte, k key.Key) (off uint64, slen uint32, ok bool) {
	b := k[key.Size-1]
	lo := uint32(0)
	if b > 0 {
		lo = fanout[b-1]
	}
	n := int(fanout[b] - lo)
	i := sort.Search(n, func(i int) bool {
		e := entries[(int(lo)+i)*indexEntrySize:]
		return bytes.Compare(e[:32], k[:]) >= 0
	})
	if i >= n {
		return 0, 0, false
	}
	e := entries[(int(lo)+i)*indexEntrySize:]
	if !bytes.Equal(e[:32], k[:]) {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(e[32:40]), binary.BigEndian.Uint32(e[40:44]), true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/footer.go packstore/footer_test.go
git commit -m "feat(packstore): footer index section — last-byte fanout + sorted entries"
```

---

### Task 3: Filter section — binary fuse filter build/serialize/parse

**Files:**
- Modify: `packstore/footer.go`
- Test: `packstore/footer_test.go`
- Modify: `go.mod`, `go.sum` (xorfilter becomes a used dependency)

Filter input per key is `BigEndian.Uint64(key[24:32])` — the uniform hash
tail. The serialized section is `type u8 (1 = binary fuse 16) | seed u64 |
segLen u32 | segLenMask u32 | segCount u32 | segCountLen u32 | fpCount u32 |
fingerprints fpCount × u16`, all BE; 29 bytes of header + 2 bytes per
fingerprint.

- [ ] **Step 1: Write the failing tests**

Append to `packstore/footer_test.go`:

```go
func TestFilterSectionMembership(t *testing.T) {
	entries := testEntries(t, 5000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !f.Contains(filterKey(e.k)) {
			t.Fatalf("false negative for %s", e.k)
		}
	}
}

func TestFilterSectionFalsePositiveRate(t *testing.T) {
	entries := testEntries(t, 1000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	// 16-bit fingerprints: FP rate ~2^-16. Expect ~1.5 hits in 100k probes;
	// 50 leaves astronomical margin while still catching a broken filter.
	fp := 0
	for i := uint64(0); i < 100_000; i++ {
		if f.Contains(0xDEAD_0000_0000_0000 + i) {
			fp++
		}
	}
	if fp > 50 {
		t.Fatalf("false positive rate too high: %d/100000", fp)
	}
}

func TestFilterSectionDuplicateTails(t *testing.T) {
	// Two entries with an identical 8-byte tail must not break the build.
	entries := testEntries(t, 2)
	copy(entries[1].k[24:32], entries[0].k[24:32])
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Contains(filterKey(entries[0].k)) || !f.Contains(filterKey(entries[1].k)) {
		t.Fatal("false negative on duplicate tails")
	}
}

func TestParseFilterSectionRejectsCorruption(t *testing.T) {
	sec, err := buildFilterSection(testEntries(t, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("short", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:10]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad type", func(t *testing.T) {
		bad := slices.Clone(sec)
		bad[0] = 99
		if _, err := parseFilterSection(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("length mismatch", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:len(sec)-2]); err == nil {
			t.Fatal("want error")
		}
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `buildFilterSection`, `parseFilterSection`, `filterKey` undefined.

- [ ] **Step 3: Implement the filter section in `packstore/footer.go`**

Add to imports: `"github.com/FastFilter/xorfilter"`. Then:

```go
const (
	filterHeaderSize       = 29
	filterTypeBinaryFuse16 = 1
)

// filterKey is the filter input for k: the last 8 bytes of the key, which lie
// in the uniformly distributed truncated-hash region.
func filterKey(k key.Key) uint64 {
	return binary.BigEndian.Uint64(k[key.Size-8:])
}

// buildFilterSection builds and serializes a binary fuse filter over the
// entries' keys. Duplicate 8-byte tails are deduplicated before the build.
func buildFilterSection(entries []indexEntry) ([]byte, error) {
	tails := make([]uint64, 0, len(entries))
	for _, e := range entries {
		tails = append(tails, filterKey(e.k))
	}
	slices.Sort(tails)
	tails = slices.Compact(tails)
	f, err := xorfilter.NewBinaryFuse[uint16](tails)
	if err != nil {
		return nil, fmt.Errorf("packstore: building fuse filter: %w", err)
	}
	out := make([]byte, filterHeaderSize+2*len(f.Fingerprints))
	out[0] = filterTypeBinaryFuse16
	binary.BigEndian.PutUint64(out[1:9], f.Seed)
	binary.BigEndian.PutUint32(out[9:13], f.SegmentLength)
	binary.BigEndian.PutUint32(out[13:17], f.SegmentLengthMask)
	binary.BigEndian.PutUint32(out[17:21], f.SegmentCount)
	binary.BigEndian.PutUint32(out[21:25], f.SegmentCountLength)
	binary.BigEndian.PutUint32(out[25:29], uint32(len(f.Fingerprints)))
	for i, fp := range f.Fingerprints {
		binary.BigEndian.PutUint16(out[filterHeaderSize+2*i:], fp)
	}
	return out, nil
}

// parseFilterSection deserializes a filter section, copying fingerprints out
// of b (which may be a read-only mmap) into RAM. The five geometry fields are
// validated against the binary-fuse construction invariants: Contains indexes
// Fingerprints from them, so crafted values would otherwise panic the read
// path rather than fail parse with ErrCorrupt.
func parseFilterSection(b []byte) (*xorfilter.BinaryFuse[uint16], error) {
	if len(b) < filterHeaderSize {
		return nil, fmt.Errorf("%w: filter section too short: %d bytes", ErrCorrupt, len(b))
	}
	if b[0] != filterTypeBinaryFuse16 {
		return nil, fmt.Errorf("%w: unknown filter type %d", ErrCorrupt, b[0])
	}
	fpCount := binary.BigEndian.Uint32(b[25:29])
	if uint64(len(b)) != filterHeaderSize+2*uint64(fpCount) {
		return nil, fmt.Errorf("%w: filter section is %d bytes, want %d", ErrCorrupt, len(b), filterHeaderSize+2*uint64(fpCount))
	}
	segLen := binary.BigEndian.Uint32(b[9:13])
	segLenMask := binary.BigEndian.Uint32(b[13:17])
	segCount := binary.BigEndian.Uint32(b[17:21])
	segCountLen := binary.BigEndian.Uint32(b[21:25])
	switch {
	case segLen == 0 || segLen&(segLen-1) != 0,
		segLenMask != segLen-1,
		uint64(segCountLen) != uint64(segCount)*uint64(segLen),
		uint64(fpCount) != uint64(segCountLen)+2*uint64(segLen):
		return nil, fmt.Errorf("%w: filter geometry invalid", ErrCorrupt)
	}
	f := &xorfilter.BinaryFuse[uint16]{
		Seed:               binary.BigEndian.Uint64(b[1:9]),
		SegmentLength:      segLen,
		SegmentLengthMask:  segLenMask,
		SegmentCount:       segCount,
		SegmentCountLength: segCountLen,
		Fingerprints:       make([]uint16, fpCount),
	}
	for i := range f.Fingerprints {
		f.Fingerprints[i] = binary.BigEndian.Uint16(b[filterHeaderSize+2*i:])
	}
	return f, nil
}
```

- [ ] **Step 4: Run tests, tidy, commit**

```bash
go get github.com/FastFilter/xorfilter@v0.5.1
go mod tidy
go test ./packstore/ -count=1
git add go.mod go.sum packstore/footer.go packstore/footer_test.go
git commit -m "feat(packstore): footer filter section — binary fuse over key hash tails"
```

Expected: PASS, clean commit (xorfilter now a direct dependency).

---

### Task 4: Footer assembly, trailer, parse, and the mmap'd sealed segment

**Files:**
- Modify: `packstore/footer.go`
- Test: `packstore/footer_test.go`

Trailer (last 64 bytes of a sealed file, all BE):

```
[0:8]    indexOff    [8:16]  indexLen   [16:24] filterOff  [24:32] filterLen
[32:40]  keyCount    [40:48] bodyLen
[48:52]  footerCRC   — CRC-32C over file[bodyLen : EOF-16)
[52:56]  reserved    — must be 0
[56:64]  magic       — "AMBERSGF"
```

- [ ] **Step 1: Write the failing tests**

Append to `packstore/footer_test.go` (add imports `"bytes"`, `"encoding/binary"`, `"os"`, `"path/filepath"`):

```go
// writeSealedFile assembles a complete sealed segment on disk from objects:
// header, records, footer. Returns the path and the entries written.
func writeSealedFile(t *testing.T, objs []Object) (string, []indexEntry) {
	t.Helper()
	var body []byte
	body = append(body, magicHeader...)
	var entries []indexEntry
	for _, o := range objs {
		rec, err := encodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{
			k: o.Key, off: uint64(len(body)),
			slen: uint32(len(rec) - recHeaderSize),
		})
		body = append(body, rec...)
	}
	footer, err := buildFooter(int64(len(body)), entries)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "0000000000000001.seg")
	if err := os.WriteFile(path, append(body, footer...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path, entries
}

func testObjects(t *testing.T, n int) []Object {
	t.Helper()
	objs := make([]Object, 0, n)
	for i := 0; i < n; i++ {
		var data []byte
		if i%2 == 0 {
			data = append(compressible(2000), byte(i), byte(i>>8))
		} else {
			data = append(incompressible(2000), byte(i), byte(i>>8))
		}
		objs = append(objs, blobObj(t, data))
	}
	return objs
}

func TestSealedSegmentRoundTrip(t *testing.T) {
	objs := testObjects(t, 200)
	path, _ := writeSealedFile(t, objs)
	seg, err := openSealed(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()
	if seg.fv.keyCount != 200 {
		t.Fatalf("keyCount = %d", seg.fv.keyCount)
	}
	for _, o := range objs {
		if !seg.has(o.Key) {
			t.Fatalf("has(%s) = false", o.Key)
		}
		data, found, err := seg.get(o.Key)
		if err != nil || !found {
			t.Fatalf("get(%s): found=%v err=%v", o.Key, found, err)
		}
		if !bytes.Equal(data, o.Data) {
			t.Fatalf("get(%s): payload mismatch", o.Key)
		}
	}
	absent := blobObj(t, []byte("not here")).Key
	if seg.has(absent) {
		t.Fatal("has(absent) = true")
	}
	if _, found, _ := seg.get(absent); found {
		t.Fatal("get(absent) found")
	}
}

func TestOpenSealedRejectsCorruption(t *testing.T) {
	objs := testObjects(t, 20)
	path, _ := writeSealedFile(t, objs)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	corrupt := func(t *testing.T, mutate func(b []byte)) {
		t.Helper()
		b := bytes.Clone(good)
		mutate(b)
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openSealed(p, 1); err == nil {
			t.Fatal("want error")
		}
	}

	t.Run("trailer magic", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[len(b)-1] ^= 0xFF })
	})
	t.Run("footer CRC over index", func(t *testing.T) {
		corrupt(t, func(b []byte) {
			tr := b[len(b)-trailerSize:]
			indexOff := binary.BigEndian.Uint64(tr[0:8])
			b[indexOff+10] ^= 0xFF
		})
	})
	t.Run("header magic", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[0] ^= 0xFF })
	})
	t.Run("reserved nonzero", func(t *testing.T) {
		corrupt(t, func(b []byte) { b[len(b)-12] = 1 }) // CRC does not cover the last 16 bytes; reserved is checked explicitly
	})
	t.Run("truncated", func(t *testing.T) {
		b := bytes.Clone(good[:len(good)-100])
		p := filepath.Join(t.TempDir(), "0000000000000001.seg")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openSealed(p, 1); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestBuildFooterRejectsEmpty(t *testing.T) {
	if _, err := buildFooter(8, nil); err == nil {
		t.Fatal("want error for empty segment")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `buildFooter`, `openSealed`, `footerView` undefined.

- [ ] **Step 3: Implement footer assembly + parse + sealedSegment in `packstore/footer.go`**

Add imports: `"hash/crc32"`, `"math"`, `"os"`, `"golang.org/x/sys/unix"`.

```go
// buildFooter assembles the complete footer (seal marker, index section,
// filter section, trailer) for a segment whose records end at bodyLen.
func buildFooter(bodyLen int64, entries []indexEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("packstore: refusing to seal an empty segment")
	}
	idx := buildIndexSection(entries)
	filt, err := buildFilterSection(entries)
	if err != nil {
		return nil, err
	}
	footer := make([]byte, 0, 1+len(idx)+len(filt)+trailerSize)
	footer = append(footer, tagSeal)
	footer = append(footer, idx...)
	footer = append(footer, filt...)

	tr := make([]byte, trailerSize)
	indexOff := uint64(bodyLen) + 1
	binary.BigEndian.PutUint64(tr[0:8], indexOff)
	binary.BigEndian.PutUint64(tr[8:16], uint64(len(idx)))
	binary.BigEndian.PutUint64(tr[16:24], indexOff+uint64(len(idx)))
	binary.BigEndian.PutUint64(tr[24:32], uint64(len(filt)))
	binary.BigEndian.PutUint64(tr[32:40], uint64(len(entries)))
	binary.BigEndian.PutUint64(tr[40:48], uint64(bodyLen))
	copy(tr[56:64], magicTrailer)
	footer = append(footer, tr...)
	// footerCRC covers [bodyLen, EOF-16): everything up to and excluding the
	// crc field itself; reserved and magic are checked explicitly on parse.
	binary.BigEndian.PutUint32(footer[len(footer)-16:], crc32.Checksum(footer[:len(footer)-16], castagnoli))
	return footer, nil
}

// footerView is the parsed footer of a sealed segment. fanout and filter live
// in RAM; entries points into the segment's mmap.
type footerView struct {
	fanout   [256]uint32
	entries  []byte
	filter   *xorfilter.BinaryFuse[uint16]
	keyCount uint64
	bodyLen  int64
	indexOff int64
	indexLen int64
}

// parseFooter validates a whole sealed-segment image (header through trailer)
// and returns its footer view. mm may be a read-only mmap; nothing is mutated.
func parseFooter(mm []byte) (*footerView, error) {
	if len(mm) < len(magicHeader)+1+fanoutSize+indexEntrySize+filterHeaderSize+trailerSize {
		return nil, fmt.Errorf("%w: file too short: %d bytes", ErrCorrupt, len(mm))
	}
	if !bytes.Equal(mm[:len(magicHeader)], magicHeader) {
		return nil, fmt.Errorf("%w: bad header magic", ErrCorrupt)
	}
	tr := mm[len(mm)-trailerSize:]
	if !bytes.Equal(tr[56:64], magicTrailer) {
		return nil, fmt.Errorf("%w: bad trailer magic", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(tr[52:56]) != 0 {
		return nil, fmt.Errorf("%w: nonzero reserved trailer field", ErrCorrupt)
	}
	indexOff := binary.BigEndian.Uint64(tr[0:8])
	indexLen := binary.BigEndian.Uint64(tr[8:16])
	filterOff := binary.BigEndian.Uint64(tr[16:24])
	filterLen := binary.BigEndian.Uint64(tr[24:32])
	keyCount := binary.BigEndian.Uint64(tr[32:40])
	bodyLen := binary.BigEndian.Uint64(tr[40:48])

	fileLen := uint64(len(mm))
	switch {
	case bodyLen < uint64(len(magicHeader)) || bodyLen >= fileLen,
		keyCount > math.MaxUint32, // fanout counts are u32; also keeps the next line overflow-free
		indexOff != bodyLen+1,
		indexLen != uint64(fanoutSize)+keyCount*indexEntrySize,
		filterOff != indexOff+indexLen,
		filterOff > fileLen-trailerSize,
		filterLen != fileLen-trailerSize-filterOff:
		return nil, fmt.Errorf("%w: trailer offsets inconsistent", ErrCorrupt)
	}
	if crc32.Checksum(mm[bodyLen:fileLen-16], castagnoli) != binary.BigEndian.Uint32(tr[48:52]) {
		return nil, fmt.Errorf("%w: footer CRC mismatch", ErrCorrupt)
	}
	if mm[bodyLen] != tagSeal {
		return nil, fmt.Errorf("%w: missing seal marker", ErrCorrupt)
	}

	fv := &footerView{
		keyCount: keyCount,
		bodyLen:  int64(bodyLen),
		indexOff: int64(indexOff),
		indexLen: int64(indexLen),
	}
	fanout, entries, err := parseIndexSection(mm[indexOff:indexOff+indexLen], keyCount)
	if err != nil {
		return nil, err
	}
	fv.fanout = *fanout
	fv.entries = entries
	if fv.filter, err = parseFilterSection(mm[filterOff : filterOff+filterLen]); err != nil {
		return nil, err
	}
	return fv, nil
}

// lookup finds k in the segment's index.
func (fv *footerView) lookup(k key.Key) (off uint64, slen uint32, ok bool) {
	return searchIndex(&fv.fanout, fv.entries, k)
}

// sealedSegment is an immutable, fully mmap'd segment.
type sealedSegment struct {
	id   uint64
	path string
	mm   []byte
	fv   *footerView
}

// openSealed maps a sealed segment and validates its footer. The fd is closed
// after mapping; the mapping keeps the file content alive.
func openSealed(path string, id uint64) (*sealedSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(len(magicHeader)+1+fanoutSize+indexEntrySize+filterHeaderSize+trailerSize) {
		return nil, fmt.Errorf("%w: %s: file too short: %d bytes", ErrCorrupt, path, st.Size())
	}
	mm, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("packstore: mmap %s: %w", path, err)
	}
	fv, err := parseFooter(mm)
	if err != nil {
		unix.Munmap(mm)
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &sealedSegment{id: id, path: path, mm: mm, fv: fv}, nil
}

func (g *sealedSegment) close() error {
	return unix.Munmap(g.mm)
}

// has reports whether k is in this segment: fuse filter first (cheap,
// probabilistic), then the exact index.
func (g *sealedSegment) has(k key.Key) bool {
	if !g.fv.filter.Contains(filterKey(k)) {
		return false
	}
	_, _, ok := g.fv.lookup(k)
	return ok
}

// get returns k's payload (caller-owned), whether it was found, and any
// corruption error. The hot path does not CRC-check; that is scrub's job.
func (g *sealedSegment) get(k key.Key) ([]byte, bool, error) {
	if !g.fv.filter.Contains(filterKey(k)) {
		return nil, false, nil
	}
	off, slen, ok := g.fv.lookup(k)
	if !ok {
		return nil, false, nil
	}
	bodyLen := uint64(g.fv.bodyLen)
	if off < uint64(len(magicHeader)) || off > bodyLen ||
		uint64(recHeaderSize)+uint64(slen) > bodyLen-off {
		return nil, false, fmt.Errorf("%w: %s: index entry out of bounds", ErrCorrupt, g.path)
	}
	end := off + recHeaderSize + uint64(slen)
	h := g.mm[off : off+recHeaderSize]
	flags := h[33]
	ulen := binary.BigEndian.Uint32(h[34:38])
	data, err := decodePayload(flags, ulen, g.mm[off+recHeaderSize:end])
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", g.path, err)
	}
	return data, true, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/footer.go packstore/footer_test.go
git commit -m "feat(packstore): footer assembly/parse and mmap'd sealed segments"
```

---

### Task 5: Tail-scan recovery of the active segment

**Files:**
- Create: `packstore/recover.go`
- Test: `packstore/recover_test.go`

`scanActive` walks an active segment file and decides how much of it is valid.
Contract (from the spec): parse records from offset 8; on the first invalid or
truncated record, stop — everything before it survives, everything from it on
is garbage to truncate. A `tagSeal` byte whose remainder parses as a valid
footer means the file is actually sealed (crash between footer-write and
rename). A file with a missing/invalid 8-byte header is safe to reset to
empty: any fsync that ever succeeded would have made the header durable too,
so nothing acknowledged is lost.

- [ ] **Step 1: Write the failing tests**

Create `packstore/recover_test.go`:

```go
package packstore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// activeFile writes b to a temp .seg.active file and returns the path.
func activeFile(t *testing.T, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "0000000000000001.seg.active")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// buildBody returns header+records bytes plus each record's (key, start offset, length).
type recSpan struct {
	obj   Object
	off   int64
	recLen int
}

func buildBody(t *testing.T, objs []Object) ([]byte, []recSpan) {
	t.Helper()
	body := append([]byte{}, magicHeader...)
	var spans []recSpan
	for _, o := range objs {
		rec, err := encodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		spans = append(spans, recSpan{obj: o, off: int64(len(body)), recLen: len(rec)})
		body = append(body, rec...)
	}
	return body, spans
}

func TestScanActiveCleanFile(t *testing.T) {
	objs := testObjects(t, 5)
	body, spans := buildBody(t, objs)
	res, err := scanActive(activeFile(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if res.sealed {
		t.Fatal("clean active reported sealed")
	}
	if res.size != int64(len(body)) {
		t.Fatalf("size = %d, want %d", res.size, len(body))
	}
	if len(res.index) != len(objs) {
		t.Fatalf("index has %d keys, want %d", len(res.index), len(objs))
	}
	for _, s := range spans {
		loc, ok := res.index[s.obj.Key]
		if !ok || loc.off != s.off {
			t.Fatalf("key %s: loc %+v ok=%v want off %d", s.obj.Key, loc, ok, s.off)
		}
	}
}

func TestScanActiveTruncationAtEveryByte(t *testing.T) {
	objs := testObjects(t, 3)
	body, spans := buildBody(t, objs)
	// boundary(cut) = largest record boundary <= cut.
	boundary := func(cut int) int64 {
		b := int64(len(magicHeader))
		for _, s := range spans {
			if s.off+int64(s.recLen) <= int64(cut) {
				b = s.off + int64(s.recLen)
			}
		}
		return b
	}
	for cut := len(magicHeader); cut <= len(body); cut++ {
		res, err := scanActive(activeFile(t, body[:cut]))
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if want := boundary(cut); res.size != want {
			t.Fatalf("cut=%d: size=%d want %d", cut, res.size, want)
		}
		wantKeys := 0
		for _, s := range spans {
			if s.off+int64(s.recLen) <= res.size {
				wantKeys++
			}
		}
		if len(res.index) != wantKeys {
			t.Fatalf("cut=%d: %d keys, want %d", cut, len(res.index), wantKeys)
		}
	}
}

func TestScanActiveCorruptByteTruncatesAtThatRecord(t *testing.T) {
	objs := testObjects(t, 3)
	body, spans := buildBody(t, objs)
	last := spans[2]
	for off := last.off; off < last.off+int64(last.recLen); off++ {
		bad := bytes.Clone(body)
		bad[off] ^= 0xFF
		res, err := scanActive(activeFile(t, bad))
		if err != nil {
			t.Fatalf("off=%d: %v", off, err)
		}
		if res.size != last.off {
			t.Fatalf("corrupt byte at %d: size=%d, want truncation at %d", off, res.size, last.off)
		}
	}
}

func TestScanActiveBadHeaderResets(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("AMB"), []byte("XXXXXXXXjunkjunk")} {
		res, err := scanActive(activeFile(t, b))
		if err != nil {
			t.Fatal(err)
		}
		if res.size != 0 || len(res.index) != 0 || res.sealed {
			t.Fatalf("bad header: %+v", res)
		}
	}
}

func TestScanActiveDetectsSealedFile(t *testing.T) {
	objs := testObjects(t, 5)
	path, _ := writeSealedFile(t, objs) // a fully sealed image, named .seg
	res, err := scanActive(path)
	if err != nil {
		t.Fatal(err)
	}
	if !res.sealed {
		t.Fatal("sealed file not detected")
	}
}

func TestScanActivePartialFooterTruncates(t *testing.T) {
	objs := testObjects(t, 5)
	body, _ := buildBody(t, objs)
	entriesLen := int64(len(body))
	footerish := append(bytes.Clone(body), tagSeal)
	footerish = append(footerish, bytes.Repeat([]byte{0xAB}, 100)...) // garbage, not a valid footer
	res, err := scanActive(activeFile(t, footerish))
	if err != nil {
		t.Fatal(err)
	}
	if res.sealed {
		t.Fatal("partial footer reported sealed")
	}
	if res.size != entriesLen {
		t.Fatalf("size=%d want %d (truncate at seal marker)", res.size, entriesLen)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `scanActive`, `scanResult`, `activeLoc` undefined.

- [ ] **Step 3: Implement `packstore/recover.go`**

```go
package packstore

import (
	"bytes"
	"os"

	"github.com/draganm/amber-store/key"
)

// activeLoc locates one record inside the active segment.
type activeLoc struct {
	off   int64 // record header offset
	flags byte
	ulen  uint32
	slen  uint32
}

// scanResult is the outcome of tail-scanning an active segment file.
type scanResult struct {
	size   int64                  // valid length; the caller truncates to this (0 ⇒ reset header)
	index  map[key.Key]activeLoc  // records fully contained in [0, size)
	sealed bool                   // the file carries a complete valid footer: rename it, it is sealed
}

// scanActive reads an active segment file and finds the boundary of valid
// data. Records are self-framing and CRC'd, so the scan accepts records until
// the first invalid byte and truncates there. Acknowledged (fsynced) data is
// always before that boundary: fsync covers the whole file, so a valid record
// can only be preceded by valid bytes.
func scanActive(path string) (scanResult, error) {
	res := scanResult{index: make(map[key.Key]activeLoc)}
	b, err := os.ReadFile(path)
	if err != nil {
		return res, err
	}
	if len(b) < len(magicHeader) || !bytes.Equal(b[:len(magicHeader)], magicHeader) {
		// Header never made it to disk; nothing in this file was ever
		// acknowledged (any successful fsync would have persisted the header
		// too). Reset to empty.
		return res, nil
	}
	off := int64(len(magicHeader))
	for off < int64(len(b)) {
		if b[off] == tagSeal {
			if _, err := parseFooter(b); err == nil {
				return scanResult{size: int64(len(b)), sealed: true}, nil
			}
			break // partial footer: truncate at the seal marker
		}
		rec, err := parseRecord(b[off:])
		if err != nil {
			break // invalid or truncated: everything from off on is garbage
		}
		res.index[rec.key] = activeLoc{off: off, flags: rec.flags, ulen: rec.ulen, slen: rec.slen}
		off += recHeaderSize + int64(rec.slen)
	}
	res.size = off
	return res, nil
}
```

Note `scanActive` reads the whole file into memory (≤ segment size + footer,
so ≤ ~300 MB worst case, once at Open) — simple beats streaming here.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS. The every-byte truncation test is the spec's crash-recovery
requirement; it is O(bodyBytes²) in CRC work but the bodies are ~6 KB, so it
runs in milliseconds.

- [ ] **Step 5: Commit**

```bash
git add packstore/recover.go packstore/recover_test.go
git commit -m "feat(packstore): active-segment tail-scan recovery"
```

---

### Task 6: Store core — Open, Put, Get, Has, Close, flock, resume

**Files:**
- Create: `packstore/packstore.go`
- Test: `packstore/packstore_test.go`

Lock ordering rule (document in code, follow everywhere): **`appendMu` before
`mu`, never the reverse.** `appendMu` (plain Mutex) serializes the write path:
appends, fsync, seal, Close. `mu` (RWMutex) guards the segment list, the
active index map, and the closed flag; readers only ever take `mu.RLock`.

- [ ] **Step 1: Write the failing tests**

Create `packstore/packstore_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `Store`, `Open`, `Option`, `ErrNotFound`, `ErrClosed` undefined.

- [ ] **Step 3: Implement `packstore/packstore.go`**

```go
package packstore

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// ErrNotFound is returned by Get for a key that is not present in the store.
var ErrNotFound = errors.New("packstore: object not found")

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("packstore: store closed")

// DefaultSegmentSize is the default rotation threshold: the active segment is
// sealed once it reaches this many bytes.
const DefaultSegmentSize = 256 << 20 // 256 MiB

const (
	sealedSuffix = ".seg"
	activeSuffix = ".seg.active"
)

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	segmentSize int64
	sync        bool
}

func defaultConfig() config {
	return config{segmentSize: DefaultSegmentSize, sync: true}
}

// WithSegmentSize sets the rotation threshold in bytes. A single oversized
// record may push one segment past it.
func WithSegmentSize(n int64) Option {
	return func(c *config) { c.segmentSize = n }
}

// WithSync controls whether writes are fsynced for crash durability. Default
// is true; disabling it speeds bulk loads and tests.
func WithSync(b bool) Option {
	return func(c *config) { c.sync = b }
}

// activeSegment is the single append-only segment accepting writes.
type activeSegment struct {
	id    uint64
	path  string
	f     *os.File
	size  int64 // accessed only under appendMu
	index map[key.Key]activeLoc
}

// Store is an on-disk content-addressable store over segment files. It is
// safe for concurrent use. Lock ordering: appendMu before mu, never the
// reverse. appendMu serializes the write path (append, fsync, seal, Close);
// mu guards sealed/active/closed for readers.
type Store struct {
	dir  string
	dirF *os.File // holds the flock; also used for directory fsyncs
	cfg  config

	appendMu sync.Mutex
	mu       sync.RWMutex
	sealed   []*sealedSegment // ascending id; newest last
	active   *activeSegment   // nil until the first write of a session
	nextID   uint64
	closed   bool
	failed   error // sticky write-path failure; written under appendMu+mu, read under either
}

// Open opens (creating if necessary) a store rooted at dir. Only one Store
// may have a given dir open at a time (flock on the directory). Sealed
// segments are mmap'd and validated; the active segment, if any, is
// tail-scanned and truncated to its last valid record.
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("packstore: creating %s: %w", dir, err)
	}
	dirF, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(dirF.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		dirF.Close()
		return nil, fmt.Errorf("packstore: %s is already open: %w", dir, err)
	}
	s := &Store{dir: dir, dirF: dirF, cfg: cfg, nextID: 1}
	if err := s.load(); err != nil {
		s.releaseDir()
		return nil, err
	}
	return s, nil
}

func (s *Store) releaseDir() {
	for _, seg := range s.sealed {
		seg.close()
	}
	s.dirF.Close() // releases the flock
}

// load scans the directory: sealed segments are opened and validated, the
// active segment (at most one) is recovered.
func (s *Store) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var activePaths []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, activeSuffix):
			activePaths = append(activePaths, name)
		case strings.HasSuffix(name, sealedSuffix):
			id, err := parseSegmentID(name, sealedSuffix)
			if err != nil {
				return err
			}
			seg, err := openSealed(filepath.Join(s.dir, name), id)
			if err != nil {
				return err
			}
			s.sealed = append(s.sealed, seg)
			if id >= s.nextID {
				s.nextID = id + 1
			}
		}
		// Anything else (e.g. .DS_Store) is ignored.
	}
	slices.SortFunc(s.sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })

	if len(activePaths) > 1 {
		return fmt.Errorf("%w: %d active segments, want at most one: %v", ErrCorrupt, len(activePaths), activePaths)
	}
	if len(activePaths) == 0 {
		return nil
	}
	return s.recoverActive(activePaths[0])
}

func parseSegmentID(name, suffix string) (uint64, error) {
	hex := strings.TrimSuffix(name, suffix)
	id, err := strconv.ParseUint(hex, 16, 64)
	if err != nil || len(hex) != 16 {
		return 0, fmt.Errorf("%w: bad segment file name %q", ErrCorrupt, name)
	}
	return id, nil
}

// recoverActive tail-scans name, then either completes a crashed seal-rename
// or truncates the file to its valid prefix and resumes it as the active
// segment.
func (s *Store) recoverActive(name string) error {
	id, err := parseSegmentID(name, activeSuffix)
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, name)
	res, err := scanActive(path)
	if err != nil {
		return err
	}
	if id >= s.nextID {
		s.nextID = id + 1
	}
	if res.sealed {
		// Crash between footer-write and rename: complete the rename.
		sealedPath := strings.TrimSuffix(path, ".active")
		if err := os.Rename(path, sealedPath); err != nil {
			return err
		}
		if err := s.dirF.Sync(); err != nil {
			return err
		}
		seg, err := openSealed(sealedPath, id)
		if err != nil {
			return err
		}
		s.sealed = append(s.sealed, seg)
		slices.SortFunc(s.sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })
		return nil
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	size := res.size
	if size < int64(len(magicHeader)) {
		// Header never became durable: reset to a fresh header.
		if err := f.Truncate(0); err != nil {
			f.Close()
			return err
		}
		if _, err := f.WriteAt(magicHeader, 0); err != nil {
			f.Close()
			return err
		}
		size = int64(len(magicHeader))
	} else {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return err
		}
	}
	s.active = &activeSegment{id: id, path: path, f: f, size: size, index: res.index}
	return nil
}

// createActive opens the next-numbered active segment. Called under appendMu.
func (s *Store) createActive() error {
	id := s.nextID
	s.nextID++
	path := filepath.Join(s.dir, fmt.Sprintf("%016x%s", id, activeSuffix))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(path) // never leave a second .seg.active to brick the next Open
		return err
	}
	if _, err := f.WriteAt(magicHeader, 0); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := s.dirF.Sync(); err != nil {
		return fail(err)
	}
	a := &activeSegment{id: id, path: path, f: f, size: int64(len(magicHeader)), index: make(map[key.Key]activeLoc)}
	s.mu.Lock()
	s.active = a
	s.mu.Unlock()
	return nil
}

// append writes one encoded record to the active segment (creating it if
// needed), publishes it in the active index, optionally fsyncs, and seals the
// segment if it reached the rotation threshold.
func (s *Store) append(k key.Key, rec []byte, syncNow bool) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if s.active == nil {
		if err := s.createActive(); err != nil {
			return err
		}
	}
	a := s.active
	if _, ok := a.index[k]; ok {
		return nil // lost a Put race for this key; the record is already appended
	}
	off := a.size
	if _, err := a.f.WriteAt(rec, off); err != nil {
		return err
	}
	loc := activeLoc{
		off:   off,
		flags: rec[33],
		ulen:  binary.BigEndian.Uint32(rec[34:38]),
		slen:  binary.BigEndian.Uint32(rec[38:42]),
	}
	s.mu.Lock()
	a.index[k] = loc
	s.mu.Unlock()
	a.size = off + int64(len(rec))

	if syncNow && s.cfg.sync {
		if err := a.f.Sync(); err != nil {
			s.setFailed(err)
			return err
		}
	}
	if a.size >= s.cfg.segmentSize {
		// A mid-seal failure can leave a renamed-but-unpublished segment or
		// an un-mmap'd sealed file; reads stay correct (the fd is still
		// open), but accepting further writes could append past a footer.
		// Poison the write path; reopen recovers cleanly.
		if err := s.sealActiveLocked(); err != nil {
			s.setFailed(err)
			return err
		}
	}
	return nil
}

// syncActive fsyncs the active segment, if syncing is enabled and one exists.
func (s *Store) syncActive() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if !s.cfg.sync || s.active == nil {
		return nil
	}
	if err := s.active.f.Sync(); err != nil {
		s.setFailed(err)
		return err
	}
	return nil
}

// setFailed poisons the write path after an fsync failure: a failed fsync may
// have dropped dirty pages, so later appends could be acknowledged while
// sitting behind a garbage hole that tail-scan recovery would truncate. Reads
// stay available; only new acknowledgments stop. Called under appendMu.
func (s *Store) setFailed(err error) {
	s.mu.Lock()
	if s.failed == nil {
		s.failed = fmt.Errorf("packstore: write path failed: %w", err)
	}
	s.mu.Unlock()
}

// sealActiveLocked seals the active segment: footer, fsync, rename to .seg,
// directory fsync, mmap. Called under appendMu. Implemented in Task 7; until
// then it is a stub that never triggers (tests use the default 256 MiB
// threshold).
func (s *Store) sealActiveLocked() error {
	return errors.New("packstore: sealing not implemented yet")
}

// Put stores a single object under k, deduplicating against existing content.
func (s *Store) Put(k key.Key, data []byte) error {
	s.mu.RLock()
	failed := s.failed
	s.mu.RUnlock()
	if failed != nil {
		return failed
	}
	has, err := s.Has(k)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	rec, err := encodeRecord(k, data)
	if err != nil {
		return err
	}
	return s.append(k, rec, true)
}

// Get returns the bytes stored under k, or ErrNotFound if k is absent. The
// returned slice is caller-owned.
func (s *Store) Get(k key.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.active != nil {
		if loc, ok := s.active.index[k]; ok {
			stored := make([]byte, loc.slen)
			if _, err := s.active.f.ReadAt(stored, loc.off+recHeaderSize); err != nil {
				return nil, err
			}
			return decodePayload(loc.flags, loc.ulen, stored)
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		data, found, err := s.sealed[i].get(k)
		// A corrupt segment fails the read loudly rather than falling back to
		// older copies: masking corruption would hide real damage from scrub.
		if err != nil {
			return nil, err
		}
		if found {
			return data, nil
		}
	}
	return nil, ErrNotFound
}

// Has reports whether an object is stored under k.
func (s *Store) Has(k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if s.active != nil {
		if _, ok := s.active.index[k]; ok {
			return true, nil
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// Close fsyncs and closes the active segment (without sealing it), unmaps all
// sealed segments, and releases the directory lock.
func (s *Store) Close() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if s.active != nil {
		if err := s.active.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.active.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.active = nil
	}
	for _, seg := range s.sealed {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.sealed = nil
	if err := s.dirF.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/packstore.go packstore/packstore_test.go
git commit -m "feat(packstore): store core — open/put/get/has/close, flock, resume"
```

---

### Task 7: Sealing and rotation

**Files:**
- Modify: `packstore/packstore.go` (replace the `sealActiveLocked` stub)
- Test: `packstore/packstore_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `packstore/packstore_test.go`:

```go
func TestRotationSealsSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny threshold + incompressible payloads: every record (~2 KB stored)
	// crosses 1024 bytes, so every Put seals a segment. (testObjects would
	// not work here: its compressible payloads store as ~76-byte records.)
	s := openStore(t, dir, WithSegmentSize(1024))
	var objs []Object
	for i := 0; i < 30; i++ {
		objs = append(objs, blobObj(t, append(incompressible(2000), byte(i), byte(i>>8))))
	}
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) != 30 {
		t.Fatalf("%d sealed segments, want 30", len(segs))
	}
	// All objects must be served from sealed segments.
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s) from sealed: %v", o.Key, err)
		}
	}
	// And survive a reopen.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir)
	for _, o := range objs {
		data, err := s2.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s) after reopen: %v", o.Key, err)
		}
	}
}

func TestRotationMidStreamKeepsAllObjects(t *testing.T) {
	// Mixed compressible/incompressible objects across several rotations:
	// every object must remain reachable from whichever segment it landed in.
	dir := t.TempDir()
	s := openStore(t, dir, WithSegmentSize(8<<10))
	objs := testObjects(t, 100)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
}

func TestCrashBetweenFooterAndRename(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 10)
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window: append a valid footer to the .active file
	// but do not rename it.
	actives, _ := filepath.Glob(filepath.Join(dir, "*.seg.active"))
	res, err := scanActive(actives[0])
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]indexEntry, 0, len(res.index))
	for k, loc := range res.index {
		entries = append(entries, indexEntry{k: k, off: uint64(loc.off), slen: loc.slen})
	}
	footer, err := buildFooter(res.size, entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(actives[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Open must complete the rename and serve everything from the sealed file.
	s2 := openStore(t, dir)
	for _, o := range objs {
		data, err := s2.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	actives, _ = filepath.Glob(filepath.Join(dir, "*.seg.active"))
	if len(segs) != 1 || len(actives) != 0 {
		t.Fatalf("segs=%v actives=%v", segs, actives)
	}
}

func TestOpenFailsOnCorruptSealedSegment(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, WithSegmentSize(1))
	o := blobObj(t, incompressible(500))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) != 1 {
		t.Fatalf("segs=%v", segs)
	}
	b, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // trailer magic
	if err := os.WriteFile(segs[0], b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open = %v, want ErrCorrupt", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: `TestRotationSealsSegments` and friends FAIL with "sealing not implemented yet".

- [ ] **Step 3: Implement sealing — replace the `sealActiveLocked` stub in `packstore/packstore.go`**

```go
// sealActiveLocked seals the active segment: build the footer from the
// in-RAM index (no body re-read), append it, fsync, rename to .seg, fsync the
// directory, and swap in the mmap'd sealed segment. Called under appendMu.
func (s *Store) sealActiveLocked() error {
	a := s.active
	if a == nil || len(a.index) == 0 {
		return nil
	}
	entries := make([]indexEntry, 0, len(a.index))
	for k, loc := range a.index {
		entries = append(entries, indexEntry{k: k, off: uint64(loc.off), slen: loc.slen})
	}
	footer, err := buildFooter(a.size, entries)
	if err != nil {
		return err
	}
	if _, err := a.f.WriteAt(footer, a.size); err != nil {
		return err
	}
	if err := a.f.Sync(); err != nil {
		return err
	}
	sealedPath := strings.TrimSuffix(a.path, ".active")
	if err := os.Rename(a.path, sealedPath); err != nil {
		return err
	}
	if err := s.dirF.Sync(); err != nil {
		return err
	}
	seg, err := openSealed(sealedPath, a.id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.sealed = append(s.sealed, seg)
	s.active = nil
	s.mu.Unlock()
	// Close the fd only after the swap: in-flight readers hold mu.RLock for
	// their whole pread, so the Lock above drained them, and post-swap
	// readers route to the sealed mmap. A close error fails the triggering
	// Put even though the object is sealed and durable — harmless; a retried
	// Put dedups via Has.
	return a.f.Close()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/packstore.go packstore/packstore_test.go
git commit -m "feat(packstore): sealing and rotation — footer, rename, mmap swap"
```

---

### Task 8: WriteBatch

**Files:**
- Modify: `packstore/packstore.go`
- Test: `packstore/packstore_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `packstore/packstore_test.go` (add imports `"fmt"`, `"iter"`):

```go
func objSeq(objs []Object, failAfter int) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		for i, o := range objs {
			if failAfter >= 0 && i == failAfter {
				yield(Object{}, fmt.Errorf("synthetic iterator error"))
				return
			}
			if !yield(o, nil) {
				return
			}
		}
	}
}

func TestWriteBatchStoresAll(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 100)
	// Duplicate some objects within the batch; they must be written once.
	batch := append(append([]Object{}, objs...), objs[:10]...)
	if err := s.WriteBatch(objSeq(batch, -1)); err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
}

func TestWriteBatchIteratorError(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 10)
	err := s.WriteBatch(objSeq(objs, 5))
	if err == nil || err.Error() != "synthetic iterator error" {
		t.Fatalf("err = %v", err)
	}
	// The already-appended prefix may remain (documented packstore semantics:
	// durable-on-return, not atomic; valid CAS objects are harmless).
	for _, o := range objs[:5] {
		if has, _ := s.Has(o.Key); !has {
			t.Fatalf("prefix object %s lost", o.Key)
		}
	}
}

func TestWriteBatchRotates(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, WithSegmentSize(16<<10))
	objs := testObjects(t, 100)
	if err := s.WriteBatch(objSeq(objs, -1)); err != nil {
		t.Fatal(err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) == 0 {
		t.Fatal("expected at least one sealed segment")
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `WriteBatch` undefined.

- [ ] **Step 3: Implement `WriteBatch` in `packstore/packstore.go`**

Add import `"iter"`.

```go
// WriteBatch stores every object the iterator yields, fsyncing once at the
// end (when WithSync is enabled): on return, all yielded objects are durable.
// Unlike diskstore's WriteBatch it is NOT atomic — a crash or iterator error
// can leave a valid prefix stored. In a content-addressed store that prefix
// is harmless: identical re-pushed content deduplicates. Objects repeated
// within the batch, or already present, are written once. When WriteBatch
// returns an error after appending part of the batch, it best-effort fsyncs
// that prefix first, so Has-visible records never stay non-durable.
func (s *Store) WriteBatch(seq iter.Seq2[Object, error]) error {
	seen := make(map[key.Key]struct{})
	appended := false
	fail := func(err error) error {
		if appended {
			s.syncActive() // best-effort; an fsync failure poisons the store
		}
		return err
	}
	for obj, err := range seq {
		if err != nil {
			return fail(err)
		}
		if _, dup := seen[obj.Key]; dup {
			continue
		}
		seen[obj.Key] = struct{}{}
		has, err := s.Has(obj.Key)
		if err != nil {
			return fail(fmt.Errorf("exists (%s): %w", obj.Key, err))
		}
		if has {
			continue
		}
		rec, err := encodeRecord(obj.Key, obj.Data)
		if err != nil {
			return fail(err)
		}
		if err := s.append(obj.Key, rec, false); err != nil {
			return fail(err)
		}
		appended = true
	}
	return s.syncActive()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/packstore.go packstore/packstore_test.go
git commit -m "feat(packstore): WriteBatch — durable-on-return batched ingest"
```

---

### Task 9: Missing

**Files:**
- Create: `packstore/missing.go` (mirror of `diskstore/missing.go`)
- Test: `packstore/missing_test.go`

- [ ] **Step 1: Write the failing test**

Create `packstore/missing_test.go`:

```go
package packstore

import (
	"slices"
	"testing"

	"github.com/draganm/amber-store/key"
)

func TestMissing(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSegmentSize(8<<10)) // force sealed + active mix
	objs := testObjects(t, 200)
	stored, absent := objs[:120], objs[120:]
	for _, o := range stored {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	// Interleave present and absent keys, with a duplicate absent key.
	var query []key.Key
	for i := range absent {
		query = append(query, stored[i%len(stored)].Key, absent[i].Key)
	}
	query = append(query, absent[0].Key) // duplicate, must be reported twice

	got, err := s.Missing(query)
	if err != nil {
		t.Fatal(err)
	}
	var want []key.Key
	for _, o := range absent {
		want = append(want, o.Key)
	}
	want = append(want, absent[0].Key)
	if !slices.Equal(got, want) {
		t.Fatalf("Missing: got %d keys, want %d (order and multiplicity preserved)", len(got), len(want))
	}
}

func TestMissingEmptyInput(t *testing.T) {
	s := openStore(t, t.TempDir())
	got, err := s.Missing(nil)
	if err != nil || got != nil {
		t.Fatalf("Missing(nil) = %v, %v", got, err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `Missing` undefined.

- [ ] **Step 3: Implement `packstore/missing.go`**

This is `diskstore/missing.go` with the package name changed — the logic is
already store-agnostic (it only calls `s.Has`):

```go
package packstore

import (
	"fmt"
	"runtime"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// minMissingChunk caps how many goroutines a small key list spawns: a chunk
// never holds fewer keys than this, so the goroutine overhead stays amortized.
const minMissingChunk = 64

const maxParallel = 16

// Missing reports which of keys are absent from the store, preserving the
// input's order and multiplicity. Lookups run concurrently over contiguous
// chunks of the input.
func (s *Store) Missing(keys []key.Key) ([]key.Key, error) {
	workers := runtime.GOMAXPROCS(0)
	if m := (len(keys) + minMissingChunk - 1) / minMissingChunk; m < workers {
		workers = m
	}
	if workers == 0 {
		return nil, nil
	}
	chunkLen := (len(keys) + workers - 1) / workers
	results := make([][]key.Key, workers)

	eg := &errgroup.Group{}
	eg.SetLimit(maxParallel)

	for i := range workers {
		// Both bounds clamp: with many workers the rounded-up chunkLen can
		// push a late worker's window past the end of keys.
		lo := min(i*chunkLen, len(keys))
		chunk := keys[lo:min((i+1)*chunkLen, len(keys))]
		eg.Go(func() error {
			var miss []key.Key
			for _, k := range chunk {
				has, err := s.Has(k)
				if err != nil {
					return fmt.Errorf("missing-check %s: %w", k, err)
				}
				if !has {
					miss = append(miss, k)
				}
			}
			results[i] = miss
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var missing []key.Key
	for _, r := range results {
		missing = append(missing, r...)
	}
	return missing, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/missing.go packstore/missing_test.go
git commit -m "feat(packstore): Missing — parallel absent-key checks"
```

---

### Task 10: Write-time verify + WriteParallel

**Files:**
- Create: `packstore/verify.go` (the `verifyObject` half; the scrub half lands in Task 11)
- Create: `packstore/parallel.go`
- Test: `packstore/parallel_test.go`

- [ ] **Step 1: Write the failing tests**

Create `packstore/parallel_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `WriteParallel`, `WriteOpts`, `ErrVerify` undefined.

- [ ] **Step 3: Implement `packstore/verify.go` (write-time half)**

Identical semantics to `diskstore/verify.go`:

```go
package packstore

import (
	"errors"
	"fmt"

	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// ErrVerify is returned (wrapped) when an object's key does not match its
// payload. Callers distinguish it with errors.Is to map to a client error.
var ErrVerify = errors.New("packstore: object verification failed")

// verifyObject recomputes o.Key from o.Data and reports ErrVerify on mismatch.
// For Blob and XattrSet — whose key length is the serialized byte length — it
// also checks the length field. Aggregate types (FileNode/DirLeaf/DirNode)
// carry a logical length the store cannot recompute without parsing, so only
// their hash is checked.
func verifyObject(o Object) error {
	sum := blake3.Sum256(o.Data)
	want, err := key.NewFromHash(o.Key.Type(), o.Key.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
	}
	if want != o.Key {
		return fmt.Errorf("%w: payload hashes to %s, not %s", ErrVerify, want, o.Key)
	}
	switch o.Key.Type() {
	case key.Blob, key.XattrSet:
		if o.Key.Length() != uint64(len(o.Data)) {
			return fmt.Errorf("%w: %s length field %d != payload %d", ErrVerify, o.Key, o.Key.Length(), len(o.Data))
		}
	}
	return nil
}
```

- [ ] **Step 4: Implement `packstore/parallel.go`**

Mirrors `diskstore/parallel.go`; workers do the CPU-heavy work (verify +
compression in `encodeRecord`) concurrently, while `Store.append` serializes
the actual byte appends. Batched fsyncs replace Pebble batch commits.

```go
package packstore

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize is the byte threshold at which a writer fsyncs the active
// segment, making everything appended so far durable.
const DefaultBatchSize = 16 << 20 // 16 MiB

// WriteStats summarizes one WriteParallel run.
type WriteStats struct {
	Stored      int   // objects newly written
	Deduped     int   // objects skipped (already present, or duplicated in the stream)
	BytesStored int64 // payload bytes of newly-written objects (uncompressed)
}

// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int  // concurrent writers; <= 0 means GOMAXPROCS
	BatchSize int  // fsync when a writer has appended this many bytes; <= 0 means DefaultBatchSize
	Verify    bool // recompute and check each new object's key before storing it
}

// WriteParallel stores every object the iterator yields using multiple
// concurrent workers. Compression and (optional) verification run in
// parallel; appends serialize on the active segment. Each worker fsyncs after
// appending BatchSize bytes and once more when the input is exhausted.
//
// Like WriteBatch, WriteParallel is durable-on-return but NOT atomic: on
// error or crash a valid prefix remains, which a content-addressed re-run
// deduplicates. If the iterator yields an error, WriteParallel stops and
// returns it. With opts.Verify, a key/payload mismatch stops the run with a
// wrapped ErrVerify.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error) {
	writers := opts.Writers
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan Object, writers*2)
	seen := newSeenSet()
	eg := &errgroup.Group{}
	var stored, deduped, bytesStored atomic.Int64

	// Distributor: forward objects from the iterator to the worker pool. A
	// yielded error cancels the run and propagates as the group's error.
	eg.Go(func() error {
		defer close(ch)
		for obj, err := range seq {
			if err != nil {
				cancel()
				return err
			}
			select {
			case ch <- obj:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	for range writers {
		eg.Go(func() error {
			err := s.runWriter(ctx, ch, seen, batchSize, opts.Verify, &stored, &deduped, &bytesStored)
			if err != nil {
				cancel() // stop the distributor and sibling workers
			}
			return err
		})
	}

	err := eg.Wait()
	return WriteStats{
		Stored:      int(stored.Load()),
		Deduped:     int(deduped.Load()),
		BytesStored: bytesStored.Load(),
	}, err
}

// runWriter consumes objects, encoding (compressing, optionally verifying)
// them concurrently with its siblings and appending them to the store. It
// fsyncs after batchSize appended bytes and once more when the channel
// closes. On ctx cancellation it returns without a final fsync (the run is
// being aborted; partial appends are safe in a content-addressed store).
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int, verify bool, stored, deduped, bytesStored *atomic.Int64) error {
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		pending = 0
		return s.syncActive()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case obj, ok := <-ch:
			if !ok {
				return flush()
			}
			if !seen.addIfAbsent(obj.Key) {
				deduped.Add(1)
				continue
			}
			has, err := s.Has(obj.Key)
			if err != nil {
				return err
			}
			if has {
				deduped.Add(1)
				continue
			}
			if verify {
				if err := verifyObject(obj); err != nil {
					return err
				}
			}
			rec, err := encodeRecord(obj.Key, obj.Data)
			if err != nil {
				return err
			}
			if err := s.append(obj.Key, rec, false); err != nil {
				return err
			}
			stored.Add(1)
			bytesStored.Add(int64(len(obj.Data)))
			pending += len(rec)
			if pending >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// seenSet is a concurrency-safe set of keys, sharded on the key's last byte
// (uniformly distributed) to spread lock contention across writers.
type seenSet struct {
	shards [256]seenShard
}

type seenShard struct {
	mu sync.Mutex
	m  map[key.Key]struct{}
}

func newSeenSet() *seenSet {
	s := &seenSet{}
	for i := range s.shards {
		s.shards[i].m = make(map[key.Key]struct{})
	}
	return s
}

// addIfAbsent records k and reports true if it was not already present.
func (s *seenSet) addIfAbsent(k key.Key) bool {
	sh := &s.shards[k[key.Size-1]]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[k]; ok {
		return false
	}
	sh.m[k] = struct{}{}
	return true
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1 -race`
Expected: PASS, race-clean (the `-race` run matters here: it exercises
concurrent append/Has/seal interleavings).

- [ ] **Step 6: Commit**

```bash
git add packstore/verify.go packstore/parallel.go packstore/parallel_test.go
git commit -m "feat(packstore): WriteParallel with parallel compression and verify"
```

---

### Task 11: Verify (scrub)

**Files:**
- Modify: `packstore/verify.go`
- Test: `packstore/verify_test.go`

Scrub contract (spec): walk each sealed segment's body; CRC every record;
re-BLAKE3 every payload against its key; recompute the index section and
compare bytewise; check the filter contains every body key (fuse filters have
no false negatives — a bytewise filter compare is impossible because
construction is seed-dependent); cross-check `keyCount` and `bodyLen`.

- [ ] **Step 1: Write the failing tests**

Create `packstore/verify_test.go`:

```go
package packstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// sealedStore builds a store with sealed segments and returns its dir.
func sealedStore(t *testing.T, objs []Object) string {
	t.Helper()
	dir := t.TempDir()
	s := openStore(t, dir, WithSegmentSize(8<<10))
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestVerifyCleanStore(t *testing.T) {
	dir := sealedStore(t, testObjects(t, 100))
	s := openStore(t, dir)
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDetectsBodyCorruption(t *testing.T) {
	dir := sealedStore(t, testObjects(t, 100))

	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) == 0 {
		t.Fatal("no sealed segments")
	}
	b, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	// Flip one payload byte inside the body, far from the footer: Open's
	// footer CRC does not cover the body, so this must surface in Verify.
	b[100] ^= 0x01
	if err := os.WriteFile(segs[0], b, 0o644); err != nil {
		t.Fatal(err)
	}

	s := openStore(t, dir) // Open succeeds: footer is intact
	if err := s.Verify(context.Background()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Verify = %v, want ErrCorrupt", err)
	}
}

func TestVerifyDetectsWrongIndexEntry(t *testing.T) {
	// Craft a segment whose footer is internally consistent (valid CRC) but
	// whose index lies about an offset — the writer-bug class that only a
	// body/index cross-check can catch.
	objs := testObjects(t, 20)
	var body []byte
	body = append(body, magicHeader...)
	var entries []indexEntry
	for _, o := range objs {
		rec, err := encodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{k: o.Key, off: uint64(len(body)), slen: uint32(len(rec) - recHeaderSize)})
		body = append(body, rec...)
	}
	entries[3].off = entries[2].off // lie
	footer, err := buildFooter(int64(len(body)), entries)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000001.seg")
	if err := os.WriteFile(path, append(body, footer...), 0o644); err != nil {
		t.Fatal(err)
	}

	s := openStore(t, dir)
	if err := s.Verify(context.Background()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Verify = %v, want ErrCorrupt", err)
	}
}

func TestVerifyHonorsContext(t *testing.T) {
	dir := sealedStore(t, testObjects(t, 100))
	s := openStore(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Verify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify = %v, want context.Canceled", err)
	}
}

func TestVerifyIgnoresActiveSegment(t *testing.T) {
	// Active-segment records are covered by reopen tail-scans, not Verify;
	// Verify only walks sealed segments. This test pins that behaviour: a
	// store with only an active segment verifies clean.
	dir := t.TempDir()
	s := openStore(t, dir)
	for _, o := range testObjects(t, 5) {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}

```

(The test file imports `context`, `errors`, `os`, `path/filepath`, `testing` —
no `bytes`; keep imports tidy with goimports.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./packstore/ -count=1`
Expected: compile error — `Verify` undefined.

- [ ] **Step 3: Implement the scrub in `packstore/verify.go`**

Add imports: `"bytes"`, `"context"`.

```go
// Verify scrubs every sealed segment: walks the body record by record
// (validating framing, CRCs, and that each payload re-hashes to its key),
// recomputes the index section and compares it bytewise with the footer's,
// and checks the filter contains every body key. The active segment is
// covered by tail-scan on reopen, not by Verify.
func (s *Store) Verify(ctx context.Context) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	segs := make([]*sealedSegment, len(s.sealed))
	copy(segs, s.sealed)
	s.mu.RUnlock()

	for _, seg := range segs {
		if err := seg.verify(ctx); err != nil {
			return err
		}
	}
	return nil
}

// verify scrubs one sealed segment. The segment is immutable and the caller
// holds no locks: scrubbing runs concurrently with reads and writes.
func (g *sealedSegment) verify(ctx context.Context) error {
	var entries []indexEntry
	off := int64(len(magicHeader))
	for off < g.fv.bodyLen {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := parseRecord(g.mm[off:g.fv.bodyLen])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		payload, err := decodePayload(rec.flags, rec.ulen, g.mm[off+recHeaderSize:off+recHeaderSize+int64(rec.slen)])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		if err := verifyObject(Object{Key: rec.key, Data: payload}); err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		if !g.fv.filter.Contains(filterKey(rec.key)) {
			return fmt.Errorf("%w: %s: filter missing key %s (offset %d)", ErrCorrupt, g.path, rec.key, off)
		}
		entries = append(entries, indexEntry{k: rec.key, off: uint64(off), slen: rec.slen})
		off += recHeaderSize + int64(rec.slen)
	}
	if off != g.fv.bodyLen {
		return fmt.Errorf("%w: %s: records end at %d, trailer says %d", ErrCorrupt, g.path, off, g.fv.bodyLen)
	}
	if uint64(len(entries)) != g.fv.keyCount {
		return fmt.Errorf("%w: %s: body has %d records, trailer says %d", ErrCorrupt, g.path, len(entries), g.fv.keyCount)
	}
	rebuilt := buildIndexSection(entries)
	stored := g.mm[g.fv.indexOff : g.fv.indexOff+g.fv.indexLen]
	if !bytes.Equal(rebuilt, stored) {
		return fmt.Errorf("%w: %s: index section does not match body", ErrCorrupt, g.path)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./packstore/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packstore/verify.go packstore/verify_test.go
git commit -m "feat(packstore): Verify scrub — record CRCs, payload hashes, index/filter cross-check"
```

---

### Task 12: Property test against a map oracle + final sweep

**Files:**
- Create: `packstore/oracle_test.go`

- [ ] **Step 1: Write the property test**

Create `packstore/oracle_test.go`:

```go
package packstore

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"

	"github.com/draganm/amber-store/key"
)

// TestOracle drives a store through random Put/WriteBatch/reopen cycles with
// a small rotation threshold and cross-checks every observable (Get, Has,
// Missing, Verify) against an in-memory map.
func TestOracle(t *testing.T) {
	dir := t.TempDir()
	r := rand.New(rand.NewPCG(1, 2))
	oracle := make(map[key.Key][]byte)

	newObj := func() Object {
		n := 1 + r.IntN(4000)
		data := make([]byte, n)
		if r.IntN(2) == 0 {
			for i := range data {
				data[i] = byte(r.Uint64()) // incompressible
			}
		} else {
			for i := range data {
				data[i] = byte(i % 7) // compressible
			}
		}
		return blobObj(t, data)
	}

	s := openStore(t, dir, WithSegmentSize(16<<10), WithSync(false))
	for round := 0; round < 20; round++ {
		switch r.IntN(3) {
		case 0: // single puts
			for i := 0; i < 20; i++ {
				o := newObj()
				if err := s.Put(o.Key, o.Data); err != nil {
					t.Fatal(err)
				}
				oracle[o.Key] = o.Data
			}
		case 1: // a batch with duplicates
			var objs []Object
			for i := 0; i < 30; i++ {
				o := newObj()
				objs = append(objs, o, o)
				oracle[o.Key] = o.Data
			}
			if err := s.WriteBatch(objSeq(objs, -1)); err != nil {
				t.Fatal(err)
			}
		case 2: // reopen (exercises seal-survival + tail-scan resume)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, dir, WithSegmentSize(16<<10), WithSync(false))
		}
	}

	// Full readback.
	var present []key.Key
	for k, want := range oracle {
		got, err := s.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(%s): %v", k, err)
		}
		present = append(present, k)
	}

	// Absent probes: compressible test data is a pure function of its length,
	// so fresh keys CAN collide with stored ones (and with each other) — skip
	// stored keys and dedupe, or Missing's exact answers look like failures.
	absentSet := make(map[key.Key]struct{})
	var absent []key.Key
	for len(absent) < 100 {
		k := newObj().Key
		if _, stored := oracle[k]; stored {
			continue
		}
		if _, dup := absentSet[k]; dup {
			continue
		}
		absentSet[k] = struct{}{}
		absent = append(absent, k)
	}

	// Missing cross-check.
	query := append(append([]key.Key{}, present...), absent...)
	miss, err := s.Missing(query)
	if err != nil {
		t.Fatal(err)
	}
	missSet := make(map[key.Key]int)
	for _, k := range miss {
		missSet[k]++
	}
	for _, k := range present {
		if missSet[k] != 0 {
			t.Fatalf("present key %s reported missing", k)
		}
	}
	for _, k := range absent {
		if missSet[k] != 1 {
			t.Fatalf("absent key %s not reported missing exactly once", k)
		}
	}

	// Structural scrub.
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run the full suite**

```bash
go test ./packstore/ -count=1 -race
gofmt -l packstore/
go vet ./packstore/
go test ./... > /dev/null && echo ALL-OK
```

Expected: packstore tests PASS race-clean; `gofmt -l` prints nothing; vet
clean; `ALL-OK` (no other package regressed — nothing else imports packstore
yet, this is the whole-repo sanity check).

- [ ] **Step 3: Commit**

```bash
git add packstore/oracle_test.go
git commit -m "test(packstore): randomized oracle test over put/batch/reopen cycles"
```

---

## Out of scope (v2, by design — see spec "Deferred to v2")

- Compactor, epoch-based reclaim, delete records (`tagDelete` is reserved but never written).
- Wiring packstore into the daemon/server as a diskstore alternative — separate plan once the store exists.
- Zero-copy read API.
