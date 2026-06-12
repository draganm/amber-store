# packstore — log-structured segment (pack file) persistence

**Status:** design approved 2026-06-13.

## Purpose

A second on-disk CAS store, parallel to `diskstore`, that persists objects in
large append-only **segment files** (pack files) instead of Pebble + per-object
blob files. The store directory contains *only* segment files: there is no
embedded database. All lookup structures (per-segment index, membership filter)
live inside each sealed segment's footer; everything in RAM is rebuilt from
footers at `Open`.

v1 is **seal-and-forget**: segments are written, sealed, and read. The on-disk
format reserves what compaction and GC need (delete-record tag, per-file
self-contained indexes) so the v2 compactor requires no format change.

## Interface

Drop-in surface of `diskstore`:

```go
package packstore

type Object struct {
	Key  key.Key
	Data []byte
}

func Open(dir string, opts ...Option) (*Store, error)
func (s *Store) Put(k key.Key, data []byte) error
func (s *Store) WriteBatch(seq iter.Seq2[Object, error]) error
func (s *Store) WriteParallel(ctx context.Context, seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error)
func (s *Store) Get(k key.Key) ([]byte, error)
func (s *Store) Has(k key.Key) (bool, error)
func (s *Store) Missing(keys []key.Key) ([]key.Key, error)
func (s *Store) Verify(ctx context.Context) error // full-store scrub
func (s *Store) Close() error
```

Options:

- `WithSync(bool)` — fsync on each `Put`/batch commit. Default `true`.
- `WithSegmentSize(n int64)` — rotation threshold. Default 256 MiB. A segment
  seals when its size reaches `n` (checked between records, so one oversized
  record may exceed it).

`Get` returns `ErrNotFound` for an absent key. `Get` always returns
caller-owned bytes (copied or decompressed out of the mmap) — no mmap-backed
slice ever escapes the package.

## Decisions of record

- **mmap**: sealed segments are mmap'd whole; footer index and filter are read
  in place. The active segment uses buffered writes + pread.
- **Index**: git-idx-style fanout + sorted entries in each sealed segment's
  footer, fanout on the **last** key byte (byte 0 is type/length-size and
  clusters; the hash tail is uniform — same reason diskstore shards blobs on
  `k[30..32]`).
- **Filter**: one binary fuse filter (16-bit fingerprints) per sealed segment,
  held in RAM; no global index of any kind, on disk or otherwise.
- **Compression**: per-record zstd, store-if-smaller, flagged per record.
- **Endianness**: all fixed-width integers in the format are **big-endian**.
  Keys are embedded as opaque 32-byte strings.
- **Durability**: record-level. A chunk exists once its record is fsynced in
  the active segment (bitcask-style); recovery tail-scans the active segment.

## On-disk layout

```
<dir>/
  0000000000000001.seg          sealed — immutable forever, mmap'd whole
  0000000000000002.seg          sealed
  0000000000000003.seg.active   the single active segment (append-only)
```

- Filenames: zero-padded lowercase hex of a monotonic u64 segment ID. v2
  compaction outputs take fresh IDs.
- At most one `.seg.active` exists. Sealing = append footer, fsync, rename to
  `.seg` (atomic), fsync directory.
- Single-writer exclusion: `flock(LOCK_EX | LOCK_NB)` on the **directory fd**.
  No lock file — the directory holds only segments.
- `Close()` does **not** seal: it fsyncs and leaves `.seg.active`; reopening
  resumes appending after a tail-scan. This prevents small-segment
  proliferation across daemon restarts (v1 has no compactor to merge them).

## Segment format

```
header   "AMBERSG\x01"   8 bytes — magic + format version
records  record*          body
footer                    sealed segments only
```

(The magic is a sibling of `amberpack`'s `"AMBERPK\x01"` wire format —
different artifact, different magic.)

### Record

```
tag      u8        0x01 = chunk record
key      [32]byte  opaque key.Key bytes
flags    u8        bit 0 = payload is zstd-compressed; other bits must be 0
ulen     u32 BE    uncompressed payload length
slen     u32 BE    stored payload length (== ulen when raw)
crc      u32 BE    CRC-32C of the whole record (header with crc field zeroed, then payload)
payload  [slen]byte
```

- Header is 46 bytes; records are unpadded (payloads need no alignment, and
  header fields are decoded with explicit BE reads).
- The CRC covers the header so a corrupted `slen` cannot desync a tail-scan.
- `ulen` is explicit because aggregate types' key length field is logical, not
  serialized length; it also lets decompression preallocate exactly.
- u32 lengths cap one object at 4 GiB.
- Reserved tags: `0x02` = delete record (v2 GC; key only, no payload),
  `0xF0` = seal marker (start of footer, not a record).

### Compression

On write, compress the payload with zstd (`klauspost/compress/zstd`, pure Go);
keep the compressed form only if strictly smaller, else store raw with flag
clear. Raw payloads are served straight from the mmap (one copy into the
caller's buffer); compressed payloads decode into a fresh `ulen`-sized buffer.

### Footer

```
seal marker  u8   0xF0
index section
filter section
trailer      64 bytes — always the last 64 bytes of the file
```

**Trailer** (read first — O(1) footer discovery, no record iteration):

```
indexOff   u64 BE   file offset of index section
indexLen   u64 BE
filterOff  u64 BE   file offset of filter section
filterLen  u64 BE
keyCount   u64 BE   number of records == index entries
bodyLen    u64 BE   offset where records end == offset of the seal marker
footerCRC  u32 BE   CRC-32C over [bodyLen, EOF-16) — seal marker, index, filter,
                    and the first 48 trailer bytes; excludes itself, reserved, magic
reserved   u32 BE   must be 0 (checked explicitly)
magic      [8]byte  "AMBERSGF" (checked explicitly)
```

**Index section**:

```
fanout   256 × u32 BE                cumulative: fanout[b] = #entries with key[31] <= b
entries  keyCount × 44 bytes:
           key   [32]byte
           off   u64 BE              file offset of the record header
           slen  u32 BE              stored payload length (readahead hint;
                                     the record header stays authoritative)
```

Entries are sorted by `(key[31], full key lexicographic)`. Lookup:
`b = k[31]`, binary search `entries[lo:fanout[b]]` on full key, where
`lo = fanout[b-1]` for `b > 0` and `0` for `b == 0`.

**Filter section** — binary fuse filter over the segment's key set:

```
filterType   u8       1 = binary fuse, 16-bit fingerprints
seed         u64 BE
segLen       u32 BE   ┐
segLenMask   u32 BE   │  binary-fuse construction parameters
segCount     u32 BE   │  (FastFilter/xorfilter)
segCountLen  u32 BE   ┘
fpCount      u32 BE
fingerprints fpCount × u16 BE
```

Filter input per key: `BigEndian.Uint64(key[24:32])` — the uniform hash tail.
(Tail collisions inside one segment are birthday-over-2^64, negligible; the
builder dedupes regardless.) ~18 bits/key, ~0.0015 % false positives: an
absent-key probe (the `Missing` hot path) almost never costs a wasted binary
search even across thousands of segments.

The index and filter are derived data — both rebuildable by scanning the body.
Scrub recomputes and cross-checks them. Divergence is per-file and
self-contained; there is no global index to drift.

## Runtime behaviour

### Open

1. `flock` the directory fd; fail if held.
2. For every `*.seg`: mmap whole, validate header magic and trailer
   (magic + footerCRC), copy the filter's fingerprints into RAM (BE→native
   swap), keep the index as an mmap'd byte slice. A sealed segment that fails
   validation **aborts Open** — sealed files are immutable, so corruption is a
   real event, never skipped silently.
3. The `*.seg.active` (at most one) gets a tail-scan from offset 8: parse each
   record header, bounds-check, verify CRC, `key.Validate()`, and build the
   in-RAM `map[key.Key]location`. First invalid or truncated record ⇒ truncate
   the file there; appending resumes after it. A `0xF0` marker with a valid
   trailer means the crash hit between footer-write and rename: complete the
   rename — it is sealed.
4. No active segment ⇒ create the next-numbered one lazily on first write.

RAM at steady state: per-segment metadata + fuse filters (~18 bits/key — 10 M
chunks ≈ 22 MB) + the active segment's map. Open cost scales with footers, not
bodies.

### Write path

- Dedup first (`Has`, exact); existing keys are skipped, as in diskstore.
- Compression and optional verify (BLAKE3 recompute, `WriteOpts.Verify`) run
  **outside** the lock; the append — reserve offset, write record, update the
  active map — holds a single mutex on the active segment.
- `WithSync(true)`: one fsync per `Put`/batch commit before returning; that
  fsync is the existence commit point.
- `WriteBatch` is durable-on-return but **not atomic**: a crash mid-batch
  persists a valid prefix. Harmless in a CAS — orphaned chunks are future
  dedup hits; references commit elsewhere only after a push completes. (This
  is the one documented semantic difference from diskstore.)
- Rotation: after a batch, if active size ≥ threshold, seal — build index and
  filter from the active map (no body re-read), append footer, fsync, rename,
  fsync dir, mmap the sealed file, drop the map.

### Read path

- `Get`/`Has`/`Missing`: check the active map first, then probe sealed
  segments' fuse filters newest→oldest; on filter hit, fanout + binary search
  that segment's mmap'd index. Filters are probabilistic; the index search is
  exact, so results never lie.
- The same key may legally appear in several segments (e.g. crash-recovered
  prefix re-pushed); first hit wins — contents are identical by construction.
- Concurrency: store-level `RWMutex` — reads take RLock; seal/rotate/Close
  take the write lock. Sealed segments need no per-read locking (immutable
  mmap). Epoch-based reclaim is deferred to v2 alongside the compactor; v1
  only unmaps in `Close`.
- `Get` does not CRC-check on the hot path (same trust model as diskstore);
  bit-rot detection is scrub's job.

### Scrub (`Verify`)

Walk each sealed segment's body: check every record CRC, recompute index and
filter and compare bytewise with the footer, optionally re-BLAKE3 payloads
against their keys. Also validates that `keyCount`/`bodyLen` match reality.

## Error handling

- `ErrNotFound` for absent keys (mirrors diskstore).
- `ErrCorrupt` (wrapped, `errors.Is`-able) for sealed-segment validation
  failures at Open and scrub findings; messages name the segment file and
  offset.
- Tail-scan truncation at Open is not an error: Open succeeds, the invalid
  tail is discarded, and appending resumes at the truncation point.
- `ErrVerify` for write-time BLAKE3 mismatches, mirroring diskstore.

## Failure modes designed against

- **Crash mid-batch**: valid prefix survives (records are self-framing +
  CRC); tail garbage truncated at next Open.
- **Crash mid-seal**: footer incomplete ⇒ trailer CRC/magic invalid ⇒ the file
  is still `.active` and tail-scan treats the partial footer as garbage tail,
  truncates it, and the store re-seals later. Crash after footer but before
  rename ⇒ Open completes the rename.
- **Index/segment divergence**: impossible globally (no global index);
  per-file divergence is caught by scrub recomputing derived sections.
- **External truncation of a sealed file**: mmap reads can SIGBUS. Accepted:
  sealed segments are immutable by contract and never truncated by the store;
  trailer validation at Open catches size mismatches up front.

## Dependencies

- `github.com/FastFilter/xorfilter` (new, direct) — binary fuse filters.
- `github.com/klauspost/compress/zstd` (existing indirect → direct).
- `golang.org/x/sys/unix` (existing direct) — mmap, flock.

## Package shape

`packstore/` parallel to `diskstore/`:

```
packstore.go   Store, Open, Put, Get, Has, Close, options
record.go      record encode/decode
footer.go      index + filter + trailer encode/decode
recover.go     active-segment tail-scan
missing.go     Missing (parallel, mirrors diskstore)
parallel.go    WriteParallel (mirrors diskstore)
verify.go      write-time verify + Verify scrub
*_test.go      mirrors diskstore's suites
```

## Testing

- Round-trip units: record (raw + compressed + store-if-smaller boundary),
  footer (index + filter + trailer), fanout/binary-search edges (empty
  buckets, last byte 0x00 and 0xFF, single-entry segment).
- Store-level suite mirroring diskstore's (Put/Get/Has/WriteBatch/Missing/
  parallel ingest).
- Crash recovery: truncate and corrupt the active tail at **every byte offset**
  of a trailing record; assert the prefix survives and the store keeps
  working. Seal-crash variants: partial footer, footer-without-rename.
- Sealed corruption: flip bytes in body/index/filter/trailer; Open must fail
  loudly (trailer/CRC) or scrub must detect (body/index/filter).
- Property test: random objects through Put/WriteBatch/seal cycles against a
  map oracle, then full readback + Missing cross-check + Verify.

## Deferred to v2 (format already accommodates)

- Compactor: greedy-by-garbage victim selection, copy-live-to-new-segment,
  epoch-based unmap/unlink. Liveness comes from mark-and-sweep against the
  ref roots; per-segment dead-bytes are computed by intersecting the live set
  with segment indexes (no persistent counters needed).
- Delete records (tag `0x02`) for explicit tombstoning if GC needs them.
- Zero-copy read API (`ReadAt`-style) if profiling justifies it.
