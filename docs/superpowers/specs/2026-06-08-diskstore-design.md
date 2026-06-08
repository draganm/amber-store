# diskstore — on-disk content-addressable persistence

**Status:** design approved 2026-06-08.

## Purpose

Persist Amber-Store CAS objects on the local filesystem and read them back by
key. Each object is a `(key.Key, []byte)` pair; the key is supplied by the
caller (objects are produced upstream by the `fstree` encoders, which already
compute the correct structured key). Small objects are stored in an embedded
Pebble database; large objects (> 16 KiB) are stored as plain files on disk.

## Interface

```go
package diskstore

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}

func Open(dir string, opts ...Option) (*Store, error)
func (s *Store) Put(k key.Key, data []byte) error
func (s *Store) WriteBatch(seq iter.Seq2[Object, error]) error
func (s *Store) Get(k key.Key) ([]byte, error)
func (s *Store) Has(k key.Key) (bool, error)
func (s *Store) Close() error
```

Options:

- `WithInlineThreshold(n int)` — boundary between inline (Pebble) and external
  (file) storage. Default 16384 (16 KiB). Objects with `len(data) > n` go to a
  file; otherwise inline.
- `WithSync(bool)` — fsync writes for durability. Default `true`. Disabling
  speeds bulk loads and tests at the cost of crash durability.

`Get` returns `ErrNotFound` for an absent key.

## On-disk layout

```
<dir>/
  db/                       # Pebble's own files (MANIFEST, *.sst, WAL)
  blobs/<hh>/<hh>/<64-hex>  # one file per large object, named by key hex
  tmp/                      # staging for atomic rename (same filesystem as blobs/)
```

Large-blob files are sharded two levels deep on the **last two bytes** of the
key (`blobs/<hex k[30]>/<hex k[31]>/<k.String()>`). Those bytes lie in the
uniformly-distributed truncated-hash region, so shards stay balanced; the
leading bytes are type/length and would cluster.

## Pebble record format

Pebble is the single index — `Has`/`Get` consult only Pebble, and it is the
authority for existence and dedup.

- Pebble key = the raw 32 key bytes (`k[:]`).
- Value = a 1-byte tag followed by an optional payload:
  - `0x00` **inline** — the remaining bytes are the object data (objects ≤ threshold).
  - `0x01` **external** — no payload; the data lives in the blob file (objects > threshold).

## Operations

### Put

1. If `len(data) > threshold` (external):
   - Stage the bytes to a unique temp file in `tmp/` (`os.CreateTemp`), `fsync`
     it, close it.
   - If the target shard file does not already exist: create the shard dirs,
     `rename` the temp file into place, `fsync` the shard directory. If it
     already exists, discard the temp file (idempotent — identical content).
   - `Set(k, {0x01})` and commit (Sync per option).
2. Else (inline): `Set(k, 0x00 ‖ data)` and commit (Sync per option).

The external file is durable on disk before the Pebble marker that references
it is committed.

### WriteBatch

Strict all-or-nothing on producer success:

1. Open one Pebble batch and a per-batch `seen` set (dedup within the batch).
2. `for obj, err := range seq`:
   - If `err != nil`: stop immediately and return `err` **without committing** —
     nothing becomes visible. (Range-over-func signals stop to the producer.)
   - Skip if `obj.Key` is already in `seen`; otherwise record it.
   - External object: write its file durably now (as in Put, skipping if the
     file already exists); stage `Set(k, {0x01})` into the batch.
   - Inline object: stage `Set(k, 0x00 ‖ data)` into the batch.
3. On normal completion: commit the batch once (Sync per option) — the single
   atomic visibility point.

### Get

Pebble lookup by `k[:]`. Missing → `ErrNotFound`. Inline (`0x00`) → return a copy
of `value[1:]`. External (`0x01`) → read and return the shard file's bytes.

### Has

Pebble existence check for `k[:]`.

## Crash & atomicity semantics

- The Pebble batch/commit is atomic at the WAL level; with `WithSync(true)` it is
  fsync-durable.
- Every external file is fsync-durable (file + containing dir) before the marker
  that references it commits, so a committed external marker always has its bytes
  on disk.
- A crash (or a `WriteBatch` rollback) before commit can leave orphan blob files.
  They are invisible (no marker references them), harmless (content-addressed:
  identical content reproduces the identical file), and reclaimable by a future
  GC pass. A retry re-yields everything and converges, since the store is
  idempotent.

## Concurrency

Pebble is safe for concurrent readers and writers; external file writes use
unique temp names via `os.CreateTemp`, so concurrent writers do not collide.
Single-process only: Pebble locks `db/` for the lifetime of the `Store`.

## Dependency

Adds `github.com/cockroachdb/pebble` (embedded KV).

## Out of scope (this iteration)

Delete / garbage collection of unreferenced objects, hash verification of stored
content, and integration of the pack driver with this store as a sink.

## Testing

Temp-dir (`t.TempDir()`) tests:

- small object round-trip (`Put` → `Get`);
- large object (> 16 KiB) round-trip, and assert the file exists under `blobs/`;
- threshold boundary: 16384 inline (no file), 16385 external (file present);
- `Has` true / false;
- dedup: `Put` the same object twice → exactly one blob file;
- `WriteBatch` with a mix of small and large objects, all retrievable after commit;
- `WriteBatch` rollback: an iterator yielding an error mid-stream commits nothing
  (`Has` false for every key it yielded);
- persistence across `Close` / `Open`;
- empty data (`len == 0`).
