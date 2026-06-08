# `fstree` pack command design

Date: 2026-06-08

## Purpose

Implement a command-line tool that, for a given directory, traverses it
depth-first and builds the content-addressed Merkle/prolly tree described in
[architecture/fstree.md](../../../architecture/fstree.md) and
[architecture/types.md](../../../architecture/types.md), **streaming** every CAS
object (chunk) it produces into a **tar archive**. The tar is written to stdout
or to a file. Memory usage must stay low — processing is streaming, never holding
a whole file's content or a whole subtree resident.

It builds on the existing `key` package
([spec](2026-06-08-key-package-design.md)), which owns the 32-byte lookup key
format and BLAKE3 hashing.

- Command: `amber-store pack [options] DIR`
- New module deps: `github.com/fxamacker/cbor/v2`,
  `github.com/PlakarKorp/go-cdc-chunkers`, `github.com/urfave/cli/v2`,
  `golang.org/x/sys/unix`.

## Decisions (locked)

1. **Entry-type scope: full POSIX + xattrs.** Regular files, directories,
   symlinks, character/block devices, fifos, sockets; extended attributes stored
   inline (DirLeaf key 8) or spilled to an `XattrSet` (key 9) above a threshold.
2. **Root surfacing: root is the last member of the tar.** The final tar
   member's name is the hex of the root directory key. The root key is also
   echoed to stderr as a convenience; the last-member rule is authoritative.
3. **Deduplication: on.** Each unique key's chunk is written to the tar exactly
   once. Seen keys are tracked in an in-memory `map[key.Key]struct{}`
   (~32 bytes per unique object).
4. **Error handling: fail fast.** Any traversal/read/encode/write error aborts
   the run with a non-zero exit; no partial tree is emitted as "complete".

## Architecture

Depth-first traversal where processing one filesystem node yields a single
**content key**. That key, plus metadata from `lstat`, becomes one entry in the
parent directory's `DirLeaf`. The recursion *is* the streaming strategy: at any
moment only the current root-to-node path, the per-level promote buffers, the
single in-flight chunk, and the current directory's sorted name list are
resident — never a whole file's bytes or a whole subtree.

```
cmd/amber-store (urfave/cli/v2)
  └─ driver: pack(rootDir, sink)
       ├─ buildDir(path) → key.Key                 (recurses; depth-first)
       │     os.ReadDir(path)                       (already sorted bytewise by name)
       │     for each dirent (in order):
       │        lstat + read metadata (raw st_mode, uid, gid, mtime, rdev, xattrs)
       │        ├─ regular file → buildFile(path) → contentKey   (key 5)
       │        ├─ subdir       → buildDir(path)  → contentKey   (key 5)
       │        ├─ symlink      → inline target (readlink)       (key 6)
       │        ├─ chr/blk dev  → inline [major, minor] of rdev  (key 7)
       │        └─ fifo/socket  → no payload key
       │        xattrs → inline (key 8) if ≤ threshold, else XattrSet (key 9)
       │        sink the child content object (consumer-sinks rule, below)
       │     stream entries → ItemChunker → DirLeaf(s) → promote → DirNode(s) → root
       └─ buildFile(path) → key.Key
             open file; stream bytes → ultracdc → Blob(s); sink each Blob
             promote Blob keys via FileNode ItemChunker → FileNode levels → root
```

## Packages & units

### `fstree` — serialization, length rules, promotion

Pure (no filesystem I/O). Owns the on-the-wire CBOR layout and the length-field
semantics from types.md.

- **Encoders**, each returns `(serialized []byte, length uint64)` and the object
  key via `key.New(type, length, serialized)`:
  - `FileNode`: CBOR array of 32-byte child-key byte strings.
    `length = Σ childKey.Length()`.
  - `DirLeaf`: CBOR array of entry maps (integer keys 0–9), entries already
    sorted by name. `length = len(serialized) + Σ contentKey.Length()` (REG/DIR
    entries) `+ Σ xattrsKey.Length()` (entries with a spilled XattrSet).
  - `DirNode`: CBOR array of `[sepName, childKey]` pairs, sorted by `sepName`.
    `length = len(serialized) + Σ childKey.Length()`.
  - `XattrSet`: CBOR map `{name → value}`. `length = len(serialized)`.
  - `Blob`: identity (raw bytes). `length = len(bytes)`.
- **Deterministic CBOR**: `github.com/fxamacker/cbor/v2` with
  `cbor.CoreDetEncOptions()` (RFC 8949 §4.2: definite-length items, shortest-form
  integers/lengths, map keys sorted by encoded bytes ascending, no floats).
  Entry maps use `keyasint` integer keys; struct fields are also declared in
  ascending key order as belt-and-suspenders. A known-answer test pins the bytes.
- **`entry`** struct → canonical CBOR map. Fields: `0:name bstr`, `1:mode uint`
  (raw `st_mode`), `2:uid uint`, `3:gid uint`, `4:mtime int` (ns since epoch, may
  be negative), then exactly one of `5:contentKey`, `6:linkTarget`,
  `7:[major,minor]` (fifo/socket have none), and optionally one of `8:inline
  xattrs map` / `9:xattrsKey`.

### `chunkers` — content-defined chunking

- **`ByteChunker`** — thin wrapper over `go-cdc-chunkers` ultracdc. Input
  `io.Reader`, yields successive `[]byte` chunks (each becomes a `Blob`). Sizes
  default to the library's ultracdc options, overridable via CLI flags. An empty
  reader yields zero chunks; the caller emits one empty `Blob`.
- **`ItemChunker`** — boundary oracle for index/entry streams. `Boundary(enc
  []byte) bool` returns true when the low `k` bits of `BLAKE3(enc)` are zero
  (target average run = `2^k` items), bounded by a minimum and maximum run length
  to cap variance. The decision is evaluated only *between* whole items, so an
  item is never split across objects. Used for `FileNode`, `DirLeaf`, and
  `DirNode` streams.

### `fstree.promote` — generic bottom-up build

Given an ordered item stream, a `makeLeaf(items)` and a `makeNode(pairs)`
function, and an `ItemChunker`:

1. Chunk the item stream into leaf-level objects; sink each; collect a rising
   list of `(sepName, childKey)` pairs (for files, `sepName` is unused).
2. While more than one object remains at the current level, item-chunk the pairs
   into node objects; sink each; collect the next level's pairs.
3. The single remaining object is the **root**; return its `(key, serialized)`
   *without sinking it* (see consumer-sinks rule).

Each object tracks its **max name** so a parent can use it as `sepName`: a
`DirLeaf`'s is its last entry's name; a `DirNode`'s is its last child's
`sepName`. Buffers are O(items / 2^k) per level and shrink each level, so peak
memory is far below O(n).

### `castar` — dedup'ing tar sink

```go
type Sink interface {
    Put(k key.Key, data []byte) error   // dedup: skip if key already written
    PutRoot(k key.Key, data []byte) error // force-write as the final member
}
```

- Backed by `archive/tar`. Each member: name = `k.String()` (64 hex chars,
  USTAR-safe), fixed mode/uid/gid/mtime and `Typeflag = TypeReg` for byte-level
  determinism; body = `data`.
- `Put` consults a `map[key.Key]struct{}`; first write records and emits,
  repeats are skipped.
- `PutRoot` writes unconditionally (bypassing dedup) so the root is present and
  **last**.

### `cmd/amber-store` — CLI + traversal driver

`urfave/cli/v2` app, `pack` subcommand. Owns all filesystem I/O: `os.ReadDir`,
`os.Lstat`, `os.Readlink`, raw `Stat_t` (`st_mode`, uid, gid, rdev), and xattr
reads. Platform specifics (`*xattr`, device major/minor, raw stat) go through
`golang.org/x/sys/unix`, build-tagged for darwin and linux.

```
amber-store pack [options] DIR
  -o, --output FILE     write the tar here (default: stdout)
  --min, --avg, --max   ultracdc byte-chunk sizes (default: ultracdc lib defaults)
  --item-bits k         item-chunker average run = 2^k items (default 7 ≈ 128)
  --xattr-inline-max N  bytes; above this, an entry's xattrs spill to an XattrSet
                        (default 256)
```

On success: the tar holds all unique chunks with the root last; the root key
(hex) is printed to stderr. Any error → non-zero exit.

## The consumer-sinks rule (dedup + root-last, together)

Every object is written to the sink by **whoever consumes it into a parent**: a
child content object is `Put` when the parent's leaf chunker incorporates its
entry; an intermediate index object is `Put` when its level is promoted. The
global root has no consumer, so the driver calls `PutRoot` last. Consequences:

- Dedup is natural: an identical subtree consumed by two parents is `Put` twice
  and suppressed once.
- The root is guaranteed to be the final member, written exactly once.
- Pathological edge: if the root object is byte-identical to something deeper
  already written, `PutRoot` re-emits it as the last member — a harmless
  duplicate name in a content-addressed bag.

## Length-field computation (recap of types.md)

- `Blob.length` = raw byte count.
- `XattrSet.length` = serialized byte length.
- `FileNode.length` = Σ `childKey.length` (file content size; excludes own bytes).
- `DirNode.length` = own serialized bytes + Σ `childKey.length`.
- `DirLeaf.length` = own serialized bytes + Σ `contentKey.length` (REG/DIR) + Σ
  `xattrsKey.length` (spilled-xattr entries). Symlinks/devices/fifos/sockets add
  nothing beyond own bytes (their data is inline in the leaf).

There is no circularity: `len(serialized)` is independent of the key, and child
contributions come from already-built child keys.

## Edge cases

- **Empty directory** → one empty `DirLeaf` (CBOR `0x80`); it is the root.
- **Empty file** → one empty `Blob` (length 0); it is the root.
- **Single chunk** → the `Blob`/`DirLeaf` *is* the root (no promotion).
- **Symlink** → key 6, target from `readlink` (no-follow); mode = `S_IFLNK | …`.
- **Char/block device** → key 7, `[major, minor]` decoded from `rdev`.
- **fifo/socket** → none of keys 5/6/7.
- **Mode** is the raw POSIX `st_mode` from `Stat_t` (encodes type + perms), *not*
  Go's `fs.FileMode`.
- Entry order: `os.ReadDir` already sorts bytewise by name — exactly the spec's
  `DirLeaf` order.

## Memory profile

Resident at peak: O(tree depth) call frames × O(fanout) promote buffers, one
in-flight chunk, and the current directory's sorted name list. No file content
and no full subtree is ever held. A multi-GB file streams through ultracdc with
the child-key stream itself chunked into `FileNode` levels.

## Testing (TDD)

- **Deterministic-CBOR KATs**: pin the exact bytes of `FileNode`, `DirLeaf`
  (each payload variant 5/6/7/8/9), `DirNode`, `XattrSet`; guards on-disk format.
- **Length-field unit tests** for every object type, including the DirLeaf
  inline-vs-spilled-xattr contribution rule.
- **ItemChunker boundary determinism**: same item stream → same boundaries;
  min/max run bounds honored; an item is never split.
- **ByteChunker integration**: ultracdc wrapper streams a reader into the
  expected chunk sequence; empty reader → caller emits one empty Blob.
- **Promote**: single-item stream → that item is root (no node level);
  multi-level promotion produces correct `sepName`s and root.
- **Dedup**: two identical files → a single shared `Blob` set in the tar.
- **End-to-end**: construct a temp tree (regular files of varying size, a
  subdir, a symlink, controlled mode/uid/gid/mtime), `pack` it, read the tar
  back, assert the member set, that the root is the last member, and that a
  repeat run is byte-identical. A tiny in-test decoder walks the tree from the
  root key to verify structure (no shippable reader in scope).
- **Fail-fast**: an unreadable file aborts with non-zero exit and no truncated
  "success".

## Out of scope

- A reader/`unpack`/verify command (tests include only a minimal in-test
  decoder).
- Snapshot/versioning objects and root-directory metadata (per types.md, the
  root carries none for now).
- Hard-link detection / inode coalescing (each path is treated independently).
- Cross-platform support beyond darwin and linux.
