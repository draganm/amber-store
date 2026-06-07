# Amber-Store

A content-addressable store for **filesystem trees** — arbitrarily deep
directories and files, where file content is split by content-defined chunking and
every object is identified by a fixed 32-byte key derived from a hash of its
content.

> **Status:** architecture/design phase. The data model is specified in
> [`architecture/`](architecture/); there is no implementation yet.

## What it is

Amber-Store models a POSIX-style filesystem as an immutable, deduplicated
[Merkle](https://en.wikipedia.org/wiki/Merkle_tree) structure in a content-addressed
store:

- **Files** are split into chunks by content-defined chunking (CDC); each chunk is a
  `Blob`. Large files get a multi-level `FileNode` index for O(log n) random-access
  seek.
- **Directories** are [prolly trees](https://docs.dolthub.com/architecture/storage-engine/prolly-tree)
  — sorted maps from name to entry, chunked at entry boundaries — so a directory
  with **>100K entries can be looked up, iterated, and mutated with sub-O(n)
  memory**.
- **Metadata** (type, mode, uid, gid, mtime, optional xattrs) lives in the parent
  directory entry. Symlinks and special files are stored inline; regular files and
  subdirectories reference content by key.
- **Deduplication is automatic**: identical content yields an identical key, and
  editing one entry re-writes only the O(log n) objects on its path — the rest is
  structurally shared with the previous tree.

## Design goals

- Very large directories (>100K entries) processed with **less than O(n)** memory.
- Directory entries carry all filesystem metadata; file content is reached through
  the store by key.
- Deterministic, implementation-independent encoding so any reader recomputes the
  same key for the same content.

## Object types

| Type       | Role                                                        |
|------------|-------------------------------------------------------------|
| `Blob`     | Raw file-content byte chunk (a CDC leaf).                   |
| `FileNode` | File chunk-index node (file content tree).                  |
| `DirLeaf`  | A run of complete directory entries (prolly-tree leaf).     |
| `DirNode`  | Directory index node (directory tree).                      |
| `XattrSet` | Spilled extended attributes, when too large to inline.      |

## Encoding

- **Keys** are 32 bytes: a type/length header plus a truncated
  [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) hash of the content.
- **Structured objects** use **deterministic CBOR**
  ([RFC 8949 §4.2](https://www.rfc-editor.org/rfc/rfc8949#name-deterministically-encoded-c));
  `Blob`s are raw bytes. Deterministic encoding is required because the key's hash
  is taken over the serialized bytes.

## Architecture

| Document | Contents |
|----------|----------|
| [`architecture/keys.md`](architecture/keys.md)   | The 32-byte lookup key: header byte, payload length, truncated hash. |
| [`architecture/types.md`](architecture/types.md) | The type model: object types, filesystem entry types, length-field semantics. |
| [`architecture/fstree.md`](architecture/fstree.md) | On-the-wire CBOR layout of every type, the chunkers, tree construction, and read paths. |

## Development

The repository uses a [Nix](https://nixos.org/) flake (with
[direnv](https://direnv.net/)) to provide the Go toolchain.

```sh
direnv allow        # or: nix develop
go build ./...
```

- Module: `github.com/draganm/amber-store`
- Go: 1.26+
