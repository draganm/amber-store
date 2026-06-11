# Amber-Store

A content-addressable store for **filesystem trees** — arbitrarily deep
directories and files, where file content is split by content-defined chunking and
every object is identified by a fixed 32-byte key derived from a hash of its
content.

> **Status:** working implementation. A store-owning daemon serves CLI clients
> over a unix socket; trees are built client-side and ingested, listed, dumped,
> and restored through it. The design is specified in
> [`architecture/`](architecture/).

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
| [`architecture/daemon.md`](architecture/daemon.md) | The daemon/CLI split: who builds trees, who stores objects, the unix-socket protocol, and the pack-write format. |
| [`architecture/references.md`](architecture/references.md) | Named pointers to keys: record layout, name rules, storage, routes. |
| [`architecture/remote.md`](architecture/remote.md) | The remote server (`amber-store serve`): identity, signed HTTP protocol, wire routes, sync algorithms. |

## Usage

One long-running daemon owns the store; every other command is a short-lived
client talking to it over a unix socket (see
[`architecture/daemon.md`](architecture/daemon.md)):

```sh
amber-store daemon --store /path/to/store      # run the store-owning daemon
```

Ingest a directory — the tree is built (walked, chunked, hashed) client-side
and streamed to the daemon; the root key (hex) is printed to stdout:

```sh
amber-store config-user alice              # once — who creates references
amber-store ingest backups/home ./some/dir # ingest + name the root
```

Inspect and export by key or reference, optionally addressing a subdirectory with
`KEY/PATH` or `ref:NAME[@PATH]`:

```sh
amber-store ls KEY[/PATH]                # list entries, ls -l style (--keys adds content keys)
amber-store content-keys KEY[/PATH]      # every key needed to fetch the whole content
amber-store dump KEY[/PATH] -o tree.tar  # PAX tar of the tree (default: stdout)
amber-store restore KEY[/PATH] ./dest    # recreate the tree on disk
amber-store ref ls                       # list references
amber-store ls ref:backups/home@sub/dir  # ref:NAME[@PATH] works wherever KEY[/PATH] does
```

To share a store over the network, run a remote server and register it with
each local daemon:

```sh
# on the server host — owns its own store, signs every response
amber-store serve --store /path/to/remote-store --listen :8590 \
  --identity /etc/amber/server_key
# authorize clients via the admin UI: set AMBER_ADMIN_PASSWORD and open /admin/

# on a client host — confirm fingerprint once, then push/pull by reference
amber-store remote add origin https://store.example.com:8590
amber-store remote push-objects origin backups/home  # push objects, then the ref
amber-store remote push-ref     origin backups/home
amber-store remote pull-objects origin backups/home  # pull objects, then the ref
amber-store remote pull-ref     origin backups/home
amber-store remote ls-refs      origin               # list all refs on the remote
```

For offline operation, `ingest` can write the pack-write stream to a file
instead of a daemon, and `load` uploads it later:

```sh
amber-store ingest -o tree.amberpack ./some/dir
amber-store load tree.amberpack
```

Ingest parallelism is set with `--jobs` (default: number of CPUs). Chunking is
tunable with `--min/--avg/--max` (ultracdc byte chunking) and `--item-bits`
(index/entry chunking); `--xattr-inline-max` controls when extended attributes
spill to an `XattrSet` object.

## Development

The repository uses a [Nix](https://nixos.org/) flake (with
[direnv](https://direnv.net/)) to provide the Go toolchain.

```sh
direnv allow        # or: nix develop
go build ./...
```

- Module: `github.com/draganm/amber-store`
- Go: 1.26+
