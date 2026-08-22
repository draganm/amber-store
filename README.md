# Amber-Store

A content-addressable store for **filesystem trees** — arbitrarily deep
directories and files, where file content is split by content-defined chunking and
every object is identified by a fixed 32-byte key derived from a hash of its
content.

> **Status:** working implementation. A store-owning daemon serves CLI clients
> over a unix socket; trees are built client-side and ingested, listed, dumped,
> restored, browsed in an interactive TUI, and (on Linux) mounted as a read-only
> FUSE filesystem through it.
> The design is specified in [`architecture/`](architecture/).

## nix-cached

Built on this store: [`nix-cached`](docs/nix-cached.md), a peer-to-peer
pull-through cache for cache.nixos.org. Machines share downloaded store
paths with each other — over the LAN, the internet, and through NATs —
while preserving upstream signatures.

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

## CLI, daemon, server

Three kinds of process, two protocols between them:

```text
source dir ──▶ CLI ──── unix socket ────▶ daemon ──── signed HTTP(S) ────▶ server
               (builds trees)             (owns the local store)           (owns a shared store)
```

- The **CLI** is short-lived: every command connects, performs one operation,
  and exits. The CPU-heavy tree building — walking the source, content-defined
  chunking, BLAKE3 hashing, CBOR encoding — happens here, fanned out across
  `--jobs` workers; and since keys are deterministic functions of content, the
  client computes the root key itself, without the daemon's help.
- The **daemon** is the only process that ever opens the store directory — no
  cross-process locking, no reader seeing a half-written object. It verifies
  and persists uploaded objects, serves the read-side traversals (`ls`, tar
  export, reachable-key walks), owns the references DB and the remote
  registry, and performs all communication with remote servers.
- The **server** (`amber-store serve`) is a TCP/HTTP(S) sibling of the daemon
  that owns its own store, for sharing over the network. Local daemons are its
  only clients; every request and every response is signed.

### How objects flow

Objects are immutable and content-addressed, so every hop can verify what it
receives, and transfers reduce to "send what the other side is missing":

- **Ingest** streams objects to the daemon as the tree build emits them, in
  the `amberpack` format — a flat, unordered, possibly-partial set of
  `key + payload` records with no root, like a git pack. The daemon re-hashes
  every payload against its claimed key before storing it, so a buggy or
  hostile client cannot poison the store. The same format works offline
  (`ingest -o` + `load`) and on the wire between daemon and server.
- **Push** walks the reachable set locally, asks the server which of those
  keys it lacks, and uploads only the missing ones in byte-balanced batches
  over parallel workers. **Pull** resolves the reference on the server, then
  BFS-walks the tree from its key, fetching only objects absent locally and
  parsing arriving tree nodes to extend the frontier.
- Because the store is a content-addressed bag, every transfer is idempotent
  and resumable: re-running after an interruption skips whatever already
  arrived.

### How references flow

A reference is a small record — name → key, plus creator, timestamp, and an
SSH signature ([`architecture/references.md`](architecture/references.md)).
References always travel **after** objects: dangling references are refused
on both ends (the daemon checks that the pointed-to key exists; the server
walks the whole tree under it and rejects the record if any reachable object
is missing), which is what makes `push-objects` → `push-ref` the natural
order. Locally a name is freely overwritable; on a server, names have owners
(see below).

### How auth works

Three layers, local to remote
([`architecture/remote.md`](architecture/remote.md)):

- **Local daemon: socket permissions.** The unix socket's file mode is the
  only access control — anything that can connect can read and write the
  store.
- **Daemon ↔ server: mutual SSH-key signing.** The daemon signs every request
  with its client key (an SSHSIG over method, path, timestamp, nonce, and a
  BLAKE3 hash of the body); the server checks the timestamp window, rejects
  replayed nonces, verifies the signature, and requires the key to be in its
  allowed-keys database — managed through the password-gated `/admin/` UI.
  Every response is signed with the server's identity key, which the daemon
  pins at `remote add` after the user confirms its fingerprint
  (trust-on-first-use, like SSH). TLS is optional and orthogonal: signatures
  already give authenticity and integrity; TLS adds confidentiality.
- **Reference ownership: record signatures.** A server only accepts signed
  reference records, and the signing key owns the name: an existing reference
  may only be overwritten by a record signed with the same key. The transport
  key and the signer key are independent identities — the same user can
  update their references from any of their machines. Deletion (which carries
  no record to sign) and ownership overrides are reserved for transport keys
  marked `admin`.

## Architecture

| Document | Contents |
|----------|----------|
| [`architecture/keys.md`](architecture/keys.md)   | The 32-byte lookup key: header byte, payload length, truncated hash. |
| [`architecture/types.md`](architecture/types.md) | The type model: object types, filesystem entry types, length-field semantics. |
| [`architecture/fstree.md`](architecture/fstree.md) | On-the-wire CBOR layout of every type, the chunkers, tree construction, and read paths. |
| [`architecture/daemon.md`](architecture/daemon.md) | The daemon/CLI split: who builds trees, who stores objects, the unix-socket protocol, and the pack-write format. |
| [`architecture/fuse.md`](architecture/fuse.md) | The `fuse` mount (Linux only): reconstructing files at open into RAM and serving reads through kernel passthrough. |
| [`architecture/references.md`](architecture/references.md) | Named pointers to keys: record layout, name rules, storage, routes. |
| [`architecture/remote.md`](architecture/remote.md) | The remote server (`amber-store serve`): identity, signed HTTP protocol, wire routes, sync algorithms. |
| [`architecture/generational-gc.md`](architecture/generational-gc.md) | Generational garbage collection: packstores as generations, the age invariant and ordered uploads, copy-forward sweeps, promotion by directory rename, leases and the ref barrier. |
| [`architecture/simple-gc.md`](architecture/simple-gc.md) | Simple garbage collection: per-root tail closures on disk, their refcounted union in RAM, pack indexes scored against it and reaped by live ratio into the active segment; horizon, removal lock, delete-time re-test. |
| [`architecture/mark-sweep-gc.md`](architecture/mark-sweep-gc.md) | The implemented collector: a mark bit per sealed record slotted by the packs' own footer indexes, a write barrier that greys concurrent ingests, and a sweep that rewrites mostly-dead packs — no state between cycles. |

## Usage

One long-running daemon owns the store; every other command is a short-lived
client talking to it over a unix socket (see
[`architecture/daemon.md`](architecture/daemon.md)):

```sh
amber-store daemon --store /path/to/store      # run the store-owning daemon
```

Ingest a directory or a single file — the content is built (walked, chunked,
hashed) client-side and streamed to the daemon; the root key (hex) is printed to
stdout. A directory root is a `DirNode`; a single-file root is the file's content
key, which `browse` opens directly in the file viewer:

```sh
amber-store config-user alice              # once — who creates references
amber-store ingest backups/home ./some/dir # ingest a directory + name the root
amber-store ingest notes ./notes.md        # ingest a single file
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

For interactive exploration, `browse` opens a terminal UI: navigate directories
(`/` or `f` filters the current listing — references too — by name), view a file
as text, a hex dump, or — for CBOR content — pretty-printed JSON (`t`/`x`/`j` to
switch), and export the highlighted directory as a tar or a file as raw bytes
(`e`). `esc` always goes up one level — out of a filter, up a directory, and from
a directory's root down to the searchable reference list (where `esc` exits) — so
you can hop between references without leaving the UI:

```sh
amber-store browse ref:backups/home@sub/dir  # or browse KEY[/PATH]; q to quit
amber-store browse                            # no argument: pick from a searchable list of refs
```

On Linux, mount a tag as a read-only filesystem instead of extracting it: files
are reconstructed into RAM at open and served through kernel passthrough (see
[`architecture/fuse.md`](architecture/fuse.md)):

```sh
amber-store fuse ref:backups/home /mnt/home  # Ctrl-C to unmount
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

`.amberignore` files exclude entries from ingestion, with `.gitignore`
semantics: negation (`!pattern`), `**` globs, directory-only (`name/`) and
anchored (`/name`) patterns; a file in any subdirectory applies to that
subtree and composes with inherited patterns (last match wins). Ignored
directories are pruned without being read. The `.amberignore` files
themselves are always stored, so a restored tree re-ingests to the same
root. `--no-ignore` disables all ignore processing.

## Development

The repository uses a [Nix](https://nixos.org/) flake (with
[direnv](https://direnv.net/)) to provide the Go toolchain.

```sh
direnv allow        # or: nix develop
go build ./...
```

- Module: `github.com/draganm/amber-store`
- Go: 1.26+
- The `browse` TUI is built on [Bubble Tea](https://github.com/charmbracelet/bubbletea)
  (with `bubbles` and `lipgloss`); the rest of the CLI has no UI dependencies.

## Building with JOBS

[`BUILD.jobs`](BUILD.jobs) builds the `amber-store` daemon with
[JOBS](https://github.com/draganm/jobs) — **fully offline, hermetic, and
CGO-free**. One recipe drives two toolchains:

1. **The admin UI is rebuilt from source.** A pinned musl Node runs
   `npm ci --offline` (every tarball in `cmd/amber-store/ui/package-lock.json` is
   fetched as a content-addressed input and seeded into npm's cache) then
   `vite build`, producing `cmd/amber-store/ui/dist`.
2. **The Go daemon is compiled offline.** Every module in the root `go.sum` is a
   content-addressed input; `go build ./cmd/amber-store` runs with
   `CGO_ENABLED=0`, embedding the **freshly built** `ui/dist` via `go:embed`.

No network and no C compiler are used at build time: `esbuild` is a static Go
binary, `rollup` ships a prebuilt `linux-musl` native (pinned in the lockfile),
and `zeebo/blake3`'s SIMD is Go assembly that `CGO_ENABLED=0` keeps. The output
is a single static binary that serves its `/admin/` SPA.

```sh
# build and smoke-run the CLI (run appends trailing args to the entrypoint):
jb run    --source . --build-file BUILD.jobs -- daemon --help

# package a loadable OCI image:
jb image  --source . --build-file BUILD.jobs -o /tmp/amber-store.oci.tar

# debug the hermetic build interactively:
jb develop --source . --build-file BUILD.jobs
```

Builds on `linux/amd64` and `linux/arm64`. The UI rebuild needs JOBS's
`nodeplugin` with `package-lock.json` support.

## License

Licensed under the GNU Affero General Public License v3.0. See [`LICENSE`](LICENSE)
for the full text.
