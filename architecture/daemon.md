# Daemon / CLI Split

This document describes **where** the work happens: how responsibilities are
divided between the long-running `amber-store daemon` and the short-lived CLI
commands that talk to it.
The object model lives in [types.md](types.md) and [fstree.md](fstree.md); the key format in [keys.md](keys.md).

## The shape

Amber-Store runs as **one daemon per store** plus any number of transient CLI
clients:

- The **daemon** (`amber-store daemon --store DIR`) opens the packstore and is
  the only process that ever touches it. It serves HTTP/1.1 over a unix socket;
  the handler is transport-agnostic, so a TCP listener can be added later
  without touching the routes.
- Every **other command** (`ingest`, `load`, `dump`, `restore`, `ls`,
  `content-keys`, `ref`) is a client: it connects to the socket, performs one
  operation, and exits. Clients never open the store directory.
  (`config-user` is the one exception — it only writes the local user config,
  contacting nothing.)

Single-process store ownership is the point: there is no cross-process locking
protocol, no reader seeing a half-written object, and crash-durability policy
(`--sync`) is decided in exactly one place.

## Division of labor

The split follows the CPU/I-O boundary:

**The client builds trees.** Everything described in
[fstree.md](fstree.md#building-a-tree-bottom-up) — walking the source
filesystem, content-defined chunking, BLAKE3 hashing, deterministic CBOR
encoding — runs inside the `ingest` CLI process, fanned out across `--jobs`
workers. The daemon never sees the source filesystem and never chunks
anything; it receives finished CAS objects. Because chunking and hashing are
deterministic functions of content, the client can compute the root key itself
and print it without the daemon's help.

**The daemon stores and reads objects.** It verifies uploads (re-hashing each
payload against its claimed key, so a buggy or hostile client cannot poison
the store), persists them, and serves the read-side traversals — directory
listing, path resolution, tar export, and reachable-key walks — which need
many small random-access `get`s and therefore belong next to the store.

| Command                 | Client-side work                                                | Daemon-side work                                  |
|-------------------------|-----------------------------------------------------------------|---------------------------------------------------|
| `daemon`                | —                                                               | owns the packstore, serves all routes             |
| `ingest NAME DIR`       | walk, chunk, hash, encode; stream pack; PUT ref; print root key | verify + store each object                        |
| `ingest -o F`           | same, but write the pack to a file — no daemon needed           | —                                                 |
| `load F`                | stream an existing pack file                                    | verify + store each object                        |
| `dump KEY[/P]`          | write the tar to stdout/file                                    | resolve path, traverse tree, emit PAX tar         |
| `restore KEY[/P] DIR`   | extract the tar into DIR (perms, mtimes, xattrs)                | resolve path, traverse tree, emit PAX tar         |
| `ls KEY[/P]`            | render `ls -l` style output                                     | resolve path, collect entries, emit NDJSON        |
| `content-keys KEY[/P]`  | print one key per line                                          | resolve path, walk every reachable object         |
| `ref create/ls/show/rm` | render output                                                   | reference CRUD against the refs DB                |
| `serve`                 | —                                                               | remote server: owns its own store, signed HTTP(S) |
| `remote add/rm/ls`      | fingerprint confirmation, render listing                        | fetch + pin server identity, registry CRUD        |
| `remote push-objects`   | render progress + stats                                         | walk, negotiate, upload missing batches           |
| `remote pull-objects`   | render progress + stats                                         | resolve remote ref, batched BFS download          |
| `remote push/pull-ref`  | render outcome                                                  | validate + transfer the reference record          |
| `remote ls-refs`        | render listing                                                  | proxy the remote's reference listing              |

Note `dump` and `restore` share one daemon route: restore is "dump + extract",
with the tar as the interchange format. The PAX format carries everything the
store models (nanosecond mtimes, xattrs, device nodes), so nothing is lost in
the middle.

## Rendezvous: the socket path

Daemon and clients must independently agree on a socket path. Resolution order
(`internal/socketpath`), identical on both sides:

1. the `--socket` flag;
2. `$AMBER_STORE_SOCKET`;
3. `$XDG_RUNTIME_DIR/amber-store.sock`, when `XDG_RUNTIME_DIR` is set and
   absolute (typical on Linux);
4. `/tmp/amber-store-<uid>.sock`.

The fallback deliberately ignores `TMPDIR`: tools like nix-shell and direnv
set a fresh per-shell `TMPDIR`, which would make a daemon and a client started
in different shells resolve different "default" paths and never meet. `/tmp`
is stable for every process of a user; the per-uid suffix avoids cross-user
collisions, and the socket's file mode keeps other users from connecting.

## Wire protocol

All routes are versioned under `/v1`. Keys travel as lowercase hex; an
optional `?path=` query names a slash-separated subdirectory of the key to
operate on instead of the key itself (resolved daemon-side, since resolution
needs object fetches).

| Route                              | Body / response                                              |
|------------------------------------|--------------------------------------------------------------|
| `POST /v1/objects`                 | pack-write stream in → store stats JSON out                  |
| `GET /v1/tar/{key}?path=`          | PAX tar of the directory tree                                |
| `GET /v1/ls/{key}?path=`           | NDJSON, one directory entry per line, name order             |
| `GET /v1/content-keys/{key}?path=` | text, one hex key per line, root first, deduped              |
| `PUT /v1/refs?name=`               | CBOR reference record in → 204                               |
| `GET /v1/refs?name=`               | CBOR reference record out                                    |
| `GET /v1/refs`                     | NDJSON, one reference per line, name order                   |
| `DELETE /v1/refs?name=`            | 204                                                          |
| `POST /v1/remotes/preflight?url=`  | → server identity + fingerprint JSON                         |
| `PUT /v1/remotes?name=&url=`       | confirmed public key JSON in → 204                           |
| `GET /v1/remotes`                  | NDJSON, one remote per line, name order                      |
| `DELETE /v1/remotes?name=`         | 204                                                          |
| `POST /v1/remote/push-objects`     | `?remote=&name=` → NDJSON progress + final stats             |
| `POST /v1/remote/pull-objects`     | `?remote=&name=` → NDJSON progress + final stats             |
| `POST /v1/remote/push-ref`         | `?remote=&name=` → 204                                       |
| `POST /v1/remote/pull-ref`         | `?remote=&name=` → 204                                       |
| `GET /v1/remote/refs?remote=`      | NDJSON, the remote's reference listing                       |

The `/v1/remotes` and `/v1/remote/*` routes are registered when the daemon was
started with remote support. In the shipped CLI they are always present: the
remote registry opens unconditionally, and signing keys come from the
`--remote-key` flag. See [remote.md](remote.md) for the server protocol and
sync algorithms.

Status mapping: malformed pack streams and hash-verification failures are
`422`; a key that is not the right object type, a `..` or through-a-file path,
is `400`; an absent root object or path component is `404`; store failures are
`500`. Streaming responses (`tar`, `ls`, `content-keys`) do as much work as
possible **before** the first byte — type/existence checks, path resolution,
and for `ls`/`content-keys` the full collection — so errors surface as proper
statuses; a failure after bytes are in flight can only be logged and the
stream cut, surfacing client-side as a truncated read.

## The pack-write format

Uploads use the `amberpack` stream (`AMBERPK\x01` magic, then framed
`key + payload` records): a **flat, unordered, possibly-partial set of
objects** with no root key — like a git pack. This buys:

- **Streaming ingest.** Objects are uploaded as the tree build emits them
  (children before parents per file/directory, but globally unordered); the
  client holds O(jobs) objects in memory, never the tree.
- **Offline operation.** `ingest -o` writes the same stream to a file with no
  daemon involved; `load` uploads it later. A pack is a self-contained
  transport artifact.
- **Idempotent, resumable uploads.** The store is a content-addressed bag:
  re-uploading objects dedups by key (reported in the stats), and a partial
  upload is harmless — missing objects simply make the root unreadable until a
  later pack supplies them.

The stream carries no root key by design: the *store* tracks objects, not
trees. The root key is the client's output (printed by `ingest`); naming roots
is handled by [references](references.md), which daemon-mode `ingest` creates
after the upload completes.

## Trust boundary

The daemon trusts nothing about an upload: each object's key must be canonical
([keys.md](keys.md)) and its payload must re-hash to the key. A failure
rejects the upload with `422` and the offending object is never stored; valid
objects already written from the same stream are harmless leftovers in a
content-addressed bag. Downstream, clients get the same guarantee in reverse
for free: every object a read route serves was verified on the way in, and any client can
re-verify any fetched object against its key. The unix socket's file
permissions are the only access control — anything that can connect can read
and write the store.
