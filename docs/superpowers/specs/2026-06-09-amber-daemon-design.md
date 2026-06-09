# amber daemon — store-owning service, pack-write format, tar export

**Status:** design approved 2026-06-09.

## Purpose

Introduce a long-running daemon that owns the `diskstore` exclusively and serves
short-lived CLI invocations over a unix socket. Today every `ingest`/`restore`
opens the Pebble-backed store directly, and Pebble locks `db/` for the lifetime
of the process — only one process at a time may use a store. The daemon resolves
this and, in the same move, splits responsibilities:

- **Client = build.** Walk a directory, content-defined-chunk, BLAKE3-hash, and
  assemble the `fstree` (the CPU-heavy work), then serialize objects into a
  compact **pack-write format**.
- **Daemon = own + verify + serve.** Hold the store, verify every object's key,
  store via `diskstore.WriteParallel`, and stream filesystem tars back out.

This satisfies four goals the client confirmed: own the exclusive store lock,
offload CPU to (parallelizable, per-caller) clients, keep the store warm across
calls, and use a protocol that can move from a unix socket to TCP later for
remote access without a rewrite.

The `pack` command and the `castar` package are removed.

## Architecture

```
   client (short-lived CLI)                         daemon (long-lived)
   ─────────────────────────                        ───────────────────
   walk → chunk → BLAKE3 → fstree                   owns diskstore (exclusive Pebble lock)
   (reuses driver / pbuilder)                       net/http handler on a unix listener
        │
        ├─ ingest:  stream pack-format ─POST /v1/objects─► verify each key → WriteParallel
        │           ◄──────── {root, stored, deduped, bytes} ──┤
        ├─ dump/restore: ──────────────GET /v1/tar/{key}──────► traverse CAS → stream PAX tar
        │           ◄──────────────── tar stream ─────────────┤
        └─ load FILE: stream a prebuilt pack file ─POST /v1/objects┘
```

- **Single binary.** `amber-store` keeps one `cmd/amber-store`; `daemon` is a
  subcommand alongside the client subcommands.
- **HTTP/1.1 over the unix socket.** stdlib `net/http` on a `net.Listen("unix",
  …)` listener. Streaming request bodies (upload) and response bodies (tar
  download) come for free, as do status codes for errors. The same
  `http.Handler` serves a TCP listener later. Routes are versioned (`/v1/…`).
- **The daemon does no chunking.** It verifies keys and drives `diskstore`.

## Command surface

| Command | Talks to daemon? | Behaviour |
|---|---|---|
| `amber-store daemon --store DIR [--socket PATH]` | — (is the daemon) | Open the store exclusively; serve HTTP on the unix socket. |
| `amber-store ingest DIR [--socket PATH]` | yes (`POST /v1/objects`) | Build the tree, **stream** pack-format to the daemon, print the root key. |
| `amber-store ingest DIR --output FILE` | no | Offline: build the tree, write the **same** pack-format to a file. Prints the root. Does not open the store, so no lock conflict. Replaces the old `pack`. |
| `amber-store load FILE [--socket PATH]` | yes (`POST /v1/objects`) | Upload a prebuilt pack file to the daemon. Prints the root. |
| `amber-store dump KEY [--socket PATH] [-o FILE]` | yes (`GET /v1/tar/{key}`) | Fetch the directory tar; write to file/stdout. |
| `amber-store restore KEY DIR [--socket PATH]` | yes (`GET /v1/tar/{key}`) | Fetch the tar; extract faithfully into DIR. |

**Removed:** the `pack` command (`runPack`, `pack()` in `driver.go`) and the
`castar` package (only `pack` used it). The tree-build core in `driver.go` /
`ingest.go` (`driver`, `pbuilder`, `buildDir`, `buildFile`, `buildEntry`) stays —
the client still builds trees; only the *sink* changes from "tar/store" to
"`amberpack.Writer` / HTTP stream".

**Socket discovery (clients and daemon agree):** `--socket` flag →
`AMBER_STORE_SOCKET` env → default `$XDG_RUNTIME_DIR/amber-store.sock`, falling
back to `/tmp/amber-store.sock` when `XDG_RUNTIME_DIR` is unset. The **store dir
is known only to the daemon**; clients never name it.

## The pack-write format

A flat, sequential, stream-friendly byte format — no seeking, no index —
identical whether streamed over the socket or written to a file.

```
Header   "AMBERPK\x01"                     8 bytes   magic + version
Records  repeat:
           0x01                            1 byte    record tag = object
           key                            32 bytes   full canonical key
           uvarint(len(payload))
           payload                   len bytes
Trailer    0x00                            1 byte    record tag = end
           rootKey                        32 bytes
           uvarint(objectCount)
```

- **The 32-byte key already encodes type and logical length** (`key[0]` =
  type + length-size; the next 1–8 bytes = length). The envelope therefore needs
  no separate type/length fields — just `key + uvarint(len) + payload`.
- **Per-object overhead ≈ 33 bytes + a 1–3 byte varint.** The old tar spent a
  512-byte header *plus* padding to a 512-byte boundary *plus* a 64-hex-char
  name per object; for directory-heavy trees with many small objects the
  pack-format is far smaller, and for blob-heavy trees it never wastes the
  ~1 KB/object tar overhead.
- **The 1-byte record tag** makes the stream self-terminating: a client that
  builds and uploads concurrently need not know the object count up front, and a
  truncated upload is detected by the missing `0x00` trailer.
- **Root in the trailer**, not the header: the root is the last object built, so
  a trailer lets build-and-upload overlap (as today's ingest pipeline does). The
  trailer `rootKey` is what `load FILE` reports for an offline file and what the
  daemon echoes back.
- **No separate checksum.** Every object is BLAKE3-verified by the daemon, and
  the trailer detects truncation; a stream-level CRC would be redundant.

## Daemon — verify and store (`POST /v1/objects`)

The request body is the pack-format stream. The daemon decodes it into an
`iter.Seq2[diskstore.Object, error]` and feeds `diskstore.WriteParallel`,
reusing the existing concurrent writer pool unchanged.

**Verification runs in the write path, conditional on dedup**, so the "skip
hashing on re-ingest" win is preserved:

- Add `Verify bool` to `diskstore.WriteOpts`. `WriteParallel` already calls
  `Has(key)` per object; if present → skip (no hash, no write — cheap dedup). If
  absent and `Verify` is set: recompute `blake3(payload)`, assemble the
  canonical key from the transmitted key's type+length prefix plus the recomputed
  hash, and **reject the whole upload on mismatch**.
- For `Blob` and `XattrSet` — the two types whose key length *is* the serialized
  byte length — additionally assert `key.Length() == len(payload)`. Cheap, and
  catches a class of client bugs. Aggregate types (FileNode / DirLeaf / DirNode)
  carry a *logical* length the daemon can't recompute without parsing; for those
  the hash check alone is the integrity guarantee.

Putting `Verify` on `diskstore` (rather than a verifying wrapper in the daemon)
avoids a redundant second `Has` lookup per object.

**Response:** JSON `{ "root": "<hex>", "objects_stored": N, "objects_deduped":
M, "bytes_stored": B }`. On verification failure: HTTP 422 with a body naming the
offending key (expected vs computed).

**Failure semantics** (consistent with today's `ingest`): `WriteParallel` is not
atomic, so a verification failure mid-stream may leave already-committed objects.
Because the store is content-addressed and idempotent, those are harmless
orphans — exactly like a crash mid-ingest — and a retry converges. No root is
"blessed" anywhere (there is no root registry; the root is just a key the client
holds), so a failed upload returns an error and no root.

## Tar export and restore (`GET /v1/tar/{key}`)

**Server-side** (new `tarexport` package): given a directory key and a
`get func(key.Key) ([]byte, error)`, traverse the CAS (the same descent as
`restore.go`'s `collectEntries` / `writeContent`) and stream a **PAX-format**
tar. PAX is required to faithfully carry what the CAS holds: nanosecond mtime
(PAX `mtime`), extended attributes (PAX `SCHILY.xattr.*`), long names, device
nodes, and fifos. Sockets cannot be archived — skipped with a warning, as
`restore.go` already does. The response body is the tar stream. A missing object
discovered mid-traversal aborts the handler (logged); headers are already sent,
so the truncated archive surfaces as a tar read error on the client.

**Client-side:**

- `dump` — copy the response body to file/stdout.
- `restore` — extract the tar into DIR with the faithful-metadata logic moved
  from `restore.go` (`applyMeta`: chmod; `lchown` only when running as root;
  xattrs best-effort; nsec mtime via `UtimesNanoAt`; unsafe-name rejection). One
  change from the recursive restorer: a tar lists a directory *before* its
  children, so directory mode + mtime are **deferred to a final pass** (standard
  tar-extractor behaviour) so writing children does not disturb a restored
  directory's mtime — the recursive restorer gets this ordering for free; the
  streaming extractor must defer it explicitly.

## Package layout

New:

- `amberpack/` — the pack-write format. `Writer` (an `fstree.Emit` sink:
  object → bytes) used by the client; `Reader` (bytes →
  `iter.Seq2[diskstore.Object, error]`) used by the daemon. One definition,
  exercised from both sides.
- `daemon/` — the `http.Handler`: routes `POST /v1/objects` and
  `GET /v1/tar/{key}`, wired to a `*diskstore.Store` and `tarexport`. Pure
  handler constructed with a store (no listener logic), so tests drive it via
  `httptest`.
- `client/` — thin HTTP client for the CLI: `Ingest(ctx, io.Reader) (Result,
  error)`, `Tar(ctx, key) (io.ReadCloser, error)`. Wraps an `http.Client` with a
  unix-socket dialer.
- `tarexport/` — server-side CAS → PAX-tar traversal.
- `tarextract/` — client-side tar → filesystem with faithful metadata (logic
  lifted from `restore.go`).

Changed:

- `diskstore/` — add `WriteOpts.Verify` (and the Blob/XattrSet length assertion).
- `cmd/amber-store/` — `main.go` rewires commands; add `daemon.go`, `dump.go`,
  `load.go`; retarget `ingest.go` to stream to the client (or write the
  `--output` file); slim `restore.go` to "fetch tar + `tarextract`".

Removed:

- `castar/` and the `pack` command.

## Error handling and edge cases

- **Daemon not running / stale socket:** the client surfaces connection-refused
  as a clear "is the daemon running? (`amber-store daemon --store …`)" message.
  The daemon removes a stale socket file on startup and re-binds.
- **Concurrent clients:** `diskstore` is concurrency-safe and the handler is
  stateless per request, so multiple ingests/dumps run in parallel;
  `WriteParallel`'s `seenSet` is per-request.
- **Dump of a bad root:** 404 if the root object is absent; 400 if the key is not
  a directory type.
- **Graceful shutdown:** the daemon traps SIGINT/SIGTERM, stops accepting, drains
  in-flight requests, `store.Close()`, and removes the socket.
- **Progress bar:** today's `ingest` progress bar is sized by a pre-scan and
  driven during the build — all client-side, and unchanged. The upload is fast
  relative to chunking and hashing.

## Testing

Following the repo's style (stdlib `testing`, no testify, parity tests):

- **amberpack round-trip:** `Writer` → `Reader` reproduces objects and root; a
  truncated stream (missing trailer) is detected; a corrupt payload fails
  verification.
- **Parity:** objects emitted into `amberpack` for a tree equal the objects from
  the existing build walk (mirrors `TestIngestObjects_ParityWithPack`), and the
  resolved root matches.
- **Daemon handler** via `httptest`: ingest then dump round-trips a tree
  byte-for-byte; a tampered key in the upload → 422; dedup counts are correct on
  re-ingest.
- **End-to-end over a real unix socket:** `daemon` plus client in one test;
  `ingest DIR` then `restore KEY DIR2`, asserting DIR2 == DIR (mode, mtime,
  symlinks, and xattrs where the test filesystem supports them), reusing
  `restore_test.go`'s comparison helpers.

## Out of scope (this iteration)

- Two-phase have/want upload negotiation (send keys, server replies missing,
  client streams only those). It would save bandwidth on re-ingest but adds
  round-trips; bandwidth is nearly free over a local unix socket. The v1 pack
  format does not preclude adding it later.
- TCP listener / TLS / authentication for remote access. The handler is written
  transport-agnostic so this is an additive change, not a rewrite.
- Garbage collection of orphan objects (inherited from `diskstore`'s scope).
