# Pack-based remote sync — one walk, one negotiation, one operation

**Status:** design approved 2026-06-15.

## Purpose

Replace the two asymmetric remote-sync algorithms with a single symmetric
primitive that transfers **packs** — the same record format `packstore` writes
on disk — instead of the bespoke `AMBERPK` stream. The change has three parts:

1. **Unify the pack format.** `amberpack` becomes the pack-format library
   (record codec, footer, fuse filter, sealed-segment reader, plus a footer-less
   *pack stream* for the wire). `packstore` keeps the store and builds on it. The
   old `AMBERPK\x02` stream format is deleted.
2. **One negotiation, not many.** Both directions reduce to: the side that has
   the objects walks the reachable set and offers the full key list; the side
   that wants them decides what is missing; the sender builds pack(s) of exactly
   that; the receiver ingests them with per-object hash verification. This drops
   push's three-stage check/re-batch/upload pipeline and pull's depth-proportional
   interleaved BFS.
3. **One operation per ref.** `push`/`pull` each transfer objects *and* the
   signed reference in a single command. The four object/ref commands collapse to
   two. Transfers are refs-only; there is no bare root-key transfer.

## Goals / non-goals

- **Goal:** fewer round-trips on high-latency links — pull collapses from
  *O(tree depth)* round-trips to two (key list, then parallel packs).
- **Goal:** one wire format that is literally the on-disk record format.
- **Goal:** one user command per direction; idempotent and re-runnable.
- **Non-goal:** changing the security model (SSHSIG request/response signing,
  signed references, ownership, no-dangling) — preserved as-is.
- **Non-goal:** changing `packstore`'s on-disk layout or durability story.
- **Non-goal:** bare root-key push/pull, server-side compaction of the many
  small segments an incremental push can create (left to the existing/`v2`
  compactor story).

## The unified pack format (`amberpack`)

Today the record/footer/filter/segment code lives in `packstore`, and
`amberpack` is a *separate* zstd-stream format. We unify on `packstore`'s record
framing and split the packages by responsibility.

**`amberpack` owns the record-level format** (the framing shared by the on-disk
store and the wire):

- **Record codec** — `EncodeRecord` / `ParseRecord` / `DecodePayload`: per-record
  CRC32-C, store-if-smaller per-record zstd, 32-byte content-addressed key. All
  integers big-endian. (Extracted from `packstore/record.go` in Phase 1a.)
- **Pack stream (new wire type)** — a `Writer` / `Reader` over
  `header + records + end-marker`: a record stream *without* a footer, terminated
  by an explicit end marker so a truncated stream is detected (not silently
  treated as a clean EOF). The `Reader` yields `fstree.Object`s and validates
  framing, CRC, and key canonicality; it does **not** verify the payload hash —
  that stays in the storage path (`WriteParallel(Verify:true)`).

**`packstore` keeps the store *and* its on-disk segment format** — the footer
(fanout index + binary-fuse-16 filter), the mmap'd sealed-segment reader,
directory/flock, active-segment append, rotation/sealing, recovery tail-scan,
concurrency, and scrub/`Verify` — building on `amberpack`'s record codec. The
footer is an on-disk random-access indexing concern the wire never uses, so it
stays in `packstore` rather than moving into the format library. (**Revised
2026-06-15:** the original design had `amberpack` own the footer and
sealed-reader too; that move was dropped as risky, store-internal churn with no
wire benefit.) Relationship of the three framings, all sharing one record codec:

```
active segment   header + records                 (in-progress; tail-scan finds the end)
sealed segment   header + records + footer        (immutable; mmap'd, self-indexed)
wire pack        header + records + end-marker     (streamed; no index)
```

The `AMBERPK\x02` magic, `tagEnd`/`tagObject` grammar, and `amberpack/pack.go`
as it stands are removed; the package keeps its name and gains the format code.

## The symmetric transfer primitive

The **sender** is whoever holds the objects; the **receiver** is whoever wants
them. Every transfer is the same four steps:

```
1. SENDER   walks the reachable set from root      → full key list
2. RECEIVER checks the key list against its store  → missing set
3. SENDER   builds byte-balanced pack(s) of missing → wire packs
4. RECEIVER ingests each pack via WriteParallel(Verify) → re-hash + dedup + store
```

The receiver always decides what is missing; the sender — the side that *has*
the objects — always produces the key list. Push and pull are the two role
assignments.

### Push (daemon → server), single operation

1. Resolve the local ref name → root key; read the local **signed** ref record
   (reject client-side if unsigned, before any network traffic).
2. Walk the reachable set locally (parallel walk, §parallel walk) → key list.
3. **Phased negotiation:** split the key list into body-cap-bounded chunks and
   `POST /v1/objects/missing` for each, in parallel; union the responses into the
   missing set. (Cheap — `Has` lookups and small key-list bodies — next to
   payload upload, so the lost overlap with upload is acceptable.)
4. Split the missing set into byte-balanced packs (reuse the existing `Batches` +
   `PushSizer`, exact stored sizes from local objects). `POST /v1/objects` each
   pack in parallel; the server ingests with verification.
5. `PUT /v1/refs?name=` the signed record. The server re-checks signature,
   ownership, and no-dangling (the objects are already present, so it passes).

### Pull (server → daemon), single operation

1. `GET /v1/refs?name=` from the server; verify against the pinned server key and
   the record's own signature; extract the root key `K`.
2. `POST /v1/objects/reachable` with `K` (the verified key, not the name — pins
   the walk, no TOCTOU). The server walks its tree from `K` (parallel walk) and
   streams the full key list back, trailer-signed.
3. Compute the missing set locally (`Has` per key).
4. Split the missing set into byte-balanced packs (reuse `Batches` + `PullSizer`:
   blob keys carry exact sizes, non-blobs get the 4 KiB nominal estimate).
   `POST /v1/objects/get` each pack in parallel; ingest each via
   `WriteParallel(Verify:true)`.
5. **Completeness gate:** run a local parallel reachable walk from `K`. It reads
   each node and confirms every child is present; any still-missing key (a pack
   lost, or a key the server omitted from its list) is fetched and the gate
   re-runs, to a fixpoint. In the honest case the walk fetches nothing.
6. Write the verified ref record into the local refstore (overwrite).

Both directions are idempotent: a re-run after interruption re-negotiates and
transfers only what is still missing; an interrupted pull is safe to repeat
because `WriteParallel` skips already-stored objects and the ref is written only
after the completeness gate passes.

## Wire protocol

Routes are nearly unchanged; two bodies change format and one route is added.
All key lists are raw concatenated 32-byte keys (`internal/keylist`); body length
not a multiple of 32 is `422`. The 64 MiB per-request body cap is unchanged.

| Route | Body / response | Change |
|---|---|---|
| `POST /v1/objects/missing` | keys in → subset the server lacks | unchanged; now called over the whole set in body-cap-bounded chunks |
| `POST /v1/objects` | **amberpack pack** in → store-stats JSON out | format: `AMBERPK` → amberpack pack stream |
| `POST /v1/objects/get` | keys in → **amberpack pack** out (trailer sig) | format: `AMBERPK` → amberpack pack stream |
| `POST /v1/objects/reachable` *(new)* | 32-byte root key in → streamed key list out (trailer sig) | new: server-side reachable walk |
| `PUT /v1/refs?name=` | CBOR ref record in → `204` | unchanged |
| `GET /v1/refs?name=` | CBOR ref record out | unchanged |
| `DELETE /v1/refs?name=` | `204`; admin transport keys only | unchanged |

`POST /v1/objects/reachable` streams keys (trailer-signed, same pattern as
`objects/get`, because the body hash is known only at the end). The key list is
not load-bearing for integrity — every fetched object is re-hashed against its
key, and the daemon's local completeness gate is authoritative — so a tampered
list at worst causes wrong fetches, which the gate corrects.

## Parallel reachable walk

`fstree.ReachableKeys` (`fstree/reachable.go`) is today a single-threaded
recursive DFS returning a buffered, depth-first pre-order slice. We replace it
with a **concurrent** walk that still returns a **buffered slice**:

- A bounded worker pool fans out over child keys; `store.Get` is already
  concurrency-safe. A sharded seen-set (on the last key byte, as `packstore`'s
  `seenSet` does) dedups; each worker accumulates locally and the results are
  merged once at the end.
- The returned set is **unordered** — the DFS pre-order guarantee is dropped. No
  caller depends on order: push chunks the list for negotiation and pull treats
  it as a set; both are order-independent. The doc comment is updated to state
  "reachable set, unspecified order, root included, each key once".

The same parallel walk serves three call sites: push's local key list, the
server's `objects/reachable` handler, and pull's completeness gate.

## User and daemon surface

CLI drops from four commands to two; transfers are refs-only:

```
amber-store remote push [REMOTE] NAME     # objects + signed ref, atomic to the user
amber-store remote pull [REMOTE] NAME     # ref + objects, atomic to the user
```

`push-objects`, `push-ref`, `pull-objects`, `pull-ref` are removed; `pull` keeps
its name (no rename to `fetch`). Each command maps to one daemon endpoint —
`POST /v1/remote/push?name=` and `POST /v1/remote/pull?name=` — that performs the
whole operation and streams the existing NDJSON progress events (`{done,total}`,
final stats or error). `--jobs` and `--batch-bytes` carry over.

## Security & completeness (preserved)

- **Request/response signing:** the four `Amber-*` headers and per-response
  SSHSIG are untouched. The new `objects/reachable` stream is trailer-signed like
  `objects/get`.
- **Integrity:** every ingested record is re-hashed against its content-addressed
  key in `WriteParallel(Verify:true)` — a hostile sender cannot poison the store
  regardless of the packs or key lists it sends. A pack that fails verification or
  is truncated (missing end marker) is rejected whole.
- **Completeness:** push still passes the server's no-dangling walk on
  `PUT /v1/refs`. Pull's local reachable-walk gate is authoritative before the ref
  is written — it both confirms completeness and recovers from a server that omits
  keys from its list.
- **"All objects belonging to a pack exist locally":** guaranteed by
  `WriteParallel` success — every record it accepts is stored or already present;
  the design asserts `stored + deduped == pack record count`.

## What is removed

- `amberpack`'s `AMBERPK\x02` stream format (`tagObject`/`tagEnd` grammar, the
  whole-stream zstd frame). Replaced by the unified record-stream pack.
- Push's three-stage pipeline: the `rebatch` re-batcher and the
  checker/uploader split (`splitJobs`). Push becomes phased.
- Pull's interleaved BFS frontier loop. Pull becomes list-then-fetch.
- `push-objects` / `pull-objects` / `push-ref` / `pull-ref` across CLI
  (`cmd/amber-store/remote.go`), daemon (`daemon/remotesync.go`), and client
  (`client/remote.go`) — merged into `push` / `pull`.
- `fstree.ReachableKeys`'s ordering guarantee (implementation parallelized).

Reused unchanged: byte-balanced `Batches` + `PushSizer` / `PullSizer`,
`internal/keylist`, `internal/httpsig`, the refstore, reference signing/verify.

## Implementation phases

Sequenced so each phase is independently testable:

1. **amberpack record-codec extraction (Phase 1a — DONE 2026-06-15).** Move the
   record codec from `packstore` into `amberpack` (exported); `packstore` builds
   on it. Behavior-preserving; existing tests (re-pointed) keep it honest. (The
   footer/sealed-reader move originally folded in here was dropped — see the
   unified-pack-format section.)
2. **Wire pack format (Phase 2).** Add the `amberpack` pack `Writer`/`Reader`
   (`header + records + end-marker`, no footer) on the record codec; delete the
   old `AMBERPK` stream; swap the `POST /v1/objects` and `POST /v1/objects/get`
   bodies; update `server`/`remoteclient` and their tests.
3. **Parallel reachable walk** — parallelize `fstree.ReachableKeys`; update its
   contract and callers.
4. **Negotiation redesign** — add `POST /v1/objects/reachable` (server handler +
   `remoteclient` method); rewrite `remotesync.Push` (phased) and
   `remotesync.Pull` (list → fetch → gate); delete the pipeline and BFS.
5. **Single operation** — merge daemon endpoints and CLI commands; rewrite
   `architecture/remote.md`.

## Decisions of record

- **Wire pack = record stream, no footer.** The receiver re-ingests via
  `WriteParallel`, preserving on-ingest dedup and the receiver's own segment
  sizing/rotation. Cost: the receiver decompresses then recompresses each
  payload, and per-record (not whole-stream) zstd compresses many tiny nodes
  slightly worse. Accepted for one unified format and an unchanged receive path.
- **The footer/sealed-reader stays in `packstore`.** The originally-spec'd move
  of the on-disk footer (fanout index + fuse filter + mmap'd sealed reader) into
  `amberpack` was dropped (2026-06-15): it is the most safety-critical packstore
  code, is 100% store-internal, and the wire never uses a footer — risky churn
  with no wire benefit. `amberpack` owns the record codec and the wire pack;
  `packstore` owns the on-disk segment format on top of it.
- **Negotiation is whole-set, push is phased.** Check the entire reachable set
  (chunked, parallel) before uploading; do not overlap check and upload. Simpler
  than the current pipeline; the check is cheap next to payload transfer.
- **Reachable walk buffers, parallelizes.** Buffered slice result (matches
  existing callers); concurrent traversal for throughput; order unspecified.
- **Refs only; keep `pull`.** No bare root-key transfers. The user-facing verbs
  are `push` and `pull`.
- **Completeness gate is a full local walk, not a `Has`-the-list check.** Reusing
  the parallel walk makes pull robust to an incomplete (buggy/hostile) server
  list at the cost of re-reading node objects locally — comparable to today's
  pull, which already reads every node.
