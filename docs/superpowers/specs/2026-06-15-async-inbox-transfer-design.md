# Async inbox transfer — receive to inbox, ack, process in the background

**Status:** design approved 2026-06-15.

## Purpose

Decouple receiving a pack from processing it. Today `POST /v1/objects` decodes →
verifies → compresses → appends → fsyncs **inline inside the request handler**
before returning 200, so the sender's many parallel upload workers all stall on
the receiver's serialized append path. This design has the receiver persist each
authenticated pack to a durable **inbox**, ack 200 immediately, and drain the
inbox into the packstore from a **background processor**. Setting the reference
becomes the synchronization barrier: it waits for that root's packs to finish
processing, runs the existing `fstree.CheckComplete`, then commits the ref.

The same machinery applies symmetrically on the pull (client) side.

## Goals / non-goals

- **Goal:** pipeline wall-clock time — network receive overlaps with CPU-bound
  decode/verify/compress/append.
- **Goal:** remove cross-request contention on the serialized append path; let
  one processor drain many packs with fewer, larger fsyncs.
- **Goal:** durable acceptance — 200 means "on disk, will be processed," surviving
  a restart.
- **Goal (secondary):** streaming receive removes the 64 MiB in-memory body
  buffer, so the receiver's pack-size ceiling becomes disk, not RAM.
- **Non-goal:** changing the security model (SSHSIG request/response signing,
  signed references, no-dangling completeness check) — preserved as-is.
- **Non-goal:** changing `packstore`'s on-disk layout or `CheckComplete`.
- **Non-goal:** skipping `WriteParallel`'s decode→re-`EncodeRecord` step
  (possible later optimization; out of scope here).
- **Non-goal (deferred):** inbox backpressure / 503-retry when the sender
  outruns the processor. Negotiation already bounds total bytes to the missing
  set; the safety valve is deferred until measured to matter.

## Concepts

- **Ref** (a.k.a. "tag"): a named, signed pointer `NAME → root`. Unchanged.
- **Root:** the 32-byte key the ref points at; the thing completeness is checked
  against.
- **Pack:** an `amberpack` stream of objects, the existing wire body.
- **Inbox:** a directory of durable, self-describing, authenticated pack files
  awaiting processing.

## On-disk inbox format

`<store>/inbox/` holds **one self-describing file per received pack** — plain
files, no auxiliary DB, no lock files (the refstore stays in its own Pebble dir;
the inbox does not add another). Each entry is a single file:

```
[u32 meta-len][meta CBOR][amberpack body]
```

`meta` = `{ ref, root(32B), pubkey, sig, nonce, receivedAt }`. Filename is
`<blake3-of-body-hex>.pack`, so a re-pushed identical pack is content-addressed
and idempotent (re-arrival is a no-op).

**Atomic commit:** write to `inbox/tmp/<rand>`, fsync the file, `rename` it into
`inbox/`, fsync the inbox dir — *then* ack 200. A file present in `inbox/` is by
construction fully received and signature-covered ("store only complete
downloads where auth matches"). Partial transfers live only under `inbox/tmp/`
and are swept on startup.

**Quarantine:** `inbox/failed/` holds packs that error during processing
(corrupt record / verify failure). They are logged and not retried.

## Push flow (server side)

`POST /v1/objects?ref=NAME&root=<key>` — the ref and root ride in the query,
which the request signature already covers, so they are authenticated. A single
push targets one `(ref, root)`; every pack in that push carries the same pair.

Handler:

1. Stream the request body into `inbox/tmp/<rand>` while computing `blake3`.
2. Verify the request signature against that hash (replaces the current
   `io.ReadAll(io.LimitReader(body, maxBody+1))` whole-body buffer — this is what
   lifts the 64 MiB RAM ceiling).
3. Write the `meta` header, fsync, `rename` into `inbox/`, fsync dir.
4. Return 200. Response means "durably accepted," not "stored."

## Processing pool & the group barrier

A pool of N workers drains `inbox/`. Per entry:

1. `amberpack` decode → `store.WriteParallel(seq, WriteOpts{Verify:true})`.
2. On success: delete the inbox file.
3. On verify/corrupt error: move the file to `inbox/failed/`, log it.

**Group index:** an in-memory `map[root]*group`, where `group` tracks the count
of pending inbox entries for that root plus a "drained" signal. The index is
keyed on **root alone** — root is what completeness depends on; `ref` is carried
only so the set operation knows the target name. The index is rebuilt by
scanning `inbox/` at startup and updated as packs arrive and drain.

## Ref-set barrier

`PUT /v1/refs?name=NAME` for root `R`:

1. Wait until `group[R]` is drained — every pack for `R` is either stored or
   quarantined.
2. Run the existing `fstree.CheckComplete(R, store.Get, store.Has)`.
3. On success, store the signed reference (unchanged path).
4. On missing objects, return the existing
   *"referenced content is incomplete: … push objects before the ref"* error.

Ordering holds without extra coordination: the client uploads all packs (each
200 = durably in inbox) **before** issuing the ref PUT, so by the time the PUT
arrives every pack for `R` is already in the inbox and counted in `group[R]`.

**Error contract:** a corrupt/quarantined pack ⇒ its objects are absent ⇒
`CheckComplete` reports the root incomplete ⇒ the client re-pushes. No new
endpoint. The precise per-record failure (CRC, verify mismatch) lives only in
the server log; the client sees "incomplete."

## Crash recovery

200 means durable. On startup the receiver:

1. Sweeps `inbox/tmp/` (incomplete transfers).
2. Scans `inbox/` and rebuilds the `map[root]*group` index.
3. Starts the processor pool.

A `PUT /v1/refs` arriving after a restart waits on the rebuilt group exactly as
in steady state. If every pack for `R` was processed before the crash, `group[R]`
is empty and the handler proceeds straight to `CheckComplete`. There is no
on-disk lock or journal — the inbox files *are* the recovery state.

## Pull flow (client side) — symmetry

The same machinery runs locally. `Pull()`:

1. Fetches packs (`FetchObjects`) and stages each into a client-side
   `<store>/inbox/`, tagged with the pull's `(ref, root)`, instead of writing
   straight to the local packstore.
2. The same processor drains the local inbox into the local packstore.
3. The local ref-set after pull is the barrier: it reuses the existing
   `localMissing` / `CheckComplete` gate, then writes the local ref.

Fetch network I/O now overlaps with local append, and fetch workers no longer
block on the append path.

## Tradeoffs (accepted)

- **Double write:** each pack hits disk twice (inbox, then packstore). The cost
  of durable-async. Acceptable unless disk write bandwidth — not CPU / append
  contention — is the real bottleneck.
- **Re-encode retained:** `WriteParallel` still decodes then re-`EncodeRecord`s.
  Not optimized here.
- **Coarser errors:** a bad pack surfaces as "ref incomplete," not "pack X
  record Y failed CRC"; the detail is in the server log only.

## Components touched

- `server/server.go` — auth middleware: stream body for the objects endpoint
  instead of buffering; drop the 64 MiB body cap there.
- `server/objects.go` — `postObjects`: write to inbox + ack instead of
  `WriteParallel`.
- `server/refs.go` — `putRef`: wait on `group[root]` before `CheckComplete`.
- **New** inbox package — entry format, atomic commit, scan/recovery, processor
  pool, `map[root]*group` barrier.
- `remoteclient` / `remotesync` — push: add `ref`/`root` query params; pull:
  stage fetched packs to the local inbox and process via the same pool.

## Open follow-ups (out of scope)

- Inbox backpressure / 503-retry under sustained overrun.
- inbox→packstore without recompression.
- Server-side compaction of the many small segments incremental pushes create
  (existing compactor story).
