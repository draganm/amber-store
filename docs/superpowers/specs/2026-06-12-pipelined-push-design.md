# Pipelined Push

Date: 2026-06-12
Status: approved

## Problem

`remotesync.Push` walks the reachable set, bins all keys into byte-balanced
check batches upfront, and then runs N workers where each worker handles one
batch end to end: negotiate the missing subset (`POST /v1/objects/missing`),
then upload exactly those objects (`POST /v1/objects`). Two consequences:

- Negotiation and upload of a batch are serialized inside one worker, so a
  worker's upload bandwidth sits idle during its negotiation round trip and
  vice versa.
- On an incremental push the missing subset of a check batch can be tiny, so
  the upload phase degenerates into many small `PushPack` calls instead of
  few full packs.

## Goal

Speed up push by decoupling negotiation from transfer: a producer side that
determines missing keys and feeds a channel, and a pool of upload workers
that consumes it. Scope is **push only**; pull keeps its round-based BFS.

## Design

`Push(ctx, store, rc, root, opts)` keeps its signature, stats type, and
idempotency. Internally it becomes a three-stage pipeline:

```text
ReachableKeys → Batches(keys, batchBytes, PushSizer)   (unchanged, upfront)
      │
      ▼
 [checkers ×C] ── rc.Missing per check batch ──▶ missingCh ([]key.Key)
      │
      ▼
 [re-batcher ×1] ── accumulate byte-balanced upload batches ──▶ uploadCh
      ▼
 [uploaders ×U] ── store.Get each key → rc.PushPack(objects)
```

### Stage 1: checkers

A pool of C goroutines drains the upfront check batches, calls
`rc.Missing(gctx, batch)` for each, and sends the missing subset (skipping
empty ones) to `missingCh`.

### Stage 2: re-batcher

A single goroutine drains `missingCh` and accumulates keys into upload
batches using the same sizing rules as the upfront batching: `PushSizer`
for per-key size, `opts.batchBytes()` target, `maxBatchKeys` cap, and an
oversized single object gets its own batch. Each full batch is sent to
`uploadCh` as soon as it is complete; the final partial batch is flushed
when `missingCh` closes. This coalesces sparse missing subsets into full
packs — the fix for the incremental-push pathology.

### Stage 3: uploaders

A pool of U goroutines drains `uploadCh`. Per batch: `store.Get` each key,
build `[]fstree.Object`, `rc.PushPack`, then update stats and report
progress (mirroring the current worker body).

### Concurrency budget: static split

Total in-flight requests stay ≤ `opts.jobs()`:

```text
checkers  C = max(1, jobs/4)        // jobs=4 → 1
uploaders U = max(1, jobs - C)      // jobs=4 → 3
```

`jobs=1` floors at C=1, U=1 — the minimum for a pipeline — and is the one
documented case that can briefly exceed the budget.

### Lifecycle and shutdown

All stages run under one `errgroup.WithContext`:

- `missingCh` is closed after all checkers finish: checkers run in a
  sub-errgroup and a companion goroutine closes the channel after its
  `Wait` returns.
- `uploadCh` is closed by the re-batcher after draining `missingCh` and
  flushing the final partial batch.
- Every channel send and receive selects on the group context's `Done()`
  so a failing stage never leaves another goroutine blocked.

Channels carry keys only, never object data; in-flight object memory stays
at today's level (≤ U × batchBytes, plus oversized singletons).

## Error handling

Fail-fast, matching today: the first error from any stage cancels the group
context, `Push` returns that error plus whatever stats accumulated. No new
error types.

## Progress

`done` becomes "keys settled":

- on a `Missing` response: `done += len(batch) − len(missing)` (keys the
  server already has settle immediately);
- on a completed upload batch: `done += len(batch)`.

`done` is monotonic and equals `total` (all reachable keys) at completion.
The `Opts.Progress` doc comment is updated; the callback is still invoked
outside the stats lock.

## Stats

`PushStats` is unchanged: `ObjectsTotal` from the walk, `ObjectsPushed` and
`BytesPushed` accumulated by uploaders under the existing mutex.

## Testing

The end-to-end harness (`remotesync/harness_test.go`: real `httptest`
server + diskstore) stays. The three existing push tests must pass
unchanged: transfers all reachable objects, minimal on rerun, reports
progress.

New tests:

- **Re-batching**: incremental push with missing keys scattered across many
  check batches produces coalesced uploads — count `POST /v1/objects`
  requests via a counting RoundTripper/middleware and assert it is
  ≈ `ceil(missingBytes / batchBytes)`, not one per sparse check batch.
- **No-op push**: zero `PushPack` calls; progress ends at `done == total`.
- **Progress monotonicity**: recorded `done` values never decrease and end
  at `total`.
- **Error propagation**: a mid-push server failure (e.g. 500 on upload)
  returns the error promptly with no goroutine leak; suite runs under
  `-race`.

## Out of scope

- Pull restructuring (stays round-based BFS).
- Dynamic/shared budget between checkers and uploaders (static split was
  chosen for simplicity; revisit only if no-op push latency on high-RTT
  links becomes a problem).
- Any wire-protocol or server-side change.
