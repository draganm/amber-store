# Remote Sync Redesign — Phase 4b: phased push

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `remotesync.Push`'s three-stage pipeline (parallel checkers → re-batcher → parallel uploaders) with a simpler **two-phase** push: negotiate the whole reachable set against the server in parallel chunks, collect the missing set, then upload byte-balanced packs of exactly those objects in parallel.

**Architecture:** Phase 1 runs `rc.Missing` over `maxBatchKeys`-sized chunks of the reachable key list concurrently (bounded by `opts.jobs()`), settling present keys and unioning the missing ones. Phase 2 runs `Batches(missing, …)` and uploads each pack via `rc.PushPack` concurrently. Collecting the entire missing set before batching coalesces sparse misses into full packs — at least as well as the old re-batcher. The public API (`Push` signature, `Opts`, `PushStats`) is unchanged, so the daemon caller is untouched. The `rebatch.go` re-batcher and `splitJobs` are deleted.

**Tech Stack:** Go, `golang.org/x/sync/errgroup`, the existing `Batches`/`PushSizer` byte-balancer.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — "Push", "What is removed".

**Base commit:** `d687a3d` (HEAD of `pack-based-remote-sync` after Phase 4a).

---

## Why the tests barely change

`remotesync/push_test.go` (the integration tests) all pass against the phased design unchanged:
- `TestPushTransfersAllReachableObjects`, `TestPushIsMinimalOnRerun`, `TestPushReportsProgress`, `TestPushNoOpMakesNoUploads`, `TestPushProgressIsMonotonic` — Phase 1 fully completes before Phase 2, so negotiation settles precede upload settles (monotonic), and a no-op push has an empty missing set (zero packs, zero uploads, all keys settle at negotiation).
- `TestPushCoalescesSparseMissingKeys` — the phased push unions **all** missing keys then `Batches` them, so sparse misses coalesce into `ceil(total/target)` packs (≤ 13 for the test's ~22 KB at 2 KB), as required.
- `TestPushSurfacesUploadFailure` / `TestPushSurfacesNegotiationFailure` — `errgroup.Wait` returns the *first* real error (the injected 500), not the `context.Canceled` unwind of sibling workers.

Only `push_internal_test.go` (`TestSplitJobs`) and `rebatch_test.go` are deleted, alongside the code they test.

## File structure (Phase 4b)

**Rewritten:** `remotesync/push.go` — the two-phase `Push` (keeps `DefaultJobs`, `Opts`, `PushStats`; removes `splitJobs`; adds `checkChunks`).
**Deleted:** `remotesync/rebatch.go`, `remotesync/rebatch_test.go`, `remotesync/push_internal_test.go`.
**Edited:** `remotesync/batch.go` — update the package doc (push is now phased, not pipelined).

---

## Task 1: Rewrite `Push` as a two-phase negotiation+upload and delete the pipeline

**Files:**
- Rewrite: `remotesync/push.go`
- Delete: `remotesync/rebatch.go`, `remotesync/rebatch_test.go`, `remotesync/push_internal_test.go`
- Edit: `remotesync/batch.go` (package doc only)

- [ ] **Step 1: Rewrite `remotesync/push.go` with EXACTLY this content**

```go
package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// DefaultJobs is the default number of parallel transfer workers.
const DefaultJobs = 4

// Opts configures Push and Pull.
type Opts struct {
	BatchBytes uint64 // per-batch payload target; 0 = DefaultBatchBytes
	Jobs       int    // parallel workers; <= 0 = DefaultJobs
	// Progress, when non-nil, is called as work completes, possibly
	// concurrently from several workers. For Push, done counts settled keys
	// — confirmed present at negotiation, or uploaded — out of total
	// reachable keys; for Pull, done counts fetched objects and total is 0
	// (unknown up front).
	Progress func(done, total int)
}

func (o Opts) batchBytes() uint64 {
	if o.BatchBytes == 0 {
		return DefaultBatchBytes
	}
	return o.BatchBytes
}

func (o Opts) jobs() int {
	if o.Jobs <= 0 {
		return DefaultJobs
	}
	return o.Jobs
}

// PushStats summarizes one Push.
type PushStats struct {
	ObjectsTotal  int   // reachable objects under the root
	ObjectsPushed int   // objects the server was missing and received
	BytesPushed   int64 // payload bytes of pushed objects
}

// Push uploads every object reachable from root that the server is missing, in
// two phases: a whole-set have/want negotiation — the reachable key list is
// checked against the server in parallel chunks — followed by a parallel upload
// of byte-balanced packs holding exactly the missing objects. Collecting the
// whole missing set before batching coalesces sparse misses into full packs.
// Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	stats := PushStats{ObjectsTotal: len(keys)}

	var mu sync.Mutex
	done := 0
	// settle records n newly-settled keys (pushed of which were uploaded,
	// totaling pushedBytes) and reports progress outside the lock so a slow
	// callback cannot stall the workers.
	settle := func(n, pushed int, pushedBytes int64) {
		mu.Lock()
		done += n
		stats.ObjectsPushed += pushed
		stats.BytesPushed += pushedBytes
		progressDone := done
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(progressDone, len(keys))
		}
	}

	// Phase 1: negotiate the whole reachable set against the server in parallel
	// chunks; keys the server already has settle immediately, the rest are
	// collected into the missing set.
	var missMu sync.Mutex
	var missing []key.Key
	neg, negCtx := errgroup.WithContext(ctx)
	neg.SetLimit(opts.jobs())
	for _, chunk := range checkChunks(keys) {
		neg.Go(func() error {
			miss, err := rc.Missing(negCtx, chunk)
			if err != nil {
				return err
			}
			// Clamped so a server replying with keys outside the queried chunk
			// cannot drive progress backwards.
			settle(max(0, len(chunk)-len(miss)), 0, 0)
			if len(miss) > 0 {
				missMu.Lock()
				missing = append(missing, miss...)
				missMu.Unlock()
			}
			return nil
		})
	}
	if err := neg.Wait(); err != nil {
		return stats, err
	}

	// Phase 2: upload byte-balanced packs holding exactly the missing objects.
	up, upCtx := errgroup.WithContext(ctx)
	up.SetLimit(opts.jobs())
	for _, batch := range Batches(missing, opts.batchBytes(), PushSizer(store)) {
		up.Go(func() error {
			objs := make([]fstree.Object, len(batch))
			var pushedBytes int64
			for i, k := range batch {
				data, err := store.Get(k)
				if err != nil {
					return fmt.Errorf("reading %s: %w", k, err)
				}
				objs[i] = fstree.Object{Key: k, Bytes: data}
				pushedBytes += int64(len(data))
			}
			if _, err := rc.PushPack(upCtx, objs); err != nil {
				return err
			}
			settle(len(batch), len(batch), pushedBytes)
			return nil
		})
	}
	if err := up.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}

// checkChunks splits the reachable key list into have/want request bodies, each
// at most maxBatchKeys keys so the request body stays well under the server's
// cap (maxBatchKeys * key.Size bytes).
func checkChunks(keys []key.Key) [][]key.Key {
	var chunks [][]key.Key
	for len(keys) > maxBatchKeys {
		chunks = append(chunks, keys[:maxBatchKeys])
		keys = keys[maxBatchKeys:]
	}
	if len(keys) > 0 {
		chunks = append(chunks, keys)
	}
	return chunks
}
```

- [ ] **Step 2: Delete the pipeline files**

```bash
git rm remotesync/rebatch.go remotesync/rebatch_test.go remotesync/push_internal_test.go
```
(`rebatch` and `splitJobs` no longer exist; `TestSplitJobs` tested `splitJobs`, `rebatch_test.go` tested `rebatch`.)

- [ ] **Step 3: Update the package doc in `remotesync/batch.go`**

Replace the package comment:

```go
// Package remotesync implements the push/pull algorithms between a local
// packstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a pipelined have/want push (parallel
// negotiation, re-batching, parallel upload), and a round-based BFS pull.
// See architecture/remote.md.
```

with:

```go
// Package remotesync implements the push/pull algorithms between a local
// packstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a phased have/want push (parallel whole-set
// negotiation, then parallel upload of byte-balanced packs), and a round-based
// BFS pull. See architecture/remote.md.
```

- [ ] **Step 4: Confirm nothing else referenced the deleted helpers**

Run: `grep -rn 'splitJobs\|rebatch' --include='*.go' .`
Expected: NO hits (both were internal to the files just rewritten/deleted). If anything remains, it must be removed/updated — report it.

- [ ] **Step 5: Build & vet**

Run: `go build ./... && go vet ./remotesync/`
Expected: clean. (If `sync/atomic` or another import is reported unused anywhere, it was only used by the old pipeline — the new `push.go` import list above is already minimal.)

- [ ] **Step 6: Run the remotesync tests**

Run: `go test ./remotesync/ -v`
Expected: PASS — every `TestPush*` in `push_test.go` (unchanged), plus `batch_test.go` and `pull_test.go`. Pay attention to `TestPushCoalescesSparseMissingKeys` (coalescing) and the two `TestPushSurfaces*Failure` (the first real error surfaces, not `context.Canceled`).

- [ ] **Step 7: Full suite**

Run: `go test ./...`
Expected: every package PASSES. The daemon's `remotePushObjects` calls `remotesync.Push` with the unchanged signature.

- [ ] **Step 8: Commit**

```bash
git add remotesync/push.go remotesync/batch.go
git rm remotesync/rebatch.go remotesync/rebatch_test.go remotesync/push_internal_test.go
git commit -m "remotesync: phased push (whole-set negotiation, then parallel upload)"
```
(If `git rm` in Step 2 already staged the deletions, the `git add` of the rewritten files plus `git commit` suffices.)

---

## Self-review

**Spec coverage:** Implements the spec's phased push ("Push": resolve → walk → whole-set missing-check in parallel chunks → byte-balanced packs → parallel upload) and the "What is removed" entries (the `rebatch` re-batcher and the checker/uploader `splitJobs` split). The pull rewrite is Phase 4c.

**Placeholder scan:** None — full `push.go`, exact deletions, the exact package-doc edit, and runnable commands.

**Type/name consistency:** Public surface unchanged — `Push(ctx, *packstore.Store, *remoteclient.Client, key.Key, Opts) (PushStats, error)`, `Opts`, `PushStats`, `DefaultJobs`. Reuses `Batches`/`PushSizer`/`maxBatchKeys`/`DefaultBatchBytes` from `batch.go` and `rc.Missing`/`rc.PushPack` unchanged. New private helper `checkChunks`. `splitJobs` removed.

**Risk:** `Push`'s observable behavior is preserved (same stats, same idempotency, same progress monotonicity, same error surfacing) — confirmed against every existing integration test, which is why none of them change. The only structural change is internal: two sequential `errgroup` phases instead of a three-stage channel pipeline, which is simpler and coalesces the missing set globally.
