# Remote Sync Redesign — Phase 4c: list-then-fetch pull

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `remotesync.Pull`'s interleaved round-based BFS (fetch a batch → parse → discover children → fetch more, *O(tree depth)* round-trips) with a **list-then-fetch** pull: ask the server for the whole reachable key set (Phase 4a's `POST /v1/objects/reachable`), fetch every locally-missing key in parallel packs, then run a local completeness walk as the authoritative gate.

**Architecture:** `rc.ReachableKeys(root)` returns the full set; `store.Missing` filters to what's absent; `fetchAll` downloads byte-balanced packs in parallel and stores them with hash verification. A local walk (`localMissing`) then confirms completeness — any straggler the server omitted is fetched and the gate re-runs, to a fixpoint (one walk, zero fetches, in the common case). The public API (`Pull` signature, `PullStats`) is unchanged, so the daemon caller is untouched.

**Tech Stack:** Go, `golang.org/x/sync/errgroup`, `rc.ReachableKeys`/`rc.FetchObjects`, `store.Missing`, `store.WriteParallel(Verify)`, `Batches`/`PullSizer`.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — "Pull", "What is removed".

**Base commit:** `8b5f726` (HEAD after Phase 4b).

---

## Why the integration tests don't change

`remotesync/pull_test.go` passes unchanged against the new design:
- `TestPullFetchesWholeTree` — `ReachableKeys` returns 4, `Missing` → 4, fetch 4, gate clean. `ObjectsFetched == 4`.
- `TestPullCompletesPartialLocalTree` — root pre-seeded, `Missing` → 3, fetch 3, gate descends the present root and finds the rest present. `ObjectsFetched == 3`.
- `TestPullIsMinimalOnRerun` — `Missing` → 0, no fetch, gate clean. `ObjectsFetched == 0`.
- `TestPullAbsentRootFails` — the root is a Blob (a leaf), so the server's `ReachableKeys` returns `[root]` without reading it; the subsequent fetch of that key hits the server's `objects/get` existence pre-check → `404` → error.

## File structure (Phase 4c)

**Rewritten:** `remotesync/pull.go` — `Pull` (list → fetch loop) + `fetchAll` + `localMissing` helpers; the round-based BFS removed.
**Added:** `remotesync/pull_internal_test.go` — a unit test for `localMissing` (the gate's classify logic).
**Edited:** `remotesync/batch.go` — package doc (pull is now list-then-fetch, not a round-based BFS).

---

## Task 1: Rewrite `Pull` as list-then-fetch with a local completeness gate

**Files:**
- Rewrite: `remotesync/pull.go`
- Add: `remotesync/pull_internal_test.go`
- Edit: `remotesync/batch.go` (package doc only)

- [ ] **Step 1: Rewrite `remotesync/pull.go` with EXACTLY this content**

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

// PullStats summarizes one Pull.
type PullStats struct {
	ObjectsFetched int   // objects downloaded from the server
	BytesFetched   int64 // their payload bytes
}

// Pull completes the local store under root from the remote: it asks the server
// for the full set of reachable keys, fetches every key the local store lacks as
// byte-balanced packs in parallel, then runs a local completeness walk as the
// authoritative gate — any object still missing (a key the server omitted from
// its list) is fetched and the gate re-runs, to a fixpoint. In the common case
// the gate walks once and fetches nothing. Idempotent: a re-run fetches nothing,
// and an interrupted run is safe to repeat — already-stored objects are skipped,
// and the gate guarantees completeness before the caller writes the reference.
func Pull(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PullStats, error) {
	var stats PullStats
	var mu sync.Mutex
	// settle records n fetched objects and reports progress outside the lock so
	// a slow callback cannot stall the workers. Pull's total is unknown to the
	// progress contract, so it reports 0.
	settle := func(n int, fetchedBytes int64) {
		mu.Lock()
		stats.ObjectsFetched += n
		stats.BytesFetched += fetchedBytes
		done := stats.ObjectsFetched
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(done, 0)
		}
	}

	// Bulk phase: fetch everything the server says is reachable that we lack.
	serverKeys, err := rc.ReachableKeys(ctx, root)
	if err != nil {
		return stats, fmt.Errorf("listing reachable keys: %w", err)
	}
	toFetch, err := store.Missing(serverKeys)
	if err != nil {
		return stats, err
	}

	// Fetch, then walk locally to confirm completeness; any straggler the
	// server's list omitted is fetched and the gate re-runs, to a fixpoint.
	for {
		if len(toFetch) > 0 {
			if err := fetchAll(ctx, store, rc, toFetch, opts, settle); err != nil {
				return stats, err
			}
		}
		toFetch, err = localMissing(root, store)
		if err != nil {
			return stats, err
		}
		if len(toFetch) == 0 {
			return stats, nil
		}
	}
}

// fetchAll downloads keys as byte-balanced packs in parallel, verifying and
// storing each pack against its keys.
func fetchAll(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, keys []key.Key, opts Opts, settle func(n int, fetchedBytes int64)) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.jobs())
	for _, batch := range Batches(keys, opts.batchBytes(), PullSizer()) {
		g.Go(func() error {
			objs, err := rc.FetchObjects(gctx, batch)
			if err != nil {
				return err
			}
			seq := func(yield func(packstore.Object, error) bool) {
				for _, o := range objs {
					if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
						return
					}
				}
			}
			// Verify: the store re-hashes every object against its key, so a
			// hostile or corrupted stream can never poison the local store.
			if _, err := store.WriteParallel(seq, packstore.WriteOpts{Verify: true}); err != nil {
				return fmt.Errorf("storing fetched objects: %w", err)
			}
			var fetchedBytes int64
			for _, o := range objs {
				fetchedBytes += int64(len(o.Bytes))
			}
			settle(len(objs), fetchedBytes)
			return nil
		})
	}
	return g.Wait()
}

// localMissing walks the tree from root over the LOCAL store and returns the
// keys that are reachable but not yet present — the set still to fetch. A
// present non-leaf node is descended; an absent node is collected and not
// descended (its children are unknown until it is fetched). Blob and XattrSet
// keys are leaves and are never descended.
func localMissing(root key.Key, store *packstore.Store) ([]key.Key, error) {
	seen := map[key.Key]bool{}
	var missing []key.Key
	frontier := []key.Key{root}
	for len(frontier) > 0 {
		var next []key.Key
		for _, k := range frontier {
			if seen[k] {
				continue
			}
			seen[k] = true
			has, err := store.Has(k)
			if err != nil {
				return nil, err
			}
			if !has {
				missing = append(missing, k)
				continue
			}
			if k.Type() == key.Blob || k.Type() == key.XattrSet {
				continue
			}
			data, err := store.Get(k)
			if err != nil {
				return nil, err
			}
			kids, err := fstree.ChildKeys(k, data)
			if err != nil {
				return nil, err
			}
			next = append(next, kids...)
		}
		frontier = next
	}
	return missing, nil
}
```

- [ ] **Step 2: Update the package doc in `remotesync/batch.go`**

Replace:

```go
// Package remotesync implements the push/pull algorithms between a local
// packstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a phased have/want push (parallel whole-set
// negotiation, then parallel upload of byte-balanced packs), and a round-based
// BFS pull. See architecture/remote.md.
```

with:

```go
// Package remotesync implements the push/pull algorithms between a local
// packstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a phased have/want push (parallel whole-set
// negotiation, then parallel upload of byte-balanced packs), and a
// list-then-fetch pull (the server-walked reachable key set, parallel pack
// fetch, then a local completeness gate). See architecture/remote.md.
```

- [ ] **Step 3: Add `remotesync/pull_internal_test.go`**

```go
package remotesync

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
)

// TestLocalMissingClassifiesPartialTree checks the completeness-gate walk:
// present interior nodes are descended, and reachable-but-absent objects are
// collected as the set still to fetch.
func TestLocalMissingClassifiesPartialTree(t *testing.T) {
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	mk := func(o fstree.Object, err error) fstree.Object {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	b1 := mk(fstree.EncodeBlob([]byte("one")))
	b2 := mk(fstree.EncodeBlob([]byte("two")))
	fn := mk(fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key}))
	leaf := mk(fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: fn.Key[:]},
	}))

	// Store the two interior nodes but neither blob.
	for _, o := range []fstree.Object{leaf, fn} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	missing, err := localMissing(leaf.Key, store)
	if err != nil {
		t.Fatalf("localMissing: %v", err)
	}
	// The walk descends leaf -> fn (both present) and reports b1, b2 missing.
	want := map[key.Key]bool{b1.Key: true, b2.Key: true}
	if len(missing) != len(want) {
		t.Fatalf("missing = %v, want the two blobs", missing)
	}
	for _, k := range missing {
		if !want[k] {
			t.Errorf("unexpected missing key %s", k)
		}
	}

	// With everything stored, the gate is clean.
	for _, o := range []fstree.Object{b1, b2} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	if m, err := localMissing(leaf.Key, store); err != nil || len(m) != 0 {
		t.Fatalf("complete tree: missing = %v, err = %v, want none", m, err)
	}
}
```

- [ ] **Step 4: Build & vet**

Run: `go build ./... && go vet ./remotesync/`
Expected: clean.

- [ ] **Step 5: Run the remotesync tests**

Run: `go test ./remotesync/ -v`
Expected: PASS — `pull_test.go` (unchanged): `TestPullFetchesWholeTree` (4), `TestPullCompletesPartialLocalTree` (3), `TestPullIsMinimalOnRerun` (0), `TestPullAbsentRootFails` (error); the new `TestLocalMissingClassifiesPartialTree`; and all the push/batch tests.

- [ ] **Step 6: Full suite**

Run: `go test ./...`
Expected: every package PASSES. The daemon's `remotePullObjects` calls `remotesync.Pull` with the unchanged signature.

- [ ] **Step 7: Commit**

```bash
git add remotesync/pull.go remotesync/pull_internal_test.go remotesync/batch.go
git commit -m "remotesync: list-then-fetch pull with a local completeness gate"
```

---

## Self-review

**Spec coverage:** Implements the spec's pull (resolve → server reachable list → fetch missing in parallel packs → local completeness gate that fetches stragglers to a fixpoint) and removes the interleaved BFS ("What is removed"). The gate is a full local walk (`localMissing`), per the spec's Decision of record, recovering from an incomplete server list rather than only detecting it.

**Placeholder scan:** None — full `pull.go`, the new test, and the exact doc edit with runnable commands.

**Type/name consistency:** Public surface unchanged — `Pull(ctx, *packstore.Store, *remoteclient.Client, key.Key, Opts) (PullStats, error)`, `PullStats`. Uses `rc.ReachableKeys` (Phase 4a), `rc.FetchObjects`, `store.Missing`, `store.WriteParallel`, `Batches`/`PullSizer`. New private helpers `fetchAll`, `localMissing`.

**Risk & termination:** The fetch loop terminates — each iteration either fetches new objects (shrinking the reachable-but-absent set, bounded by the tree size) or `fetchAll` errors (a server `404`/verify failure aborts). `WriteParallel(Verify)` re-hashes every object, and `objects/get` pre-checks existence, so a successful `fetchAll` stores exactly its requested keys and the next gate makes progress — no infinite loop. Observable behavior matches every existing pull test (whole/partial/rerun/absent), which is why none change. The straggler-recovery loop body (iteration ≥ 2) only runs against a server that omits keys — defense-in-depth that the harness cannot easily inject; its per-iteration logic is the same `fetchAll`+`localMissing` exercised on the honest path and unit-tested via `localMissing`.
