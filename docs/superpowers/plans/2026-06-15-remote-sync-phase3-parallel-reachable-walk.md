# Remote Sync Redesign — Phase 3: parallel reachable walk

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `fstree.ReachableKeys`'s single-threaded recursive DFS with a **concurrent breadth-first walk** that fetches each round's frontier in parallel, still returning a buffered slice with **root first**.

**Architecture:** Each BFS round fetches the current frontier's children concurrently (one `get` per non-leaf node, bounded by `GOMAXPROCS` via `errgroup`), indexing results by frontier position so the output stays deterministic. Dedup and next-frontier construction happen sequentially between rounds (no shared mutable state across goroutines, so no lock). The function signature is unchanged, and **root remains element 0** — the only ordering property any caller or test relies on — so all callers and their tests work unmodified.

**Tech Stack:** Go (1.22+ per-iteration loop variables), `golang.org/x/sync/errgroup`, `runtime.GOMAXPROCS`.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — "Parallel reachable walk".

**Base commit:** `2c10fc6` (HEAD of `pack-based-remote-sync`).

---

## Roadmap context

Phase 1a (codec) and Phase 2 (wire pack) are done; Phase 1b was dropped. This is **Phase 3**. After it: Phase 4 (negotiation redesign + `POST /v1/objects/reachable`, which reuses this walk on the server), Phase 5 (single push/pull operation).

## Why callers don't change

`grep -rn "ReachableKeys"` shows the callers are `remotesync/push.go` (chunks the result — order-independent), `daemon/daemon.go` (the `content-keys` endpoint — streams the keys), plus tests. Every caller/test depends on at most **root-first**:
- `fstree/reachable_test.go`: `TestReachableKeys` asserts `got[0] == root`; `TestReachableKeys_FileRoot` asserts `[fileNode, blob]` (root first, single child); both hold under the BFS.
- `cmd/amber-store/content_keys_test.go` and `daemon/daemon_test.go` (`TestGetContentKeys_ListsReachableKeys`): assert `lines[0]/got[0] == root` and treat the rest as a set.

The DFS pre-order is therefore safe to drop; root-first is preserved.

## File structure (Phase 3)

**Rewritten:** `fstree/reachable.go` — the concurrent walk.
**Extended:** `fstree/reachable_test.go` — add one wide-frontier test exercising many concurrent fetches (run under `-race`). Existing tests are unchanged.

---

## Task 1: Parallelize `fstree.ReachableKeys`

**Files:**
- Rewrite: `fstree/reachable.go`
- Modify: `fstree/reachable_test.go` (add one test; leave the rest)

- [ ] **Step 1: Rewrite `fstree/reachable.go`**

Replace the ENTIRE file with:

```go
package fstree

import (
	"fmt"
	"runtime"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// ReachableKeys returns the keys of every object reachable from root — the set
// that must be transferred to hold the whole content — with root first and the
// remaining keys in unspecified order, each key listed once even when referenced
// repeatedly. root may be any object type. get fetches the bytes stored under a
// key; Blob and XattrSet objects are leaves and are not fetched.
//
// The walk is a breadth-first sweep that fetches each round's frontier
// concurrently (bounded by GOMAXPROCS), so a wide tree of small objects is read
// in parallel. Results are indexed by frontier position, so the order is in fact
// deterministic, but callers must rely only on root-first. get must be safe for
// concurrent use.
func ReachableKeys(root key.Key, get func(key.Key) ([]byte, error)) ([]key.Key, error) {
	seen := map[key.Key]bool{root: true}
	out := []key.Key{root}
	frontier := []key.Key{root}

	for len(frontier) > 0 {
		// Fetch each frontier node's children concurrently. childLists[i] holds
		// frontier[i]'s children; indexing by position keeps the result
		// deterministic regardless of completion order, and needs no lock.
		childLists := make([][]key.Key, len(frontier))
		eg := &errgroup.Group{}
		eg.SetLimit(runtime.GOMAXPROCS(0))
		for i, k := range frontier {
			if k.Type() == key.Blob || k.Type() == key.XattrSet {
				continue // leaf: no children, nothing to fetch
			}
			eg.Go(func() error {
				data, err := get(k)
				if err != nil {
					return fmt.Errorf("fstree: reading %s: %w", k, err)
				}
				children, err := ChildKeys(k, data)
				if err != nil {
					return err
				}
				childLists[i] = children
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		var next []key.Key
		for _, children := range childLists {
			for _, ck := range children {
				if seen[ck] {
					continue
				}
				seen[ck] = true
				out = append(out, ck)
				next = append(next, ck)
			}
		}
		frontier = next
	}
	return out, nil
}
```

Notes for the implementer:
- The `eg.Go` closure captures the loop variables `i` and `k`. This is correct because the module targets Go 1.22+, where each iteration has its own copy. **Confirm `go.mod` declares `go 1.22` or newer**; if (unexpectedly) older, add `i, k := i, k` at the top of the loop body. Do not otherwise change the logic.
- `errgroup.Group` with `SetLimit` makes `eg.Go` block once the limit is reached, so even a frontier of millions of keys never spawns more than `GOMAXPROCS` goroutines.
- `errgroup` (`golang.org/x/sync/errgroup`) is already a module dependency (used by `packstore`/`remotesync`); no `go get` needed.

- [ ] **Step 2: Verify the existing fstree tests still pass**

Run: `go test ./fstree/ -run TestReachableKeys -v`
Expected: PASS — `TestReachableKeys` (root-first + 7-key set + dedup of the twice-referenced blob), `TestReachableKeys_FileRoot` (`[fileNode, blob]`), `TestReachableKeys_MissingObject` (error on absent child) all pass unchanged. If any fails, STOP — the rewrite changed observable behavior it should not have.

- [ ] **Step 3: Add a wide-frontier concurrency test**

Append to `fstree/reachable_test.go`. First ensure the import block includes `"fmt"` (add it, gofmt-sorted, if absent). Then add:

```go
// TestReachableKeys_WideParallel walks a root with many subdirectories, forcing
// a wide frontier so the concurrent fetch path runs many workers at once (run
// under -race to guard the walk). It confirms root-first, the full set, and that
// every key appears once.
func TestReachableKeys_WideParallel(t *testing.T) {
	const n = 64
	var objs []fstree.Object
	add := func(o fstree.Object) { objs = append(objs, o) }

	var entries []fstree.Entry
	for i := 0; i < n; i++ {
		blob, err := fstree.EncodeBlob([]byte(fmt.Sprintf("content-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		add(blob)
		sub, err := fstree.EncodeDirLeaf([]fstree.Entry{
			{Name: []byte("f"), Mode: 0o100644, ContentKey: blob.Key[:]},
		})
		if err != nil {
			t.Fatal(err)
		}
		add(sub)
		entries = append(entries, fstree.Entry{
			Name: []byte(fmt.Sprintf("d%02d", i)), Mode: 0o040755, ContentKey: sub.Key[:],
		})
	}
	root, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	add(root)

	got, err := fstree.ReachableKeys(root.Key, mapGetter(objs...))
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	if len(got) == 0 || got[0] != root.Key {
		t.Fatalf("first key = %v, want the root %s", got, root.Key)
	}
	if want := 1 + 2*n; len(got) != want { // root + (sub leaf + blob) per entry
		t.Fatalf("got %d keys, want %d", len(got), want)
	}
	seen := map[key.Key]bool{}
	for _, k := range got {
		if seen[k] {
			t.Errorf("duplicate key %s", k)
		}
		seen[k] = true
	}
}
```

Notes:
- `mapGetter(objs ...fstree.Object)` is the existing helper used by the other tests in this file (it builds a `get` closure over the given objects). If its exact signature differs, adapt the call — but it is variadic over `fstree.Object` in the current tests, so `mapGetter(objs...)` should compile as-is.
- If `go vet`/`gofmt` flags the `for i := 0; i < n; i++` loop with a range-over-int modernization suggestion, that is a non-blocking style hint consistent with the rest of this test file; leave the explicit form for clarity unless gofmt rewrites it.

- [ ] **Step 4: Run fstree tests (including `-race`)**

Run: `go test -race ./fstree/`
Expected: PASS with no data-race reports — the new `TestReachableKeys_WideParallel` exercises 64 concurrent fetches and the race detector finds nothing (the walk shares no mutable state across goroutines; `childLists[i]` writes are to distinct indices).

- [ ] **Step 5: Run the full suite**

Run: `go build ./... && go vet ./fstree/ && go test ./...`
Expected: every package PASSES. The `content-keys` endpoint and `remotesync` push consume `ReachableKeys` unchanged (root-first preserved). If `daemon`, `cmd/amber-store`, or `remotesync` tests fail, investigate — a failure means a caller relied on the old DFS order after all; report it rather than papering over.

- [ ] **Step 6: Commit**

```bash
git add fstree/reachable.go fstree/reachable_test.go
git commit -m "fstree: parallel breadth-first ReachableKeys (root-first, unordered rest)"
```

---

## Self-review

**Spec coverage:** Implements the "Parallel reachable walk" item — a concurrent walk returning a buffered slice, root included, order no longer the DFS pre-order. The spec said "unspecified order"; the BFS is in fact deterministic, which is a stronger guarantee that still satisfies "callers rely only on root-first." Drops the DFS pre-order guarantee as the spec's "What is removed" notes.

**Placeholder scan:** None — full file content, the exact new test, and runnable commands with expected output.

**Type/name consistency:** Signature unchanged: `ReachableKeys(root key.Key, get func(key.Key) ([]byte, error)) ([]key.Key, error)`. Uses `key.Blob`/`key.XattrSet` leaf checks and `ChildKeys` exactly as the old walk did. New import: `golang.org/x/sync/errgroup` + `runtime`.

**Risk:** The only behavioral change is order (DFS → deterministic BFS) and concurrency. Verified against every caller/test that none depend on more than root-first. `get` (i.e. `packstore.Store.Get`) is documented concurrency-safe, satisfying the new concurrent-access requirement. `-race` covers the parallel path.
