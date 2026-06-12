# Pipelined Push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure `remotesync.Push` into a three-stage pipeline — parallel checkers negotiating missing keys, a single re-batcher coalescing them into full upload batches, and parallel uploaders — per the approved spec `docs/superpowers/specs/2026-06-12-pipelined-push-design.md`.

**Architecture:** Checkers run `POST /v1/objects/missing` per upfront check batch and feed missing keys to `missingCh`; one re-batcher accumulates them into byte-balanced upload batches on `uploadCh`; long-lived uploader workers drain it with `store.Get` + `PushPack`. Everything runs under one `errgroup.WithContext` (fail-fast), all channel ops select on the group context, and the `jobs` budget is statically split `checkers = max(1, jobs/4)`, `uploaders = max(1, jobs-checkers)`.

**Tech Stack:** Go 1.26 (builtin `max`, range-over-int are fine), `golang.org/x/sync/errgroup`, existing `httptest`-based e2e harness in `remotesync/harness_test.go`.

**Read first:** the spec (`docs/superpowers/specs/2026-06-12-pipelined-push-design.md`), `remotesync/push.go`, `remotesync/batch.go`, `remotesync/harness_test.go`.

**File map:**

| File | Change | Responsibility |
|---|---|---|
| `remotesync/batch.go` | Modify | Extract a reusable `batcher` accumulator; `Batches` rewritten on top of it |
| `remotesync/rebatch.go` | Create | Channel-driven `rebatch` (stage 2) built on `batcher` |
| `remotesync/rebatch_test.go` | Create | Unit tests for `rebatch` (internal, `package remotesync`) |
| `remotesync/push.go` | Modify | `splitJobs` + the three-stage pipeline in `Push`; doc comments |
| `remotesync/push_internal_test.go` | Create | Unit test for `splitJobs` (internal, `package remotesync`) |
| `remotesync/harness_test.go` | Modify | `newHarnessMW` so tests can wrap the server handler |
| `remotesync/push_test.go` | Modify | New e2e tests (coalescence, no-op, monotonic progress, errors) |
| `architecture/remote.md` | Modify | Update the **Push-objects** section |

All commands run from the repo root `/Users/dragan/draganm/amber-store`.

---

### Task 1: Extract the `batcher` accumulator

Pure refactor: `Batches` keeps its exact behavior (existing tests in `remotesync/batch_test.go` are the safety net), but the accumulate/flush logic becomes a reusable `batcher` that Task 3's `rebatch` will share. DRY: the overflow rules (byte target, `maxBatchKeys` cap, oversized-key-alone) must exist in exactly one place.

**Files:**
- Modify: `remotesync/batch.go:56-77` (replace `Batches`)
- Test: `remotesync/batch_test.go` (existing, unchanged)

- [ ] **Step 1: Replace the `Batches` function with `batcher` + a thin `Batches`**

In `remotesync/batch.go`, replace the existing `Batches` function (lines 56-77, from the `// Batches bins keys...` comment to the end of the file) with:

```go
// batcher accumulates keys into byte-balanced batches: estimated payload
// sizes approach target without exceeding it, a batch never holds more than
// maxBatchKeys keys, and a single object larger than target gets its own
// batch.
type batcher struct {
	target uint64
	size   SizeOf
	cur    []key.Key
	bytes  uint64
}

// add appends k to the current batch, first returning the completed batch
// k would have overflowed (nil if k still fits).
func (b *batcher) add(k key.Key) []key.Key {
	s := b.size(k)
	var full []key.Key
	if len(b.cur) > 0 && (b.bytes+s > b.target || len(b.cur) >= maxBatchKeys) {
		full = b.cur
		b.cur, b.bytes = nil, 0
	}
	b.cur = append(b.cur, k)
	b.bytes += s
	return full
}

// flush returns the final partial batch, nil if empty.
func (b *batcher) flush() []key.Key {
	out := b.cur
	b.cur, b.bytes = nil, 0
	return out
}

// Batches bins keys, in order, into batches whose estimated payload sizes
// approach target without exceeding it (a single object larger than target
// gets its own batch).
func Batches(keys []key.Key, target uint64, size SizeOf) [][]key.Key {
	b := batcher{target: target, size: size}
	var out [][]key.Key
	for _, k := range keys {
		if full := b.add(k); full != nil {
			out = append(out, full)
		}
	}
	if last := b.flush(); last != nil {
		out = append(out, last)
	}
	return out
}
```

- [ ] **Step 2: Run the remotesync tests to verify behavior is preserved**

Run: `go test ./remotesync/ -v -run TestBatches`
Expected: `TestBatchesBalanceByBytes`, `TestBatchesOversizedSingleItemGetsOwnBatch` PASS.

Run: `go test ./remotesync/`
Expected: `ok` (all package tests pass).

- [ ] **Step 3: Commit**

```bash
git add remotesync/batch.go
git commit -m "refactor(remotesync): extract batcher accumulator from Batches"
```

---

### Task 2: `splitJobs` budget split

**Files:**
- Create: `remotesync/push_internal_test.go`
- Modify: `remotesync/push.go` (add `splitJobs` below the `Opts` methods, around line 40)

- [ ] **Step 1: Write the failing test**

Create `remotesync/push_internal_test.go` (note: `package remotesync`, NOT `remotesync_test` — it tests an unexported function):

```go
package remotesync

import "testing"

func TestSplitJobs(t *testing.T) {
	for _, tc := range []struct{ jobs, checkers, uploaders int }{
		{1, 1, 1}, // pipeline floor: the one case allowed to exceed the budget
		{2, 1, 1},
		{3, 1, 2},
		{4, 1, 3},
		{8, 2, 6},
		{16, 4, 12},
	} {
		c, u := splitJobs(tc.jobs)
		if c != tc.checkers || u != tc.uploaders {
			t.Errorf("splitJobs(%d) = (%d, %d), want (%d, %d)",
				tc.jobs, c, u, tc.checkers, tc.uploaders)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./remotesync/ -run TestSplitJobs`
Expected: FAIL to build with `undefined: splitJobs`.

- [ ] **Step 3: Implement `splitJobs`**

In `remotesync/push.go`, add after the `jobs()` method (after line 40):

```go
// splitJobs divides the jobs budget between negotiation and upload workers
// so total in-flight requests stay <= jobs. jobs=1 floors at 1+1, the
// minimum for a pipeline and the one documented budget overshoot.
func splitJobs(jobs int) (checkers, uploaders int) {
	checkers = max(1, jobs/4)
	uploaders = max(1, jobs-checkers)
	return checkers, uploaders
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./remotesync/ -run TestSplitJobs -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add remotesync/push.go remotesync/push_internal_test.go
git commit -m "feat(remotesync): add splitJobs budget split for the push pipeline"
```

---

### Task 3: Channel-driven `rebatch` (stage 2)

**Files:**
- Create: `remotesync/rebatch.go`
- Create: `remotesync/rebatch_test.go`

- [ ] **Step 1: Write the failing tests**

Create `remotesync/rebatch_test.go` (`package remotesync` — internal). Blob keys encode their exact payload length, so the size function in these tests is just `key.Key.Length` and no diskstore is needed:

```go
package remotesync

import (
	"context"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// testKeys returns n distinct blob keys whose payloads are size bytes long
// (size must be >= 2; the first two bytes make the contents distinct).
func testKeys(t *testing.T, n, size int) []key.Key {
	t.Helper()
	out := make([]key.Key, 0, n)
	for i := range n {
		payload := make([]byte, size)
		payload[0], payload[1] = byte(i), byte(i>>8)
		o, err := fstree.EncodeBlob(payload)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o.Key)
	}
	return out
}

func blobLength(k key.Key) uint64 { return k.Length() }

// runRebatch feeds inputs through rebatch and collects the emitted batches.
func runRebatch(t *testing.T, inputs [][]key.Key, target uint64) [][]key.Key {
	t.Helper()
	in := make(chan []key.Key)
	out := make(chan []key.Key)
	errc := make(chan error, 1)
	go func() {
		errc <- rebatch(context.Background(), in, out, target, blobLength)
		close(out)
	}()
	go func() {
		for _, batch := range inputs {
			in <- batch
		}
		close(in)
	}()
	var got [][]key.Key
	for b := range out {
		got = append(got, b)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	return got
}

func batchShapes(batches [][]key.Key) []int {
	shapes := make([]int, 0, len(batches))
	for _, b := range batches {
		shapes = append(shapes, len(b))
	}
	return shapes
}

func TestRebatchCoalescesSmallInputsAndFlushesTail(t *testing.T) {
	// ten single-key arrivals of 100-byte blobs, 400-byte target:
	// coalesce into [4 4] and flush the final partial [2]
	keys := testKeys(t, 10, 100)
	var inputs [][]key.Key
	for _, k := range keys {
		inputs = append(inputs, []key.Key{k})
	}
	got := batchShapes(runRebatch(t, inputs, 400))
	want := []int{4, 4, 2}
	if len(got) != len(want) {
		t.Fatalf("batch shapes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("batch shapes = %v, want %v", got, want)
		}
	}
}

func TestRebatchOversizedKeyGetsOwnBatch(t *testing.T) {
	big, err := fstree.EncodeBlob(make([]byte, 1000))
	if err != nil {
		t.Fatal(err)
	}
	small, err := fstree.EncodeBlob([]byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	got := runRebatch(t, [][]key.Key{{big.Key, small.Key}}, 100)
	if len(got) != 2 || len(got[0]) != 1 || len(got[1]) != 1 {
		t.Fatalf("batch shapes = %v, want one key each", batchShapes(got))
	}
}

func TestRebatchHonorsMaxBatchKeys(t *testing.T) {
	// 9000 tiny keys under a huge byte target must still split at 8192
	keys := testKeys(t, 9000, 2)
	got := batchShapes(runRebatch(t, [][]key.Key{keys}, 1<<30))
	if len(got) != 2 || got[0] != maxBatchKeys || got[1] != 9000-maxBatchKeys {
		t.Fatalf("batch shapes = %v, want [%d %d]", got, maxBatchKeys, 9000-maxBatchKeys)
	}
}

func TestRebatchStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := make(chan []key.Key)  // never closed
	out := make(chan []key.Key) // never drained
	if err := rebatch(ctx, in, out, 100, blobLength); err == nil {
		t.Fatal("rebatch returned nil on canceled context")
	}
}

func TestRebatchUnblocksSendOnCancel(t *testing.T) {
	// two 100-byte keys against a 100-byte target force an emit; nobody
	// receives on out, so only cancellation can unblock the send
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan []key.Key, 1)
	in <- testKeys(t, 2, 100)
	out := make(chan []key.Key)
	done := make(chan error, 1)
	go func() { done <- rebatch(ctx, in, out, 100, blobLength) }()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("rebatch returned nil after cancel while blocked on send")
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./remotesync/ -run TestRebatch`
Expected: FAIL to build with `undefined: rebatch`.

- [ ] **Step 3: Implement `rebatch`**

Create `remotesync/rebatch.go`:

```go
package remotesync

import (
	"context"

	"github.com/draganm/amber-store/key"
)

// rebatch drains in — slices of missing keys in arbitrary arrival order —
// and accumulates them into byte-balanced batches (target bytes, the
// maxBatchKeys cap, an oversized single key alone), sending each completed
// batch and the final partial one to out. It returns once in is closed and
// everything is flushed, or with ctx.Err() when ctx ends first. The caller
// owns closing out.
func rebatch(ctx context.Context, in <-chan []key.Key, out chan<- []key.Key, target uint64, size SizeOf) error {
	b := batcher{target: target, size: size}
	send := func(batch []key.Key) error {
		select {
		case out <- batch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		select {
		case keys, ok := <-in:
			if !ok {
				if last := b.flush(); last != nil {
					return send(last)
				}
				return nil
			}
			for _, k := range keys {
				if full := b.add(k); full != nil {
					if err := send(full); err != nil {
						return err
					}
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass (with the race detector)**

Run: `go test ./remotesync/ -run TestRebatch -race -v`
Expected: all five `TestRebatch*` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add remotesync/rebatch.go remotesync/rebatch_test.go
git commit -m "feat(remotesync): add channel-driven rebatch for the push pipeline"
```

---

### Task 4: Harness middleware support

Lets e2e tests observe or fail raw HTTP requests at the server boundary. Pure test-harness refactor; behavior with a nil middleware is identical.

**Files:**
- Modify: `remotesync/harness_test.go:42-69` (split `newHarness`)

- [ ] **Step 1: Add `newHarnessMW`**

In `remotesync/harness_test.go`, add `"net/http"` to the imports, then replace the `newHarness` function (lines 42-69) with:

```go
func newHarness(t *testing.T) *harness {
	return newHarnessMW(t, nil)
}

// newHarnessMW is newHarness with the server handler wrapped by mw (nil =
// unwrapped), so tests can observe or fail raw requests.
func newHarnessMW(t *testing.T, mw func(http.Handler) http.Handler) *harness {
	t.Helper()
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	var handler http.Handler = server.New(server.Config{
		Store: store, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	})
	if mw != nil {
		handler = mw(handler)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, refs: refs, identity: identity, client: client, admin: admin}
}
```

- [ ] **Step 2: Run the package tests**

Run: `go test ./remotesync/`
Expected: `ok` — all existing tests still pass through the nil-middleware path.

- [ ] **Step 3: Commit**

```bash
git add remotesync/harness_test.go
git commit -m "test(remotesync): allow wrapping the harness server with middleware"
```

---

### Task 5: Coalescence e2e test (RED against the current Push)

This is the spec's headline behavior and a true failing-first test: today each sparse check batch produces its own tiny `PushPack`, so the upload count is ~21; the pipeline must coalesce to ~11.

**Files:**
- Modify: `remotesync/push_test.go` (add helpers + test)

- [ ] **Step 1: Write the test and its helpers**

In `remotesync/push_test.go`, change the imports to:

```go
import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remotesync"
)
```

and append at the end of the file:

```go
// countUploads counts POST /v1/objects requests — uploads, not the
// /v1/objects/missing negotiation.
func countUploads(n *atomic.Int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/v1/objects" {
				n.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// putBlob stores a size-byte blob whose leading byte is seed (distinct
// seeds give distinct keys) and returns it.
func putBlob(t *testing.T, store *diskstore.Store, seed byte, size int) fstree.Object {
	t.Helper()
	payload := make([]byte, size)
	payload[0] = seed
	o, err := fstree.EncodeBlob(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	return o
}

// buildFileTree stores DirLeaf{ file "f" → FileNode → blobKeys } and returns
// the root key; the blobs themselves must already be in store.
func buildFileTree(t *testing.T, store *diskstore.Store, blobKeys []key.Key) key.Key {
	t.Helper()
	fn, err := fstree.EncodeFileNode(blobKeys)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: fn.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{fn, leaf} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return leaf.Key
}

func TestPushCoalescesSparseMissingKeys(t *testing.T) {
	var uploads atomic.Int64
	h := newHarnessMW(t, countUploads(&uploads))
	local := newLocalStore(t)
	ctx := context.Background()
	rc := h.rc(t)
	opts := remotesync.Opts{BatchBytes: 2000}

	// 40 1000-byte blobs; the even-seeded half forms the first tree
	var oldKeys, allKeys []key.Key
	for i := range 40 {
		o := putBlob(t, local, byte(i), 1000)
		if i%2 == 0 {
			oldKeys = append(oldKeys, o.Key)
		}
		allKeys = append(allKeys, o.Key)
	}
	if _, err := remotesync.Push(ctx, local, rc, buildFileTree(t, local, oldKeys), opts); err != nil {
		t.Fatal(err)
	}

	// the second tree interleaves old (present) and new (missing) blobs, so
	// every 2000-byte check batch is half-present: ~1 missing key each
	rootB := buildFileTree(t, local, allKeys)
	uploads.Store(0)
	stats, err := remotesync.Push(ctx, local, rc, rootB, opts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 22 { // 20 new blobs + FileNode + DirLeaf
		t.Fatalf("pushed %d objects, want 22", stats.ObjectsPushed)
	}
	// ~21 sparse check batches must coalesce into ~ceil(22kB/2kB) upload
	// packs, not one sliver-sized pack per check batch
	if n := uploads.Load(); n > 13 {
		t.Fatalf("push made %d upload requests, want coalesced (<= 13)", n)
	}
}
```

- [ ] **Step 2: Run it to verify it fails for the right reason**

Run: `go test ./remotesync/ -run TestPushCoalescesSparseMissingKeys -v`
Expected: FAIL with `push made 21 upload requests, want coalesced (<= 13)` (21 ± 1 — one `PushPack` per check batch that had a missing key). The `ObjectsPushed` assertion must NOT be the failure.

- [ ] **Step 3: Commit the red test**

```bash
git add remotesync/push_test.go
git commit -m "test(remotesync): red test for coalescing sparse missing keys on push"
```

---

### Task 6: The pipeline `Push`

**Files:**
- Modify: `remotesync/push.go` (rewrite `Opts.Progress` doc, `Push` doc + body)
- Modify: `remotesync/batch.go:1-5` (package comment)

- [ ] **Step 1: Rewrite `push.go`**

Replace the entire contents of `remotesync/push.go` with (note: `splitJobs` from Task 2 stays where it is in this listing):

```go
package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// DefaultJobs is the default number of parallel transfer workers.
const DefaultJobs = 4

// Opts configures Push and Pull.
type Opts struct {
	BatchBytes uint64 // per-batch payload target; 0 = DefaultBatchBytes
	Jobs       int    // parallel workers; <= 0 = DefaultJobs
	// Progress, when non-nil, is called as work completes. For Push, done
	// counts settled keys — confirmed present at negotiation, or uploaded —
	// out of total reachable keys; for Pull, done counts fetched objects
	// and total is 0 (unknown up front).
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

// splitJobs divides the jobs budget between negotiation and upload workers
// so total in-flight requests stay <= jobs. jobs=1 floors at 1+1, the
// minimum for a pipeline and the one documented budget overshoot.
func splitJobs(jobs int) (checkers, uploaders int) {
	checkers = max(1, jobs/4)
	uploaders = max(1, jobs-checkers)
	return checkers, uploaders
}

// PushStats summarizes one Push.
type PushStats struct {
	ObjectsTotal  int   // reachable objects under the root
	ObjectsPushed int   // objects the server was missing and received
	BytesPushed   int64 // payload bytes of pushed objects
}

// Push uploads every object reachable from root that the server is missing,
// as a three-stage pipeline: checkers negotiate each byte-balanced batch of
// the local reachable set against the server, a re-batcher coalesces the
// sparse missing subsets into full upload batches, and uploaders send each
// batch as one amberpack. Negotiation and upload overlap, with the jobs
// budget split between the two pools. Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *diskstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	checkBatches := Batches(keys, opts.batchBytes(), PushSizer(store))
	checkers, uploaders := splitJobs(opts.jobs())

	var mu sync.Mutex
	stats := PushStats{ObjectsTotal: len(keys)}
	done := 0
	// settle records n settled keys (pushed of them uploaded, totaling
	// pushedBytes) and reports progress outside the lock so a slow callback
	// (e.g. an HTTP flush) cannot stall the workers; values are monotonic
	// snapshots but may arrive out of order under contention.
	settle := func(n, pushed int, pushedBytes int64) {
		mu.Lock()
		done += n
		stats.ObjectsPushed += pushed
		stats.BytesPushed += pushedBytes
		progressDone, progressTotal := done, stats.ObjectsTotal
		mu.Unlock()
		if opts.Progress != nil {
			opts.Progress(progressDone, progressTotal)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	missingCh := make(chan []key.Key)
	uploadCh := make(chan []key.Key)

	// Checkers: negotiate each check batch; keys the server already has
	// settle immediately, missing ones flow to the re-batcher. The
	// companion closes missingCh once every checker is done.
	g.Go(func() error {
		cg, cctx := errgroup.WithContext(gctx)
		cg.SetLimit(checkers)
		for _, batch := range checkBatches {
			cg.Go(func() error {
				missing, err := rc.Missing(cctx, batch)
				if err != nil {
					return err
				}
				settle(len(batch)-len(missing), 0, 0)
				if len(missing) == 0 {
					return nil
				}
				select {
				case missingCh <- missing:
					return nil
				case <-cctx.Done():
					return cctx.Err()
				}
			})
		}
		err := cg.Wait()
		close(missingCh)
		return err
	})

	// Re-batcher: coalesce sparse missing subsets into full upload batches.
	g.Go(func() error {
		defer close(uploadCh)
		return rebatch(gctx, missingCh, uploadCh, opts.batchBytes(), PushSizer(store))
	})

	// Uploaders: long-lived workers draining uploadCh.
	for range uploaders {
		g.Go(func() error {
			for {
				var batch []key.Key
				var ok bool
				select {
				case batch, ok = <-uploadCh:
					if !ok {
						return nil
					}
				case <-gctx.Done():
					return gctx.Err()
				}
				objs := make([]fstree.Object, 0, len(batch))
				var pushedBytes int64
				for _, k := range batch {
					data, err := store.Get(k)
					if err != nil {
						return fmt.Errorf("reading %s: %w", k, err)
					}
					objs = append(objs, fstree.Object{Key: k, Bytes: data})
					pushedBytes += int64(len(data))
				}
				if _, err := rc.PushPack(gctx, objs); err != nil {
					return err
				}
				settle(len(batch), len(batch), pushedBytes)
			}
		})
	}

	if err := g.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}
```

Fail-fast wiring, for the reviewer: an uploader or re-batcher error returns from a direct member of `g`, cancelling `gctx` immediately; in-flight `Missing` calls then fail on the cancelled `cctx` and drain fast. A checker error cancels only `cctx` first; the companion then closes `missingCh`, returns the error to `g`, and `gctx` cancellation unwinds the re-batcher (its sends select on `gctx`) and the uploaders. `errgroup` returns the first error, so the real cause — not a `context.Canceled` from the unwind — surfaces from `Push`.

- [ ] **Step 2: Update the stale package comment**

In `remotesync/batch.go`, the package comment (lines 1-4) says "a two-round-trip have/want push". Replace it with:

```go
// Package remotesync implements the push/pull algorithms between a local
// diskstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a pipelined have/want push (parallel
// negotiation, re-batching, parallel upload), and a round-based BFS pull.
// See architecture/remote.md.
```

- [ ] **Step 3: Run the coalescence test — now green**

Run: `go test ./remotesync/ -run TestPushCoalescesSparseMissingKeys -race -v`
Expected: PASS (upload count ~11, well under 13).

- [ ] **Step 4: Run the whole package with the race detector**

Run: `go test ./remotesync/ -race -v`
Expected: every test PASSes — in particular the pre-existing `TestPushTransfersAllReachableObjects`, `TestPushIsMinimalOnRerun`, `TestPushReportsProgress` are unchanged and green.

- [ ] **Step 5: Commit**

```bash
git add remotesync/push.go remotesync/batch.go
git commit -m "feat(remotesync): pipeline push into checkers, re-batcher, and uploaders"
```

---

### Task 7: Remaining e2e tests

Regression tests for behavior the pipeline must preserve (these should pass immediately; any failure is a Task 6 bug).

**Files:**
- Modify: `remotesync/push_test.go` (append tests + one helper)

- [ ] **Step 1: Write the tests**

Append to `remotesync/push_test.go` (`sync` joins the imports):

```go
// failPath 500s every request to path, untouched otherwise. The error
// reaches Push either as a StatusError or — because the injected 500 is
// unsigned — as a server-identity error; both are push failures.
func failPath(path string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == path {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func TestPushNoOpMakesNoUploads(t *testing.T) {
	var uploads atomic.Int64
	h := newHarnessMW(t, countUploads(&uploads))
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}

	uploads.Store(0)
	var last, total int
	stats, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{
		Progress: func(d, tot int) { last, total = d, tot },
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 0 || stats.BytesPushed != 0 {
		t.Fatalf("no-op push transferred %+v, want nothing", stats)
	}
	if n := uploads.Load(); n != 0 {
		t.Fatalf("no-op push made %d upload requests, want 0", n)
	}
	if last != 4 || total != 4 {
		t.Fatalf("final progress = %d/%d, want 4/4 (all keys settle at negotiation)", last, total)
	}
}

func TestPushProgressIsMonotonic(t *testing.T) {
	// the 4-object tree fits one check batch, so the two callbacks —
	// settle-at-negotiation then settle-at-upload — are strictly ordered
	// and the recorded sequence must never decrease
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	var mu sync.Mutex
	var seen []int
	_, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{
		Progress: func(done, total int) {
			if total != 4 {
				t.Errorf("total = %d, want 4", total) // Errorf: callback runs off the test goroutine
			}
			mu.Lock()
			seen = append(seen, done)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("progress went backwards: %v", seen)
		}
	}
	if len(seen) == 0 || seen[len(seen)-1] != 4 {
		t.Fatalf("recorded progress %v, want it to end at 4", seen)
	}
}

func TestPushSurfacesUploadFailure(t *testing.T) {
	h := newHarnessMW(t, failPath("/v1/objects"))
	local := newLocalStore(t)
	root := buildTree(t, local)
	if _, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{}); err == nil {
		t.Fatal("push succeeded although every upload failed")
	}
}

func TestPushSurfacesNegotiationFailure(t *testing.T) {
	h := newHarnessMW(t, failPath("/v1/objects/missing"))
	local := newLocalStore(t)
	root := buildTree(t, local)
	if _, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{}); err == nil {
		t.Fatal("push succeeded although negotiation failed")
	}
}
```

- [ ] **Step 2: Run the new tests with the race detector**

Run: `go test ./remotesync/ -run 'TestPushNoOp|TestPushProgressIsMonotonic|TestPushSurfaces' -race -v`
Expected: all four PASS. The two failure tests double as shutdown checks — a pipeline deadlock would hang them (package timeout would flag it), and `-race` covers the cancellation paths.

- [ ] **Step 3: Commit**

```bash
git add remotesync/push_test.go
git commit -m "test(remotesync): cover no-op uploads, progress monotonicity, push failures"
```

---

### Task 8: Documentation + final verification

**Files:**
- Modify: `architecture/remote.md:189-199` (the **Push-objects** paragraph)

- [ ] **Step 1: Update the Push-objects section**

In `architecture/remote.md`, replace lines 189-199 (the paragraph starting `**Push-objects:**` through `missing-check.`) with:

```markdown
**Push-objects:** the daemon resolves the local reference name to its key,
walks the reachable set (the existing reachable-keys walk), bins keys into
byte-balanced batches, and runs a three-stage pipeline. The `--jobs` budget
(default 4) caps total in-flight requests: a quarter of it — at least one
worker — negotiates while the rest upload.

1. Checkers `POST /v1/objects/missing` with each batch's key list; keys the
   server already has settle on the spot.
2. A re-batcher coalesces missing keys from all responses into fresh
   byte-balanced upload batches, so an incremental push sends a few full
   packs instead of one sliver per check batch.
3. Uploaders `POST /v1/objects` with one amberpack per upload batch.

Nothing the server already has crosses the wire, and negotiation overlaps
upload. Re-running after an interruption is naturally idempotent —
already-pushed objects drop out in the missing-check.
```

- [ ] **Step 2: Full verification**

Run: `gofmt -l . && go vet ./...`
Expected: no output (no unformatted files, no vet findings).

Run: `go test ./... -race`
Expected: every package `ok` — the daemon and server packages exercise Push end-to-end and must stay green.

- [ ] **Step 3: Commit**

```bash
git add architecture/remote.md
git commit -m "docs: describe the pipelined push in architecture/remote.md"
```
