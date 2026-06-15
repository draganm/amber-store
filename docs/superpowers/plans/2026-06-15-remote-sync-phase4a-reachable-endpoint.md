# Remote Sync Redesign — Phase 4a: server reachable endpoint + client method

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /v1/objects/reachable` — the server walks its tree from a given root key (via the parallel `fstree.ReachableKeys`) and returns the full set of reachable keys — plus the matching `remoteclient.ReachableKeys` method. This is what **pull** (Phase 4c) uses to learn the whole key set before fetching.

**Architecture:** Purely additive — no existing handler or sync algorithm changes. The handler walks fully before responding (so an incomplete tree surfaces as a clean status, not a truncated body), then returns the key list as raw concatenated 32-byte keys (`internal/keylist`), header-signed like `POST /v1/objects/missing`. The client reuses the existing buffered `do` path (which verifies the signature against the pinned key and maps non-2xx to `StatusError`).

**Tech Stack:** Go, `net/http`, `internal/keylist`, `internal/httpsig`, `fstree.ReachableKeys`.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — "Wire protocol" / "Pull".

**Base commit:** `7b18fb6` (HEAD of `pack-based-remote-sync`).

---

## Roadmap context

Phases 1a, 2, 3 are done (1b dropped). This is **Phase 4a**, the first slice of Phase 4 (negotiation redesign). After it: **4b** (rewrite `remotesync.Push` to phased whole-set negotiation), **4c** (rewrite `remotesync.Pull` to list→fetch→gate, which consumes this endpoint), then **Phase 5** (merge daemon endpoints + CLI into single `push`/`pull`).

## Design decision: buffered response, not streamed

The spec's wire table sketched this endpoint as a *streamed, trailer-signed* response (to avoid a body cap). We instead return a **buffered, header-signed** body, because:
- `fstree.ReachableKeys` (Phase 3) buffers the entire key set in RAM server-side anyway (it returns a slice), and the daemon needs every key in RAM to compute the missing set — so streaming saves no memory on either side.
- It reuses the existing `do` path verbatim (verify + `StatusError`), so the client method is ~5 lines instead of a bespoke streaming reader.
- The only cost is the client's `maxResponse` cap (256 MiB ≈ 8M keys per response). A reachable set that large is far beyond current scale; if it ever matters, a streamed variant can be added without changing callers. This cap is the one accepted limitation.

## File structure (Phase 4a)

**Modified:**
- `server/objects.go` — add the `postObjectsReachable` handler; add the `key` import.
- `server/server.go` — register the route.
- `remoteclient/objects.go` — add the `ReachableKeys` method.
- `remoteclient/remoteclient_test.go` — add two end-to-end tests.

---

## Task 1: Add the reachable endpoint and client method

**Files:**
- Modify: `server/objects.go`, `server/server.go`, `remoteclient/objects.go`
- Modify (tests): `remoteclient/remoteclient_test.go`

- [ ] **Step 1: Add the `postObjectsReachable` handler to `server/objects.go`**

Add `"github.com/draganm/amber-store/key"` to the import block, then append this handler (it mirrors `postMissing`'s shape and the "walk fully before writing" convention used by the daemon's content-keys endpoint):

```go
// postObjectsReachable walks the tree under the requested root key and returns
// the full set of reachable keys as raw concatenated 32-byte keys. The request
// body is the 32-byte root key. The walk runs to completion before any byte is
// written, so an incomplete tree (a reachable object missing from the store)
// surfaces as a clean 500 rather than a truncated body. The response is
// header-signed; pull uses it to learn the whole key set before fetching.
func (h *handler) postObjectsReachable(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	root, err := key.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		h.log.Error("reachable walk failed", "root", root, "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", keylist.Flatten(keys))
}
```

(`fstree` and `keylist` are already imported by `server/objects.go`; only `key` is new. `key.Parse` requires the body to be exactly 32 bytes and canonical — a wrong length or non-canonical key yields the `422`.)

- [ ] **Step 2: Register the route in `server/server.go`**

After the `POST /v1/objects/get` line (in `New`), add:

```go
	mux.HandleFunc("POST /v1/objects/reachable", h.auth(h.postObjectsReachable))
```

- [ ] **Step 3: Add the `ReachableKeys` method to `remoteclient/objects.go`**

`remoteclient/objects.go` already imports `context`, `net/http`, `key`, and `keylist`. Append:

```go
// ReachableKeys asks the server for the full set of keys reachable from root:
// the server walks its tree and returns them as raw concatenated 32-byte keys.
// The response is verified against the pinned server key by do.
func (c *Client) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects/reachable", "application/octet-stream", root[:])
	if err != nil {
		return nil, err
	}
	return keylist.Parse(body)
}
```

- [ ] **Step 4: Build**

Run: `go build ./...`
Expected: compiles. (If `server/objects.go` reports `key` unused, you forgot Step 1's call; if it reports it imported-and-unused that means a typo — fix.)

- [ ] **Step 5: Add end-to-end tests to `remoteclient/remoteclient_test.go`**

Add `"github.com/draganm/amber-store/fstree"` and `"github.com/draganm/amber-store/key"` to the test file's import block (keep gofmt-sorted; `context` is already imported). Then append:

```go
func TestReachableKeys_RoundTrip(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)

	// Build and store a small tree: a directory leaf referencing one blob.
	blob, err := fstree.EncodeBlob([]byte("hello reachable"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{blob, leaf} {
		if err := h.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	got, err := c.ReachableKeys(context.Background(), leaf.Key)
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	want, err := fstree.ReachableKeys(leaf.Key, h.store.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	if len(got) == 0 || got[0] != leaf.Key {
		t.Fatalf("first key = %v, want the root %s", got, leaf.Key)
	}
	gotSet := map[key.Key]bool{}
	for _, k := range got {
		gotSet[k] = true
	}
	for _, k := range want {
		if !gotSet[k] {
			t.Errorf("missing reachable key %s", k)
		}
	}
}

func TestReachableKeys_IncompleteTreeErrors(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)

	blob, err := fstree.EncodeBlob([]byte("absent"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Store only the leaf; its blob is missing, so the server's walk fails and
	// the endpoint returns a non-2xx the client surfaces as an error.
	if err := h.store.Put(leaf.Key, leaf.Bytes); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReachableKeys(context.Background(), leaf.Key); err == nil {
		t.Fatal("expected an error walking an incomplete tree")
	}
}
```

- [ ] **Step 6: Run the relevant tests**

Run: `go test ./remoteclient/ ./server/ -v -run 'Reachable|Missing|Fetch|Push'`
Expected: PASS — the two new `TestReachableKeys_*` plus the existing object-method tests.

- [ ] **Step 7: Full suite**

Run: `go build ./... && go vet ./server/ ./remoteclient/ && go test ./...`
Expected: every package PASSES (this phase is additive; nothing else is touched).

- [ ] **Step 8: Commit**

```bash
git add server/objects.go server/server.go remoteclient/objects.go remoteclient/remoteclient_test.go
git commit -m "server,remoteclient: add POST /v1/objects/reachable + ReachableKeys"
```

---

## Self-review

**Spec coverage:** Adds the `POST /v1/objects/reachable` route from the spec's wire table and the client method pull needs. Deviation from the spec's "streamed (trailer sig)" to a buffered, header-signed response is documented above (both sides buffer anyway; the only cost is the 256 MiB / ~8M-key `do` cap).

**Placeholder scan:** None — full handler, route line, client method, and two complete tests with runnable commands.

**Type/name consistency:** `postObjectsReachable` matches the `authedHandler` signature `(w, r, *authedRequest)` and uses `h.signError`/`h.signAndWrite` exactly like `postMissing`. `ReachableKeys(ctx, root key.Key) ([]key.Key, error)` mirrors `Missing`. Body encoding is `keylist.Flatten`/`keylist.Parse` (raw 32-byte keys), the same as `missing`. The handler runs under `h.auth`, so request auth/replay/allowlist are enforced before the walk, consistent with every other authenticated route.

**Risk:** Additive only. The walk reuses the Phase 3 parallel `ReachableKeys` (concurrency-safe with `packstore.Store.Get`). Failure modes: bad/short body → `422`; incomplete tree / store error → `500`; both verified (the second by `TestReachableKeys_IncompleteTreeErrors`).
