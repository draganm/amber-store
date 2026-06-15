# Remote Sync Redesign — Phase 5a: single push/pull operation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the four-step transfer (`push-objects` + `push-ref`, `pull-objects` + `pull-ref`) into two atomic operations: `push` transfers objects **and** the signed reference; `pull` transfers the reference **and** all its objects. One daemon endpoint, one client method, one CLI command per direction. Refs only.

**Architecture:** The daemon's `remotePush` resolves the local ref, reads/validates its signed record up front (so a missing/unsigned ref is a clean `404`/`422` before any work), pushes objects via `remotesync.Push` (streaming NDJSON progress), then `PutRef`s the record — all in one streamed response. `remotePull` fetches+verifies the remote record, pulls objects via `remotesync.Pull` (whose completeness gate guarantees the whole tree is local), then writes the record into the local refstore. The four old daemon handlers, client methods, and CLI subcommands are removed; `localRefKey` and `remoteRefAction` go with them.

**Tech Stack:** Go, the existing NDJSON `eventStream`/`syncEvent`, `remotesync.Push`/`Pull`, `reference`/`refstore`, `urfave/cli/v2`.

**Spec:** [docs/superpowers/specs/2026-06-15-pack-based-remote-sync-design.md](../specs/2026-06-15-pack-based-remote-sync-design.md) — "User and daemon surface".

**Base commit:** `f6c8e9c` (HEAD after Phase 4c).

---

## Roadmap context

The protocol redesign (Phases 1a, 2, 3, 4a, 4b, 4c) is done. This is **Phase 5a** (the user-facing merge). **Phase 5b** rewrites `architecture/remote.md`.

## What changes, end to end

- **Daemon** (`daemon/remotesync.go`, `daemon/daemon.go`): `remotePushObjects`+`remotePushRef` → `remotePush`; `remotePullObjects`+`remotePullRef` → `remotePull`. Routes `push-objects`/`pull-objects`/`push-ref`/`pull-ref` → `push`/`pull`. `localRefKey` deleted (the new `remotePush` reads the raw record directly).
- **Client** (`client/remote.go`): `RemotePushObjects`→`RemotePush`, `RemotePullObjects`→`RemotePull`; `RemotePushRef`/`RemotePullRef`/`remoteRefAction` deleted.
- **CLI** (`cmd/amber-store/remote.go`): the four subcommands → `push`/`pull` via a new `remotePushPullCommand`; `remoteSyncCommand`/`remoteRefCommand` deleted.
- **Tests** (`daemon/remotesync_test.go`, `cmd/amber-store/remote_test.go`): updated to the merged operations; the now-impossible `TestPullRefBeforeObjectsConflicts` is removed (pull is atomic).

This is one atomic change — the build is red until daemon, client, and CLI agree, so it lands in a single commit.

---

## Task 1: Merge objects+ref into single push/pull across daemon, client, and CLI

**Files:**
- Edit: `daemon/remotesync.go`, `daemon/daemon.go`, `client/remote.go`, `cmd/amber-store/remote.go`
- Edit (tests): `daemon/remotesync_test.go`, `cmd/amber-store/remote_test.go`

- [ ] **Step 1: `daemon/remotesync.go` — replace the four handlers and delete `localRefKey`**

Delete `localRefKey` (lines ~105–116), `remotePushObjects`, `remotePushRef`, `remotePullObjects`, `remotePullRef`. Keep `remoteFor`, `syncOpts`, the `syncEvent`/`eventStream`/`pushStatsLine`/`pullStatsLine` types, `fetchAndVerifyRemoteRef`, and `remoteLsRefs`. Add these two handlers:

```go
// remotePush pushes everything reachable from the local ref ?name= to the
// remote and then uploads the signed reference — one operation, streaming
// NDJSON progress. The signed record is read and validated up front, so a
// missing or unsigned reference is a clean 404/422 before any object moves.
func (h *handler) remotePush(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	raw, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		http.Error(w, "reference is not signed — configure a signing key and re-create it", http.StatusUnprocessableEntity)
		return
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Push(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("push failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	// Objects are up; push the signed reference. The server re-verifies it and
	// the no-dangling rule, which now holds.
	if err := rc.PutRef(r.Context(), name, raw); err != nil {
		h.log.Warn("push reference failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("push complete", "name", name, "pushed", stats.ObjectsPushed)
	stream.send(syncEvent{PushStats: &pushStatsLine{
		ObjectsTotal:  stats.ObjectsTotal,
		ObjectsPushed: stats.ObjectsPushed,
		BytesPushed:   stats.BytesPushed,
	}})
}

// remotePull resolves ?name= on the REMOTE (so it works before any local ref
// exists), pulls everything reachable from its key, and then writes the
// verified reference into the local refstore — one operation, streaming NDJSON
// progress. Pull's completeness gate guarantees the whole tree is local before
// the reference is written (the no-dangling rule).
func (h *handler) remotePull(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	raw, rec, status, err := fetchAndVerifyRemoteRef(r, rc, name)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Pull(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("pull failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	if err := h.refs.Put(name, raw); err != nil {
		h.log.Warn("pull reference write failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("pull complete", "name", name, "fetched", stats.ObjectsFetched, "key", root)
	stream.send(syncEvent{Key: root.String(), PullStats: &pullStatsLine{
		ObjectsFetched: stats.ObjectsFetched,
		BytesFetched:   stats.BytesFetched,
	}})
}
```

Also update the `syncEvent` doc comment (line ~62) from "a push/pull-objects response" to "a push/pull response". If `go build` reports any import in this file now unused, remove it (none should be — `errors`, `key`, `reference`, `refstore`, `remotesync`, `sshsign` are all still used by the kept functions).

- [ ] **Step 2: `daemon/daemon.go` — update routes**

Replace these four lines:

```go
		mux.HandleFunc("POST /v1/remote/push-objects", h.remotePushObjects)
		mux.HandleFunc("POST /v1/remote/pull-objects", h.remotePullObjects)
		mux.HandleFunc("POST /v1/remote/push-ref", h.remotePushRef)
		mux.HandleFunc("POST /v1/remote/pull-ref", h.remotePullRef)
```

with:

```go
		mux.HandleFunc("POST /v1/remote/push", h.remotePush)
		mux.HandleFunc("POST /v1/remote/pull", h.remotePull)
```

(Leave the `GET /v1/remote/refs` line and the `NewWithRemotes` doc comment referencing `/v1/remote/*` as-is.)

- [ ] **Step 3: `client/remote.go` — replace the object/ref methods with `RemotePush`/`RemotePull`**

Delete `RemotePushObjects`, `RemotePullObjects`, `remoteRefAction`, `RemotePushRef`, `RemotePullRef`. Keep `runSync`, `syncQuery`, `PushStats`, `PullStats`, `syncEvent`. Add:

```go
// RemotePush pushes the objects reachable from local ref name AND the signed
// reference to the remote — one streamed operation.
func (c *Client) RemotePush(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PushStats, error) {
	ev, err := c.runSync(ctx, "/v1/remote/push"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PushStats{}, err
	}
	if ev.PushStats == nil {
		return PushStats{}, fmt.Errorf("sync stream ended without push stats")
	}
	return *ev.PushStats, nil
}

// RemotePull fetches the reference name from the remote AND every object it
// reaches into the local store — one streamed operation; returns the resolved
// root key (hex).
func (c *Client) RemotePull(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PullStats, string, error) {
	ev, err := c.runSync(ctx, "/v1/remote/pull"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PullStats{}, "", err
	}
	if ev.PullStats == nil {
		return PullStats{}, "", fmt.Errorf("sync stream ended without pull stats")
	}
	return *ev.PullStats, ev.Key, nil
}
```

- [ ] **Step 4: `cmd/amber-store/remote.go` — replace the four subcommands with `push`/`pull`**

In `remoteCommand`, replace the four `remoteSyncCommand`/`remoteRefCommand` lines with:

```go
			remotePushPullCommand("push"),
			remotePushPullCommand("pull"),
```

Delete `remoteSyncCommand` and `remoteRefCommand` entirely and add:

```go
// remotePushPullCommand builds the push / pull commands (same flags and arg
// shape; each performs the whole objects+reference transfer in one operation).
func remotePushPullCommand(name string) *cli.Command {
	var socket string
	var jobs int
	var batchBytes uint64
	usage := "push the local reference NAME and all its objects to the remote"
	if name == "pull" {
		usage = "pull the reference NAME and all its objects from the remote"
	}
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "[REMOTE] NAME",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.IntFlag{
				Name:        "jobs",
				Aliases:     []string{"j"},
				Value:       4,
				Usage:       "parallel transfer workers",
				Destination: &jobs,
			},
			&cli.Uint64Flag{
				Name:        "batch-bytes",
				Value:       8 << 20,
				Usage:       "per-batch payload target in bytes",
				Destination: &batchBytes,
			},
		},
		Action: func(c *cli.Context) error {
			remote, refName, err := remoteAndName(c, "remote "+name)
			if err != nil {
				return err
			}
			cl := client.New(socketpath.Resolve(socket))
			progress := func(done, total int) {
				if total > 0 {
					fmt.Fprintf(os.Stderr, "\r%s: %d/%d objects", name, done, total)
				} else {
					fmt.Fprintf(os.Stderr, "\r%s: %d objects", name, done)
				}
			}
			defer fmt.Fprintln(os.Stderr)
			if name == "push" {
				stats, err := cl.RemotePush(c.Context, remote, refName, jobs, batchBytes, progress)
				if err != nil {
					return err
				}
				fmt.Fprintf(c.App.Writer, "pushed %s: %d objects (%d bytes), %d already present\n",
					refName, stats.ObjectsPushed, stats.BytesPushed, stats.ObjectsTotal-stats.ObjectsPushed)
				return nil
			}
			stats, rootKey, err := cl.RemotePull(c.Context, remote, refName, jobs, batchBytes, progress)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "pulled %s: %d objects (%d bytes), root %s\n",
				refName, stats.ObjectsFetched, stats.BytesFetched, rootKey)
			return nil
		},
	}
}
```

Update the `remote` command `Usage` string at the top if it names the old steps (it currently reads "manage remote servers and push/pull objects and references" — that is still accurate; leave it).

- [ ] **Step 5: Build**

Run: `go build ./...`
Expected: compiles. Fix any leftover reference to a deleted symbol (`grep -rn 'RemotePushObjects\|RemotePullObjects\|RemotePushRef\|RemotePullRef\|remoteRefAction\|localRefKey\|remoteSyncCommand\|remoteRefCommand' --include='*.go' .` should return only matches inside test files you update next, or nothing).

- [ ] **Step 6: Update `daemon/remotesync_test.go`**

Rewrite the endpoint calls to the merged operations and delete the now-impossible conflict test.

- `TestPushPullRoundTripThroughDaemon`: replace the `push-objects` + `push-ref` pair with a single `POST /v1/remote/push?remote=origin&name=backup` (assert 200, no `error`, `push_stats.objects_pushed == 4`, and `h.srvRefs.Get("backup")` succeeds). Replace the `pull-objects` + `pull-ref` pair with a single `POST /v1/remote/pull?remote=origin&name=backup` (assert 200, no `error`), then keep the existing assertions that `h2.refs.Get("backup")` decodes to `root` and the local tree has 4 reachable keys. Concretely the body becomes:

```go
func TestPushPullRoundTripThroughDaemon(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "backup", root, signer)

	// push: objects + signed reference in one operation
	code, body := h.doReq(t, "POST", "/v1/remote/push?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("push = %d: %s", code, body)
	}
	ev := lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("push error: %v", ev["error"])
	}
	ps, ok := ev["push_stats"].(map[string]any)
	if !ok || ps["objects_pushed"].(float64) != 4 {
		t.Fatalf("final event = %v, want 4 objects pushed", ev)
	}
	if _, err := h.srvRefs.Get("backup"); err != nil {
		t.Fatalf("ref not on server after push: %v", err)
	}

	// a second daemon pulls everything back in one operation
	h2 := newRemoteHarnessWithServer(t, h, h.clientSigner)
	h2.addRemote(t, "origin")
	code, body = h2.doReq(t, "POST", "/v1/remote/pull?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("pull = %d: %s", code, body)
	}
	ev = lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("pull error: %v", ev["error"])
	}
	rec, err := h2.refs.Get("backup")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := reference.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if string(ref.Key) != string(root[:]) {
		t.Fatal("pulled ref points elsewhere")
	}
	if keys, err := fstree.ReachableKeys(root, h2.store.Get); err != nil || len(keys) != 4 {
		t.Fatalf("pulled tree incomplete: %d keys, %v", len(keys), err)
	}
}
```

- `TestPushRefRejectsUnsigned` → rename to `TestPushRejectsUnsignedRef`; change the request to `POST /v1/remote/push?remote=origin&name=plain` and keep the assertion `code == 422 && body contains "signed"` (the merged `remotePush` validates the signature before any object transfer).
- `TestPullRefBeforeObjectsConflicts`: **delete this test** — pull is now atomic, so the "ref before objects" `409` state no longer exists.
- `TestRemoteLsRefs`: replace the `push-objects` + `push-ref` calls with one `POST /v1/remote/push?remote=origin&name=listme`, then keep the `GET /v1/remote/refs` assertion.
- `TestUnknownRemoteAndRef`: change both `push-objects` requests to `push` (unknown remote → 404; unknown local ref → 404 — both still hold, as `remotePush` resolves the remote then reads the ref before streaming).

- [ ] **Step 7: Update `cmd/amber-store/remote_test.go`**

These are mechanical command substitutions (the new CLI prints "pushed …" and "pulled …, root …", so the existing `strings.Contains(out, "pushed")` and `strings.Contains(out, "root ")` assertions still hold):

- `TestRemotePushPullCycle`: replace `"remote", "push-objects", …` with `"remote", "push", …`; delete the separate `"remote", "push-ref", …` call (push now does both). Replace `"remote", "pull-objects", …` with `"remote", "pull", …`; delete the separate `"remote", "pull-ref", …` call.
- `TestRemoteArgParsing`: replace both `"remote", "push-objects", …` invocations with `"remote", "push", …` (the no-arg and 3-arg error assertions are unchanged).
- `TestRemotePushWithAutoIdentity`: replace `"remote", "push-objects", …` with `"remote", "push", …`.

- [ ] **Step 8: Confirm no dangling references**

Run: `grep -rn 'push-objects\|pull-objects\|push-ref\|pull-ref\|RemotePushObjects\|RemotePullObjects\|RemotePushRef\|RemotePullRef\|remoteRefAction\|localRefKey\|remoteSyncCommand\|remoteRefCommand' --include='*.go' .`
Expected: NO hits. (Doc-comment mentions are fine to leave only if they describe history accurately, but prefer none — report any remaining.)

- [ ] **Step 9: Full suite**

Run: `go build ./... && go vet ./daemon/ ./client/ ./cmd/amber-store/ && go test ./...`
Expected: every package PASSES, including the rewritten `daemon` and `cmd/amber-store` tests.

- [ ] **Step 10: Commit**

```bash
git add daemon/remotesync.go daemon/daemon.go client/remote.go cmd/amber-store/remote.go daemon/remotesync_test.go cmd/amber-store/remote_test.go
git commit -m "remote: single push/pull operation (objects + reference, refs only)"
```

---

## Self-review

**Spec coverage:** Implements the spec's "User and daemon surface": `push`/`pull` CLI (refs only), one daemon endpoint each (`/v1/remote/push`, `/v1/remote/pull`) performing objects+ref, and the removal of `push-objects`/`pull-objects`/`push-ref`/`pull-ref`. The push ordering (objects then ref) preserves no-dangling; pull writes the ref only after the completeness gate.

**Placeholder scan:** None — full handler/method/command code, exact route and test edits, runnable commands.

**Type/name consistency:** `RemotePush`/`RemotePull` keep the `(ctx, remote, name, jobs, batchBytes, onProgress)` shape and `PushStats`/`(PullStats, key)` returns of the methods they replace, so the CLI wiring is a direct swap. The daemon handlers reuse `remoteFor`/`syncOpts`/`fetchAndVerifyRemoteRef`/`eventStream`/`pushStatsLine`/`pullStatsLine` unchanged. `localRefKey`/`remoteRefAction`/`remoteSyncCommand`/`remoteRefCommand` removed.

**Risk:** The user-visible CLI surface changes (breaking: four commands become two) — this is the intended, specced behavior. Error surfacing: a missing/unsigned local ref or unknown remote fails before the stream (clean HTTP status); a server-side ref rejection (e.g. ownership) after objects are pushed surfaces as a stream error event (the operation is necessarily objects-then-ref). All existing behaviors are covered by the rewritten tests; the only deleted test asserted a state (`pull-ref` before objects) that the atomic `pull` makes unreachable.
