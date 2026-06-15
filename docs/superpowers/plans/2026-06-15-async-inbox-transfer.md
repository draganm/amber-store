# Async Inbox Transfer — Push Side Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the remote server receive pushed packs into a durable inbox and ack 200 immediately, processing them into the packstore asynchronously, with `PUT /v1/refs` waiting on the relevant packs before its completeness check.

**Architecture:** A new `inbox` package owns a directory of self-describing pack files (`[u32 meta-len][meta CBOR][amberpack body]`), atomic content-addressed commit, crash recovery, a processing-worker pool that drains entries into `*packstore.Store`, and a per-root barrier. The remote server's `POST /v1/objects` streams the body into the inbox (no full-body RAM buffer), authenticates against the streamed hash, and returns 200 on durable commit. `putRef` calls `inbox.WaitFor(root)` before `fstree.CheckComplete`. A corrupt pack is quarantined, so its objects stay absent and `CheckComplete` reports the ref incomplete — the client re-pushes.

**Tech Stack:** Go, `github.com/fxamacker/cbor/v2`, `github.com/zeebo/blake3`, `golang.org/x/sync/errgroup` (existing deps). No new dependencies.

**Scope:** Push (server-receiving) side only. The pull/client side gets the same treatment in a follow-up plan — it depends on this `inbox` package and is independently shippable. Pull is unaffected by this plan and keeps working: it writes via `WriteParallel` and sets the local ref directly, never through the server barrier.

---

## File Structure

- **Create** `inbox/entry.go` — `Meta` type and the framed-CBOR-header codec (`writeMetaHeader` / `readMetaHeader`).
- **Create** `inbox/entry_test.go` — header round-trip.
- **Create** `inbox/inbox.go` — `Inbox` type: `Open`, `Stage`, `Discard`, `Commit`, `WaitFor`, `Close`, recovery, processing pool.
- **Create** `inbox/inbox_test.go` — commit→process, idempotency, recovery, quarantine, barrier.
- **Modify** `internal/httpsig/httpsig.go` — add `VerifyRequestHash`; refactor `VerifyRequest` to call it.
- **Modify** `internal/httpsig/httpsig_test.go` — test for `VerifyRequestHash`.
- **Modify** `server/server.go` — `Config.Inbox`, `handler.inbox`, register `POST /v1/objects` without the buffering `auth` wrapper.
- **Modify** `server/objects.go` — rewrite `postObjects` as streaming receive→stage→auth→commit; drop `uploadResponse`.
- **Modify** `server/refs.go` — `putRef` calls `h.inbox.WaitFor(root)` before `CheckComplete`.
- **Modify** `server/server_test.go` — construct an `Inbox` in `newTestServer`; helpers to await processing.
- **Modify** `server/objects_test.go` — adapt push tests to async (200 then await).
- **Modify** `remoteclient/objects.go` — `PushPack(ctx, ref, root, objs) error` (drop `Stats` return; add `?ref=&root=` query).
- **Modify** `remoteclient/objects_test.go` — adapt `PushPack` call sites.
- **Modify** `remotesync/push.go` — `Push(ctx, store, rc, name, root, opts)`; forward `name`/`root` to `PushPack`.
- **Modify** `remotesync/push_test.go` and `remotesync/harness_test.go` — adapt `Push` call sites.
- **Modify** `daemon/remotesync.go:153` — pass `name` to `remotesync.Push`.
- **Modify** `cmd/amber-store/serve.go` — construct `inbox.Open(...)`, `defer Close`, pass `Inbox:` into `server.Config`.

---

## Phase 1 — The `inbox` package

### Task 1: Entry header codec

**Files:**
- Create: `inbox/entry.go`
- Test: `inbox/entry_test.go`

- [ ] **Step 1: Write the failing test**

```go
package inbox

import (
	"bytes"
	"testing"
)

func TestMetaHeaderRoundTrip(t *testing.T) {
	root := make([]byte, 32)
	for i := range root {
		root[i] = byte(i)
	}
	in := Meta{Ref: "site", Root: root, ReceivedAt: 1234567890}
	var buf bytes.Buffer
	if err := writeMetaHeader(&buf, in); err != nil {
		t.Fatalf("writeMetaHeader: %v", err)
	}
	// Append a fake body; readMetaHeader must stop right after the header.
	buf.WriteString("BODYBYTES")
	out, err := readMetaHeader(&buf)
	if err != nil {
		t.Fatalf("readMetaHeader: %v", err)
	}
	if out.Ref != in.Ref || out.ReceivedAt != in.ReceivedAt || !bytes.Equal(out.Root, in.Root) {
		t.Fatalf("round-trip mismatch: got %+v want %+v", out, in)
	}
	if rest := buf.String(); rest != "BODYBYTES" {
		t.Fatalf("reader left body wrong: %q", rest)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./inbox/ -run TestMetaHeaderRoundTrip`
Expected: FAIL — `undefined: Meta`, `writeMetaHeader`, `readMetaHeader`.

- [ ] **Step 3: Write `inbox/entry.go`**

```go
// Package inbox stores authenticated packs that have been received but not yet
// processed into the packstore. An entry is a single self-describing file: a
// CBOR meta header framed by its big-endian length, followed by the raw
// amberpack body. Entries are content-addressed by blake3 of the body, so a
// re-received identical pack is idempotent. A pool of workers drains the
// directory into the store; setting a reference waits on the entries tagged
// with that root. The directory is the only durable state — on restart a scan
// rebuilds the in-memory view and resumes processing.
package inbox

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// Meta tags a staged pack with the (ref, root) it belongs to and when it
// arrived. The barrier keys on Root alone; Ref and ReceivedAt are carried for
// operability (which ref a pending pack was for, how old it is).
type Meta struct {
	Ref        string `cbor:"0,keyasint"`
	Root       []byte `cbor:"1,keyasint"` // 32-byte root key
	ReceivedAt int64  `cbor:"2,keyasint"` // ns since the Unix epoch
}

// encMode mirrors the deterministic CBOR conventions used across the project
// (reference, httpsig, fstree).
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("inbox: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// writeMetaHeader writes [u32 BE meta-len][meta CBOR] to w.
func writeMetaHeader(w io.Writer, m Meta) error {
	b, err := encMode.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding inbox meta: %w", err)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// readMetaHeader reads a header written by writeMetaHeader and leaves r
// positioned at the first body byte.
func readMetaHeader(r io.Reader) (Meta, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Meta{}, fmt.Errorf("reading inbox meta length: %w", err)
	}
	buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return Meta{}, fmt.Errorf("reading inbox meta: %w", err)
	}
	var m Meta
	if err := cbor.Unmarshal(buf, &m); err != nil {
		return Meta{}, fmt.Errorf("decoding inbox meta: %w", err)
	}
	return m, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./inbox/ -run TestMetaHeaderRoundTrip`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add inbox/entry.go inbox/entry_test.go
git commit -m "inbox: entry meta-header codec"
```

---

### Task 2: Inbox commit → async processing

**Files:**
- Create: `inbox/inbox.go`
- Test: `inbox/inbox_test.go`

- [ ] **Step 1: Write the failing test**

This test uses a real `*packstore.Store` (the inbox writes into one) and a helper that builds an amberpack body for a single blob object. `key.NewFromHash`-style construction is avoided by using the store's own ingest path is overkill — instead we hand-build a blob key the way `verifyObject` expects: a `Blob`-type key whose hash is `blake3(data)` and whose length field equals `len(data)`. Use the existing `amberpack`/`key`/`blake3` packages.

```go
package inbox

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/zeebo/blake3"
)

// blobObject builds a valid Blob object (key = blake3(data), length = len(data))
// that packstore's Verify accepts.
func blobObject(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	sum := blake3.Sum256(data)
	k, err := key.NewFromHash(key.Blob, uint64(len(data)), sum)
	if err != nil {
		t.Fatalf("NewFromHash: %v", err)
	}
	return fstree.Object{Key: k, Bytes: data}
}

// packBody serializes objs as an amberpack pack-stream body.
func packBody(t *testing.T, objs ...fstree.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			t.Fatalf("pack add: %v", err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pack close: %v", err)
	}
	return buf.Bytes()
}

func newTestStore(t *testing.T) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(filepath.Join(t.TempDir(), "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatalf("packstore.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCommitProcessesIntoStore(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(filepath.Join(t.TempDir(), "inbox"), store, 2, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	obj := blobObject(t, []byte("hello inbox"))
	root := obj.Key // a blob is its own reachable set
	body := packBody(t, obj)

	tmp, h, _, err := ib.Stage(Meta{Ref: "r", Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	added, err := ib.Commit(tmp, h, root)
	if err != nil || !added {
		t.Fatalf("Commit: added=%v err=%v", added, err)
	}

	ib.WaitFor(root)

	has, err := store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("object not stored after WaitFor: has=%v err=%v", has, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./inbox/ -run TestCommitProcessesIntoStore`
Expected: FAIL — `undefined: Open` (and `Stage`, `Commit`, `WaitFor`, `Close`).

> Note: if `key.NewFromHash` has a different name/signature, grep `key` for the constructor that builds a key from a type, length and 32-byte hash and use it; the test only needs a verify-valid blob key.

- [ ] **Step 3: Write `inbox/inbox.go`**

```go
package inbox

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/zeebo/blake3"
)

// Inbox receives packs, persists them durably, and drains them into a
// packstore from a worker pool. It is safe for concurrent use.
type Inbox struct {
	dir     string
	tmpDir  string
	failDir string
	store   *packstore.Store
	log     *slog.Logger

	mu     sync.Mutex
	cond   *sync.Cond
	work   []workItem
	groups map[key.Key]int // root -> count of unprocessed entries
	closed bool

	wg sync.WaitGroup
}

type workItem struct {
	name string
	root key.Key
}

// Open prepares the inbox directory tree, recovers entries left by a previous
// run, and starts `workers` processing goroutines. workers <= 0 means
// runtime.GOMAXPROCS(0). A nil log discards.
func Open(dir string, store *packstore.Store, workers int, log *slog.Logger) (*Inbox, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	ib := &Inbox{
		dir:     dir,
		tmpDir:  filepath.Join(dir, "tmp"),
		failDir: filepath.Join(dir, "failed"),
		store:   store,
		log:     log,
		groups:  map[key.Key]int{},
	}
	ib.cond = sync.NewCond(&ib.mu)
	for _, d := range []string{ib.dir, ib.tmpDir, ib.failDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := ib.recover(); err != nil {
		return nil, err
	}
	for range workers {
		ib.wg.Add(1)
		go ib.processLoop()
	}
	return ib, nil
}

// Stage writes meta and streams body into a fresh tmp file, returning the tmp
// path, blake3 of the body bytes (only the body feeds the hash), and the body
// length. The caller authorizes the request against the hash, then calls
// Commit or Discard.
func (ib *Inbox) Stage(meta Meta, body io.Reader) (tmpPath string, bodyHash []byte, n int64, err error) {
	f, err := os.CreateTemp(ib.tmpDir, "stage-*")
	if err != nil {
		return "", nil, 0, err
	}
	tmpPath = f.Name()
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(tmpPath)
		}
	}()
	if err := writeMetaHeader(f, meta); err != nil {
		return "", nil, 0, err
	}
	h := blake3.New()
	n, err = io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		return "", nil, 0, err
	}
	if err := f.Sync(); err != nil {
		return "", nil, 0, err
	}
	if err := f.Close(); err != nil {
		return "", nil, 0, err
	}
	committed = true
	return tmpPath, h.Sum(nil), n, nil
}

// Discard removes a staged tmp file (authorization failed or oversize).
func (ib *Inbox) Discard(tmpPath string) {
	_ = os.Remove(tmpPath)
}

// Commit publishes a staged tmp file under its content-addressed name and
// enqueues it. It is idempotent: if an entry with the same body already exists,
// the tmp file is discarded and added is false. root updates the barrier
// accounting and must equal the root staged into the file.
func (ib *Inbox) Commit(tmpPath string, bodyHash []byte, root key.Key) (added bool, err error) {
	name := hex.EncodeToString(bodyHash) + ".pack"
	dst := filepath.Join(ib.dir, name)
	switch _, statErr := os.Stat(dst); {
	case statErr == nil:
		ib.Discard(tmpPath)
		return false, nil
	case !errors.Is(statErr, os.ErrNotExist):
		return false, statErr
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return false, err
	}
	if err := syncDir(ib.dir); err != nil {
		return false, err
	}
	ib.mu.Lock()
	ib.groups[root]++
	ib.work = append(ib.work, workItem{name: name, root: root})
	ib.cond.Broadcast()
	ib.mu.Unlock()
	return true, nil
}

// WaitFor blocks until no entries tagged with root remain unprocessed. With an
// empty group it returns immediately.
func (ib *Inbox) WaitFor(root key.Key) {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	for ib.groups[root] > 0 {
		ib.cond.Wait()
	}
}

// Close stops accepting new work, drains what is already queued, and waits for
// the processing goroutines to exit. Staged-but-uncommitted tmp files are left
// for the next Open to sweep.
func (ib *Inbox) Close() error {
	ib.mu.Lock()
	ib.closed = true
	ib.cond.Broadcast()
	ib.mu.Unlock()
	ib.wg.Wait()
	return nil
}

// recover sweeps partial transfers and enqueues committed entries from a
// previous run.
func (ib *Inbox) recover() error {
	tmps, err := os.ReadDir(ib.tmpDir)
	if err != nil {
		return err
	}
	for _, e := range tmps {
		_ = os.Remove(filepath.Join(ib.tmpDir, e.Name()))
	}
	entries, err := os.ReadDir(ib.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		root, err := ib.readRoot(e.Name())
		if err != nil {
			ib.log.Error("inbox: unreadable entry on recovery, quarantining", "name", e.Name(), "error", err)
			_ = os.Rename(filepath.Join(ib.dir, e.Name()), filepath.Join(ib.failDir, e.Name()))
			continue
		}
		ib.groups[root]++
		ib.work = append(ib.work, workItem{name: e.Name(), root: root})
	}
	return nil
}

// readRoot reads just the meta header of an entry and returns its root key.
func (ib *Inbox) readRoot(name string) (key.Key, error) {
	f, err := os.Open(filepath.Join(ib.dir, name))
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()
	m, err := readMetaHeader(f)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(m.Root)
}

func (ib *Inbox) processLoop() {
	defer ib.wg.Done()
	for {
		ib.mu.Lock()
		for len(ib.work) == 0 && !ib.closed {
			ib.cond.Wait()
		}
		if len(ib.work) == 0 && ib.closed {
			ib.mu.Unlock()
			return
		}
		item := ib.work[0]
		ib.work = ib.work[1:]
		ib.mu.Unlock()

		ib.process(item.name)

		ib.mu.Lock()
		if n := ib.groups[item.root]; n > 0 {
			if n == 1 {
				delete(ib.groups, item.root)
			} else {
				ib.groups[item.root] = n - 1
			}
		}
		ib.cond.Broadcast()
		ib.mu.Unlock()
	}
}

// process ingests one entry into the store. On success the file is removed; on
// a decode/verify error it is quarantined under failed/.
func (ib *Inbox) process(name string) {
	path := filepath.Join(ib.dir, name)
	f, err := os.Open(path)
	if err != nil {
		ib.log.Error("inbox: opening entry failed", "name", name, "error", err)
		return
	}
	if _, err := readMetaHeader(f); err != nil {
		f.Close()
		ib.quarantine(name, err)
		return
	}
	rd := amberpack.NewReader(f) // positioned at the body
	seq := func(yield func(packstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(packstore.Object{}, err)
				return
			}
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	_, werr := ib.store.WriteParallel(seq, packstore.WriteOpts{Verify: true})
	f.Close()
	if werr != nil {
		ib.quarantine(name, werr)
		return
	}
	if err := os.Remove(path); err != nil {
		ib.log.Error("inbox: removing processed entry failed", "name", name, "error", err)
	}
}

func (ib *Inbox) quarantine(name string, cause error) {
	ib.log.Error("inbox: entry failed processing, quarantining", "name", name, "error", cause)
	if err := os.Rename(filepath.Join(ib.dir, name), filepath.Join(ib.failDir, name)); err != nil {
		ib.log.Error("inbox: quarantine rename failed", "name", name, "error", err)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./inbox/ -run TestCommitProcessesIntoStore`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add inbox/inbox.go inbox/inbox_test.go
git commit -m "inbox: durable staging, atomic commit, async processing, barrier"
```

---

### Task 3: Idempotent commit

**Files:**
- Test: `inbox/inbox_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestCommitIdempotent(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(filepath.Join(t.TempDir(), "inbox"), store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	obj := blobObject(t, []byte("dup payload"))
	root := obj.Key
	body := packBody(t, obj)

	tmp1, h1, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	added1, err := ib.Commit(tmp1, h1, root)
	if err != nil || !added1 {
		t.Fatalf("first commit: added=%v err=%v", added1, err)
	}
	tmp2, h2, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	added2, err := ib.Commit(tmp2, h2, root)
	if err != nil {
		t.Fatalf("second commit err: %v", err)
	}
	if added2 {
		t.Fatalf("second commit of identical body should report added=false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails — then passes**

Run: `go test ./inbox/ -run TestCommitIdempotent`
Expected: PASS already (logic exists). If it FAILS, fix `Commit`'s stat-exists branch.

- [ ] **Step 3: Commit**

```bash
git add inbox/inbox_test.go
git commit -m "inbox: test idempotent commit"
```

---

### Task 4: Crash recovery resumes a pre-existing entry

**Files:**
- Test: `inbox/inbox_test.go`

- [ ] **Step 1: Write the failing test**

This writes a committed entry file directly (simulating an entry left by a crashed prior run) and asserts a fresh `Open` processes it.

```go
import "os"

func TestRecoveryResumesEntry(t *testing.T) {
	store := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "inbox")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	obj := blobObject(t, []byte("left behind by a crash"))
	root := obj.Key
	body := packBody(t, obj)

	// Hand-write a committed entry: [meta header][body] at <hex(blake3(body))>.pack
	var entry bytes.Buffer
	if err := writeMetaHeader(&entry, Meta{Root: root[:]}); err != nil {
		t.Fatal(err)
	}
	entry.Write(body)
	sum := blake3.Sum256(body)
	name := hex.EncodeToString(sum[:]) + ".pack"
	if err := os.WriteFile(filepath.Join(dir, name), entry.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	ib, err := Open(dir, store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	ib.WaitFor(root)
	has, err := store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("recovered entry not processed: has=%v err=%v", has, err)
	}
}
```

Add `"encoding/hex"` to the test imports.

- [ ] **Step 2: Run test to verify it passes**

Run: `go test ./inbox/ -run TestRecoveryResumesEntry`
Expected: PASS (recovery logic exists). If FAIL, fix `recover()`.

- [ ] **Step 3: Commit**

```bash
git add inbox/inbox_test.go
git commit -m "inbox: test crash recovery resumes committed entries"
```

---

### Task 5: A corrupt pack is quarantined and unblocks the barrier

**Files:**
- Test: `inbox/inbox_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestCorruptPackQuarantined(t *testing.T) {
	store := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "inbox")
	ib, err := Open(dir, store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	// Valid meta, but the body is not a valid amberpack stream.
	root := blobObject(t, []byte("x")).Key
	garbage := []byte("NOT-AN-AMBERPACK-STREAM")
	tmp, h, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(garbage))
	if err != nil {
		t.Fatal(err)
	}
	added, err := ib.Commit(tmp, h, root)
	if err != nil || !added {
		t.Fatalf("Commit: added=%v err=%v", added, err)
	}

	// The barrier must release even though processing failed.
	ib.WaitFor(root)

	name := hex.EncodeToString(h) + ".pack"
	if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
		t.Fatalf("entry should have left the inbox dir; stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "failed", name)); err != nil {
		t.Fatalf("entry should be quarantined under failed/: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `go test ./inbox/ -run TestCorruptPackQuarantined`
Expected: PASS. If FAIL, ensure `process` quarantines on `WriteParallel` error and `processLoop` always decrements the group.

- [ ] **Step 3: Run the whole inbox package**

Run: `go test ./inbox/ -race`
Expected: PASS, no race warnings.

- [ ] **Step 4: Commit**

```bash
git add inbox/inbox_test.go
git commit -m "inbox: test corrupt pack quarantine and barrier release"
```

---

## Phase 2 — httpsig streaming verify

### Task 6: `VerifyRequestHash`

**Files:**
- Modify: `internal/httpsig/httpsig.go:117-157`
- Test: `internal/httpsig/httpsig_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestVerifyRequestHashMatchesVerifyRequest(t *testing.T) {
	signer := testSigner(t) // existing helper in this package's tests
	body := []byte("streamed body bytes")
	req, err := http.NewRequest(http.MethodPost, "http://x/v1/objects?root=abc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("nonce-1234567890")
	now := time.Unix(1000, 0)
	if err := SignRequest(req, signer, now.UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	pub, gotNonce, err := VerifyRequestHash(req, HashBody(body), now, DefaultWindow)
	if err != nil {
		t.Fatalf("VerifyRequestHash: %v", err)
	}
	if !bytes.Equal(gotNonce, nonce) {
		t.Fatalf("nonce mismatch")
	}
	if pub.Type() != signer.PublicKey().Type() {
		t.Fatalf("unexpected key type")
	}
}
```

> If the httpsig test file lacks a `testSigner` helper, add the same ed25519→ssh.Signer helper used in `server/server_test.go:22`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/httpsig/ -run TestVerifyRequestHashMatchesVerifyRequest`
Expected: FAIL — `undefined: VerifyRequestHash`.

- [ ] **Step 3: Refactor `VerifyRequest` to delegate**

Replace the body of `VerifyRequest` (httpsig.go:117-157) so it computes the hash and calls the new function, and add `VerifyRequestHash` holding the moved logic:

```go
// VerifyRequest checks r's Amber-* headers against body. See VerifyRequestHash.
func VerifyRequest(r *http.Request, body []byte, now time.Time, window time.Duration) (ssh.PublicKey, []byte, error) {
	return VerifyRequestHash(r, HashBody(body), now, window)
}

// VerifyRequestHash is VerifyRequest for callers that have already hashed the
// body (e.g. a streaming receiver). bodyHash must be blake3-256 of the exact
// request body bytes. It returns the claimed key and nonce; the caller still
// checks the nonce for replay and the key against an allowlist. The nonce is
// returned even on failure so error responses can be signed over it.
func VerifyRequestHash(r *http.Request, bodyHash []byte, now time.Time, window time.Duration) (ssh.PublicKey, []byte, error) {
	pubB64 := r.Header.Get(HeaderPublicKey)
	tsStr := r.Header.Get(HeaderTimestamp)
	nonceB64 := r.Header.Get(HeaderNonce)
	sigB64 := r.Header.Get(HeaderSignature)
	if pubB64 == "" || tsStr == "" || nonceB64 == "" || sigB64 == "" {
		return nil, nil, errors.New("request is not signed (missing Amber-* headers)")
	}
	pubWire, err := base64.StdEncoding.DecodeString(pubB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderPublicKey, err)
	}
	pub, err := ssh.ParsePublicKey(pubWire)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing public key: %w", err)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing %s: %w", HeaderTimestamp, err)
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderNonce, err)
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderSignature, err)
	}
	d := now.Sub(time.Unix(0, ts))
	if d < -window || d > window {
		return nil, nonce, fmt.Errorf("request timestamp outside the ±%s window", window)
	}
	payload, err := requestSigPayload(r.Method, r.URL.RequestURI(), ts, nonce, bodyHash)
	if err != nil {
		return nil, nonce, fmt.Errorf("encoding request signature payload: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, sig, pubWire, Namespace); err != nil {
		return nil, nonce, fmt.Errorf("request signature: %w", err)
	}
	return pub, nonce, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/httpsig/`
Expected: PASS (existing `VerifyRequest` tests still green via delegation).

- [ ] **Step 5: Commit**

```bash
git add internal/httpsig/httpsig.go internal/httpsig/httpsig_test.go
git commit -m "httpsig: add VerifyRequestHash for streaming receivers"
```

---

## Phase 3 — Server integration

### Task 7: Wire the inbox into the server handler

**Files:**
- Modify: `server/server.go:27-83` (Config, handler, New)

- [ ] **Step 1: Add `Inbox` to `Config` and `handler`**

In `Config` (after `MaxBody`):

```go
	MaxBody  int64         // request body cap; 0 = DefaultMaxBody
	Inbox    *inbox.Inbox  // receives pushed packs; required
```

In `handler` (after `maxBody`):

```go
	maxBody  int64
	inbox    *inbox.Inbox
```

In `New`, set it after `maxBody`:

```go
	h := &handler{
		store:    cfg.Store,
		refs:     cfg.Refs,
		allow:    cfg.Allow,
		identity: cfg.Identity,
		log:      log,
		window:   window,
		maxBody:  maxBody,
		inbox:    cfg.Inbox,
		nonces:   nonces.New(window),
	}
```

Change the objects route to **not** use the buffering `auth` wrapper (it self-authenticates from the streamed hash):

```go
	mux.HandleFunc("POST /v1/objects", h.postObjects)
```

Add the import `"github.com/draganm/amber-store/inbox"`.

- [ ] **Step 2: Build to confirm it compiles against the next task's `postObjects` signature**

Run: `go build ./server/`
Expected: FAIL — `postObjects` still has the old `(w, r, *authedRequest)` signature. Proceed to Task 8 (same logical change set).

---

### Task 8: Streaming `postObjects` (receive → stage → auth → commit)

**Files:**
- Modify: `server/objects.go:20-87` (rewrite `postObjects`, drop `uploadResponse`)
- Test: `server/objects_test.go`

- [ ] **Step 1: Write the failing test**

Extend the test server (see Task 10 for `newTestServer` changes) with an `awaitRoot` helper, then test the async push:

```go
func TestPostObjectsStagesAndStores(t *testing.T) {
	ts := newTestServer(t)

	obj := blobObject(t, []byte("pushed via async inbox")) // reuse helper; see note
	root := obj.Key
	body := packBody(t, obj)

	q := "/v1/objects?ref=site&root=" + root.String()
	status, _ := ts.signedDo(t, ts.client, http.MethodPost, q, body)
	if status != http.StatusOK {
		t.Fatalf("push status = %d, want 200", status)
	}

	ts.inbox.WaitFor(root)
	has, err := ts.store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("object not stored after WaitFor: has=%v err=%v", has, err)
	}
}
```

> Note: `blobObject`/`packBody` live in the `inbox` test package. Copy small equivalents into a `server` test helper file (`server/helpers_test.go`) — they only need `amberpack`, `fstree`, `key`, `blake3`. Do not import test code across packages.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestPostObjectsStagesAndStores`
Expected: FAIL to compile (`ts.inbox` undefined) until Task 10; then FAIL on behavior. Implement Steps 3–4, then Task 10, then re-run.

- [ ] **Step 3: Rewrite `postObjects`**

Replace `uploadResponse` (objects.go:20-25) and `postObjects` (objects.go:45-87) with:

```go
// postObjects receives a pushed pack. It streams the body into the inbox while
// hashing it, authenticates the request against that hash (the body is never
// fully buffered, so a pack is bounded by disk, not memory), and — once the
// pack is durably staged — returns 200. Processing into the store happens
// asynchronously; setting the ref waits for it (refs.go).
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request) {
	rootHex := r.URL.Query().Get("root")
	rootBytes, err := hex.DecodeString(rootHex)
	if err != nil {
		h.signError(w, nil, http.StatusUnprocessableEntity, "invalid root query parameter: "+err.Error())
		return
	}
	root, err := key.Parse(rootBytes)
	if err != nil {
		h.signError(w, nil, http.StatusUnprocessableEntity, "invalid root key: "+err.Error())
		return
	}
	meta := inbox.Meta{Ref: r.URL.Query().Get("ref"), Root: root[:], ReceivedAt: time.Now().UnixNano()}
	tmp, bodyHash, n, err := h.inbox.Stage(meta, io.LimitReader(r.Body, h.maxBody+1))
	if err != nil {
		h.signError(w, nil, http.StatusInternalServerError, err.Error())
		return
	}
	if n > h.maxBody {
		h.inbox.Discard(tmp)
		h.signError(w, nil, http.StatusRequestEntityTooLarge, "request body exceeds the server limit")
		return
	}
	now := time.Now()
	pub, nonce, err := httpsig.VerifyRequestHash(r, bodyHash, now, h.window)
	if err != nil {
		h.inbox.Discard(tmp)
		h.log.Warn("request authentication failed", "error", err)
		h.signError(w, nonce, http.StatusUnauthorized, err.Error())
		return
	}
	if h.nonces.SeenBefore(ssh.FingerprintSHA256(pub), nonce, now) {
		h.inbox.Discard(tmp)
		h.log.Warn("replayed nonce", "key", ssh.FingerprintSHA256(pub))
		h.signError(w, nonce, http.StatusUnauthorized, "replayed nonce")
		return
	}
	if _, ok := h.allow().Lookup(pub.Marshal()); !ok {
		h.inbox.Discard(tmp)
		h.log.Warn("key not allowed", "key", ssh.FingerprintSHA256(pub))
		h.signError(w, nonce, http.StatusForbidden, "public key is not in the server allowlist")
		return
	}
	if _, err := h.inbox.Commit(tmp, bodyHash, root); err != nil {
		h.log.Error("inbox commit failed", "error", err)
		h.signError(w, nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("pack accepted", "root", root, "bytes", n)
	h.signAndWrite(w, nonce, http.StatusOK, "application/json", []byte(`{"accepted":true}`))
}
```

Update `server/objects.go` imports: add `"github.com/draganm/amber-store/inbox"`, `"golang.org/x/crypto/ssh"`, `"time"`; drop now-unused `"bytes"`, `"encoding/json"` if nothing else uses them (it does not after removing `uploadResponse`). Keep `"errors"`, `"io"`, `"strings"`, `amberpack`, `fstree`, `httpsig`, `keylist`, `key`, `packstore`, `blake3` — verify with `go build` and fix.

- [ ] **Step 4: Build**

Run: `go build ./server/`
Expected: PASS once Task 7 + Task 10 changes are in. Fix any unused-import errors reported.

- [ ] **Step 5: Commit (with Task 7, 9, 10 — they form one compiling unit)**

Defer the commit to Task 10 Step 5 so the package compiles.

---

### Task 9: `putRef` waits on the inbox barrier

**Files:**
- Modify: `server/refs.go:83-91`

- [ ] **Step 1: Insert the barrier before `CheckComplete`**

In `putRef`, after `k, err := key.Parse(rec.Key)` succeeds (refs.go:83-87), and before the `CheckComplete` call (refs.go:91):

```go
	k, err := key.Parse(rec.Key)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	// Uploads are acked as soon as they are durably staged, not stored. Wait for
	// any packs tagged with this root to finish processing before checking
	// completeness; a pack that failed to process was quarantined, so its
	// objects stay absent and CheckComplete reports the ref incomplete.
	h.inbox.WaitFor(k)
	// The referenced content must be complete: every object reachable from
	// the key must exist in the store. The walk runs parallel lookups —
	// referenced trees can be large.
	err = fstree.CheckComplete(k, h.store.Get, h.store.Has, 0)
```

- [ ] **Step 2: Build**

Run: `go build ./server/`
Expected: PASS (with Tasks 7, 8, 10).

---

### Task 10: Test harness — construct the inbox in `newTestServer`

**Files:**
- Modify: `server/server_test.go:38-76` (`testServer` struct + `newTestServer`)
- Create: `server/helpers_test.go` (the `blobObject`/`packBody` helpers)

- [ ] **Step 1: Add the inbox to the test server**

In `testServer` add a field:

```go
type testServer struct {
	srv      *httptest.Server
	store    *packstore.Store
	refs     *refstore.Store
	inbox    *inbox.Inbox
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}
```

In `newTestServer`, after `refs` is opened and before `server.New`, construct and register the inbox:

```go
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ib.Close() })
```

Pass it into the config and store it on the struct:

```go
	h := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
		Inbox:    ib,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testServer{srv: srv, store: store, refs: refs, inbox: ib, identity: identity, client: client, admin: admin}
```

Add imports `"github.com/draganm/amber-store/inbox"` and `"path/filepath"` if not already present.

- [ ] **Step 2: Create `server/helpers_test.go`**

```go
package server_test

import (
	"bytes"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

func blobObject(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	sum := blake3.Sum256(data)
	k, err := key.NewFromHash(key.Blob, uint64(len(data)), sum)
	if err != nil {
		t.Fatalf("NewFromHash: %v", err)
	}
	return fstree.Object{Key: k, Bytes: data}
}

func packBody(t *testing.T, objs ...fstree.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			t.Fatalf("pack add: %v", err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pack close: %v", err)
	}
	return buf.Bytes()
}
```

- [ ] **Step 3: Adapt any existing push test in `server/objects_test.go`**

Existing tests that pushed via `POST /v1/objects` and asserted on the JSON stats response or immediate storage must change to: push with the `?ref=&root=` query, expect `200` with `{"accepted":true}`, then `ts.inbox.WaitFor(root)` before asserting `ts.store.Has(...)`. Update each such test accordingly (the exact set is whatever currently references `/v1/objects` in `objects_test.go`).

- [ ] **Step 4: Run the server tests**

Run: `go test ./server/ -race`
Expected: PASS, no races.

- [ ] **Step 5: Commit Phase 3 as one unit**

```bash
git add server/
git commit -m "server: receive pushes into a durable inbox, process async, gate ref-set on the barrier"
```

---

## Phase 4 — Client and orchestration

### Task 11: `PushPack` sends `ref`/`root`, returns only error

**Files:**
- Modify: `remoteclient/objects.go:37-57`
- Test: `remoteclient/objects_test.go`

- [ ] **Step 1: Update the test call site(s)**

Wherever `objects_test.go` calls `PushPack`, change to the new signature and assert success/storage via the server's barrier (or, for a unit test against a stub, just that the request carries the query). Example shape:

```go
err := client.PushPack(ctx, "site", root, objs)
if err != nil {
	t.Fatalf("PushPack: %v", err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./remoteclient/ -run PushPack`
Expected: FAIL — signature mismatch.

- [ ] **Step 3: Rewrite `PushPack`**

```go
// PushPack uploads objs as one amberpack to the remote, tagged with the (ref,
// root) the objects belong to. The server stages the pack durably and acks
// before processing it; this call returns once the pack is accepted, not once
// it is stored. Completeness is enforced when the reference is set.
func (c *Client) PushPack(ctx context.Context, ref string, root key.Key, objs []fstree.Object) error {
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	q := url.Values{}
	q.Set("ref", ref)
	q.Set("root", root.String())
	_, _, err := c.do(ctx, http.MethodPost, "/v1/objects?"+q.Encode(), "application/octet-stream", buf.Bytes())
	return err
}
```

Update `remoteclient/objects.go` imports: add `"net/url"`; drop `"encoding/json"` and the `Stats` decode if `Stats` is now unused in this file (verify `Stats` is not referenced elsewhere in the package before removing its decode; the type itself may still be used by other methods — only remove the unmarshal in `PushPack`).

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./remoteclient/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add remoteclient/
git commit -m "remoteclient: PushPack tags packs with ref+root, returns error only"
```

---

### Task 12: `remotesync.Push` threads the ref name

**Files:**
- Modify: `remotesync/push.go:57,124`
- Modify: `daemon/remotesync.go:153`
- Test: `remotesync/push_test.go`, `remotesync/harness_test.go`

- [ ] **Step 1: Update `Push` signature and the call to `PushPack`**

Signature (push.go:57):

```go
func Push(ctx context.Context, store *packstore.Store, rc *remoteclient.Client, name string, root key.Key, opts Opts) (PushStats, error) {
```

Upload loop (push.go:124):

```go
			if err := rc.PushPack(upCtx, name, root, objs); err != nil {
				return err
			}
```

- [ ] **Step 2: Update the daemon caller**

`daemon/remotesync.go:153`:

```go
	stats, err := remotesync.Push(r.Context(), h.store, rc, name, root, opts)
```

- [ ] **Step 3: Update test call sites**

In `remotesync/push_test.go` (and `harness_test.go` if it calls `Push`), pass a ref name (e.g. `"site"`) as the new argument. Where the harness builds an `httptest` server for the remote, it must now construct it with an `inbox.Inbox` too (reuse the `server` package construction pattern) and `WaitFor(root)`/`Close` to drain before asserting transferred objects exist on the remote store.

- [ ] **Step 4: Run**

Run: `go test ./remotesync/ -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add remotesync/ daemon/remotesync.go
git commit -m "remotesync: thread ref name into Push so packs carry their tag"
```

---

### Task 13: Construct the inbox in the remote server command

**Files:**
- Modify: `cmd/amber-store/serve.go:163-199`

- [ ] **Step 1: Open the inbox and pass it to the server**

After `store` is opened (serve.go:163-167) and before `server.New` (serve.go:192), add:

```go
	ib, err := inbox.Open(filepath.Join(cfg.store, "inbox"), store, 0, logger)
	if err != nil {
		return err
	}
	defer ib.Close()
```

Add `Inbox: ib,` to the `server.Config` literal:

```go
	handler := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    keys.Current,
		Identity: signer,
		Log:      logger,
		Window:   cfg.authWindow,
		Inbox:    ib,
	})
```

Add the import `"github.com/draganm/amber-store/inbox"`.

> Defer order: `store.Close()` is deferred at serve.go:167, `ib.Close()` is deferred later, so `ib.Close()` runs first (LIFO) — the inbox finishes draining into the store before the store closes. Correct.

- [ ] **Step 2: Build the whole tree**

Run: `go build ./...`
Expected: PASS. Fix any unused-import or call-site breakage surfaced here.

- [ ] **Step 3: Commit**

```bash
git add cmd/amber-store/serve.go
git commit -m "serve: open the inbox and wire it into the remote server"
```

---

## Phase 5 — Full verification

### Task 14: End-to-end push test

**Files:**
- Test: `server/objects_test.go` (or a new `server/push_e2e_test.go`)

- [ ] **Step 1: Write a test covering push-then-set-ref**

Build a small tree (a blob plus a dir node referencing it, using the existing fstree helpers used elsewhere in tests), push the packs via signed `POST /v1/objects?ref=&root=`, then `PUT /v1/refs` and assert:
- pushing returns 200,
- a `PUT /v1/refs` for the root succeeds (`204`) after the barrier drains,
- a `PUT /v1/refs` for a root whose objects were never pushed returns `404` ("incomplete").

```go
func TestPushThenSetRefWaitsForProcessing(t *testing.T) {
	ts := newTestServer(t)

	obj := blobObject(t, []byte("root blob"))
	root := obj.Key
	body := packBody(t, obj)

	if status, _ := ts.signedDo(t, ts.client, http.MethodPost,
		"/v1/objects?ref=site&root="+root.String(), body); status != http.StatusOK {
		t.Fatalf("push status = %d", status)
	}

	rec := signedRefRecord(t, ts.client, "site", root) // existing ref-building helper
	status, _ := ts.signedDo(t, ts.client, http.MethodPut, "/v1/refs?name=site", rec)
	if status != http.StatusNoContent {
		t.Fatalf("PUT ref status = %d, want 204", status)
	}
}

func TestSetRefIncompleteWithoutPush(t *testing.T) {
	ts := newTestServer(t)
	missing := blobObject(t, []byte("never pushed")).Key
	rec := signedRefRecord(t, ts.client, "site", missing)
	status, _ := ts.signedDo(t, ts.client, http.MethodPut, "/v1/refs?name=site", rec)
	if status != http.StatusNotFound {
		t.Fatalf("PUT ref status = %d, want 404", status)
	}
}
```

> Use the existing helper in `server`/`reference` tests that builds a signed `reference.Reference` and encodes it; if none exists, build one inline with `reference.Reference{Name, Key: root[:], User, CreatedAt}`, sign `SignaturePayload()` with `sshsign`, set `Signature`/`PublicKey`, and `Encode()`. Match the pattern already used in `server`'s ref tests.

- [ ] **Step 2: Run**

Run: `go test ./server/ -race -run 'Ref'`
Expected: PASS

- [ ] **Step 3: Run the full suite**

Run: `go test ./... -race`
Expected: PASS across the repo.

- [ ] **Step 4: Remove any build artifacts**

Run: `git status --porcelain` and delete any binaries the build produced (per the project rule: remove generated binaries). There should be none from `go test`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "server: end-to-end push + barrier-gated ref-set tests"
```

---

## Self-Review notes (carried into execution)

- **Spec coverage:** inbox format (Task 1–2), atomic commit (Task 2), per-root barrier keyed on root alone (Task 2 `WaitFor`, Task 9), durable startup recovery (Task 4), streaming receive removing the RAM cap (Task 8), error-via-ref-set (Task 5 quarantine + Task 14 incomplete test). **Deferred per spec:** backpressure/503 — not implemented; `maxBody` retained only as a disk-size guard. **Deviation from spec meta fields:** `Meta` carries `{Ref, Root, ReceivedAt}` only — `PubKey`/`Sig`/`Nonce` are dropped because the signature is verified *after* the body is staged (the meta header is written *before* auth runs), and persisting per-request replay nonces would be misleading; pusher identity is available via request logging. **Not in this plan:** the pull/client side (separate follow-up plan, depends on this package).
- **Type consistency:** `Stage` returns `(tmpPath string, bodyHash []byte, n int64, err error)` everywhere; `Commit(tmpPath, bodyHash, root)`; `WaitFor(root key.Key)`; `PushPack(ctx, ref string, root key.Key, objs []fstree.Object) error`; `Push(ctx, store, rc, name string, root key.Key, opts) (PushStats, error)`.
- **Open follow-ups (next plan):** pull-side inbox symmetry; inbox→packstore without recompression; disk-size backpressure.
```
