# Fast Parallel Ingest with Progress — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `amber-store ingest` faster on large trees (parallel batch writes + parallel pre-scan) and show a live progress bar with percentage, throughput, elapsed time, and ETA.

**Architecture:** A cheap parallel pre-scan (`scanTree`) counts total regular-file bytes to size a byte-driven progress bar. The already-parallel tree build is instrumented to report bytes/files via a lock-free `*Progress`. The serial single-batch write path is replaced by `diskstore.WriteParallel`: N writer goroutines, each accumulating its own bounded Pebble batch and committing concurrently, deduping through a shared sharded seen-set. The root key prints to stdout (`c.App.Writer`) after every batch flushes.

**Tech Stack:** Go 1.26, Pebble v2 (embedded KV), `golang.org/x/sync/errgroup`, urfave/cli v2. No new dependencies.

**Reference:** Design spec at `docs/superpowers/specs/2026-06-09-fast-parallel-ingest-design.md`.

**Verified facts (do not re-derive):**
- `key.Key` is `[32]byte`; `key.Size == 32`. It is comparable (usable as a map key). Its last byte (`k[key.Size-1]`) lies in the uniformly-distributed truncated-hash region — good for sharding.
- Pebble `(*Batch)` has `Len() int` (current batch size in bytes), `Empty() bool`, `Set(key, value []byte, *WriteOptions) error`, `Commit(*WriteOptions) error`, `Close() error`.
- The existing atomic `Store.WriteBatch` stays unchanged; `WriteParallel` is additive.
- Existing `ingestObjects` signature is `(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, root *key.Key)`. A `p *Progress` parameter is inserted before `root`.

---

## Task 1: Progress type and formatting helpers

**Files:**
- Create: `cmd/amber-store/progress.go`
- Test: `cmd/amber-store/progress_test.go`

- [ ] **Step 1: Write failing tests for the formatting helpers and render**

Create `cmd/amber-store/progress_test.go`:

```go
package main

import (
	"strings"
	"testing"
	"time"
)

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestFmtDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{23 * time.Second, "0:23"},
		{62 * time.Second, "1:02"},
		{3723 * time.Second, "1:02:03"},
		{-5 * time.Second, "0:00"},
	}
	for _, c := range cases {
		if got := fmtDuration(c.d); got != c.want {
			t.Errorf("fmtDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestProgressRender(t *testing.T) {
	p := NewProgress(10, 1000)
	p.AddBytes(500)
	for range 5 {
		p.FileDone()
	}
	line := p.render(10 * time.Second)
	for _, want := range []string{"50.0%", "files 5/10"} {
		if !strings.Contains(line, want) {
			t.Errorf("render = %q, want substring %q", line, want)
		}
	}
}

func TestProgressRenderFinalizing(t *testing.T) {
	p := NewProgress(1, 100)
	p.AddBytes(100)
	line := p.render(5 * time.Second)
	if !strings.Contains(line, "finalizing") {
		t.Errorf("render at 100%% = %q, want finalizing state", line)
	}
}

func TestProgressNilSafe(t *testing.T) {
	var p *Progress
	p.AddBytes(5) // must not panic
	p.FileDone()  // must not panic
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'TestHumanBytes|TestFmtDuration|TestProgress' -v`
Expected: FAIL — `undefined: humanBytes`, `undefined: NewProgress`, etc.

- [ ] **Step 3: Implement progress.go**

Create `cmd/amber-store/progress.go`:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Progress tracks ingest progress — bytes read from source content and regular
// files completed — against totals from the pre-scan, and renders a one-line
// bar. A nil *Progress is a no-op, so callers that disable progress pass nil.
type Progress struct {
	totalFiles int64
	totalBytes int64
	bytesDone  atomic.Uint64
	filesDone  atomic.Uint64
}

// NewProgress returns a Progress sized by the pre-scan totals.
func NewProgress(totalFiles, totalBytes int64) *Progress {
	return &Progress{totalFiles: totalFiles, totalBytes: totalBytes}
}

// AddBytes records n bytes read from source content. nil-safe.
func (p *Progress) AddBytes(n int) {
	if p == nil {
		return
	}
	p.bytesDone.Add(uint64(n))
}

// FileDone records one completed regular file. nil-safe.
func (p *Progress) FileDone() {
	if p == nil {
		return
	}
	p.filesDone.Add(1)
}

// render builds the progress line for the given elapsed time. It is pure (no
// clock, no I/O), so the render loop and tests share it. Throughput and ETA use
// the overall average rate (bytes/elapsed), which is smooth and deterministic.
// When every byte is read but the call has not yet returned, it shows a
// "finalizing" suffix.
func (p *Progress) render(elapsed time.Duration) string {
	done := p.bytesDone.Load()
	files := p.filesDone.Load()
	total := uint64(p.totalBytes)

	secs := elapsed.Seconds()
	var rate float64
	if secs > 0 {
		rate = float64(done) / secs
	}

	pct := 100.0
	if total > 0 {
		pct = float64(done) / float64(total) * 100
		if pct > 100 {
			pct = 100
		}
	}

	eta := "--:--"
	if total > 0 && done > 0 && done < total && rate > 0 {
		remain := time.Duration(float64(total-done)/rate) * time.Second
		eta = fmtDuration(remain)
	}

	tail := ""
	if total > 0 && done >= total {
		tail = "  finalizing…"
	}

	return fmt.Sprintf("ingest %5.1f%% %s  %s/%s  %s/s  elapsed %s  eta %s  files %d/%d%s",
		pct,
		bar(pct, 16),
		humanBytes(done), humanBytes(total),
		humanBytes(uint64(rate)),
		fmtDuration(elapsed), eta,
		files, p.totalFiles,
		tail,
	)
}

// Run renders progress to w until ctx is cancelled, using start as t0. On a TTY
// it redraws one line in place and clears it on exit; on a non-TTY it prints a
// plain line at a slower cadence. start is injected so the caller owns the clock.
func (p *Progress) Run(ctx context.Context, w io.Writer, start time.Time, isTTY bool) {
	interval := 150 * time.Millisecond
	if !isTTY {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	draw := func() {
		line := p.render(time.Since(start))
		if isTTY {
			fmt.Fprintf(w, "\r\033[K%s", line)
		} else {
			fmt.Fprintf(w, "%s\n", line)
		}
	}
	for {
		select {
		case <-ctx.Done():
			if isTTY {
				fmt.Fprint(w, "\r\033[K")
			}
			return
		case <-t.C:
			draw()
		}
	}
}

// bar renders a width-cell progress bar for pct in [0,100].
func bar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

// humanBytes formats n with binary (KiB/MiB/…) units.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// fmtDuration formats d as M:SS or H:MM:SS, clamping negatives to zero.
func fmtDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

// isTerminal reports whether f is a character device (a terminal), used to pick
// between an in-place redraw and plain progress lines.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run 'TestHumanBytes|TestFmtDuration|TestProgress' -v`
Expected: PASS (all five tests).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/progress.go cmd/amber-store/progress_test.go
git commit -m "feat(ingest): progress tracker and terminal renderer"
```

---

## Task 2: Parallel pre-scan (`scanTree`)

**Files:**
- Create: `cmd/amber-store/scan.go`
- Test: `cmd/amber-store/scan_test.go`

- [ ] **Step 1: Write the failing test**

Create `cmd/amber-store/scan_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanTree_CountsRegularFileBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "c.txt"), []byte("cee"), 0o644); err != nil { // 3
		t.Fatal(err)
	}

	files, bytes, err := scanTree(dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	if files != 3 {
		t.Errorf("files = %d, want 3", files)
	}
	if bytes != 14 {
		t.Errorf("bytes = %d, want 14", bytes)
	}
}

func TestScanTree_ExcludesSymlinks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "real"), []byte("1234"), 0o644); err != nil { // 4
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	files, bytes, err := scanTree(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if files != 1 || bytes != 4 {
		t.Errorf("scanTree = (%d files, %d bytes), want (1, 4)", files, bytes)
	}
}

func TestScanTree_EmptyDir(t *testing.T) {
	files, bytes, err := scanTree(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if files != 0 || bytes != 0 {
		t.Errorf("scanTree(empty) = (%d, %d), want (0, 0)", files, bytes)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestScanTree -v`
Expected: FAIL — `undefined: scanTree`.

- [ ] **Step 3: Implement scan.go**

Create `cmd/amber-store/scan.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// scanTree walks the tree at dir concurrently (ReadDir + Lstat only, no content
// reads) and returns the number of regular files and the total size of their
// content. Used to size the ingest progress bar. Only regular-file bytes are
// counted, since only regular files are read during ingest; symlinks, dirs and
// special files contribute nothing. The first stat/read error aborts the scan.
//
// The fan-out mirrors the parallel build: each entry may run on a pooled
// goroutine, but when the pool is full the work runs inline on the current
// goroutine, so the recursion cannot deadlock waiting on a slot held by a
// descendant.
func scanTree(dir string, jobs int) (files int64, bytes int64, err error) {
	if jobs < 1 {
		jobs = 1
	}
	s := &scanner{sem: make(chan struct{}, jobs)}
	s.walk(dir)
	if e := s.err(); e != nil {
		return 0, 0, e
	}
	return s.files.Load(), s.bytes.Load(), nil
}

type scanner struct {
	files atomic.Int64
	bytes atomic.Int64
	sem   chan struct{}

	mu       sync.Mutex
	firstErr error
}

func (s *scanner) setErr(e error) {
	s.mu.Lock()
	if s.firstErr == nil {
		s.firstErr = e
	}
	s.mu.Unlock()
}

func (s *scanner) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

func (s *scanner) walk(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		s.setErr(err)
		return
	}
	var wg sync.WaitGroup
	for _, de := range ents {
		full := filepath.Join(dir, de.Name())
		do := func(full string) {
			info, err := os.Lstat(full)
			if err != nil {
				s.setErr(err)
				return
			}
			switch info.Mode() & os.ModeType {
			case os.ModeDir:
				s.walk(full)
			case 0: // regular file
				s.files.Add(1)
				s.bytes.Add(info.Size())
			default:
				// symlink, device, socket, fifo — not read during ingest.
			}
		}
		select {
		case s.sem <- struct{}{}:
			wg.Add(1)
			go func(full string) {
				defer wg.Done()
				defer func() { <-s.sem }()
				do(full)
			}(full)
		default:
			do(full)
		}
	}
	wg.Wait()
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestScanTree -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/scan.go cmd/amber-store/scan_test.go
git commit -m "feat(ingest): parallel pre-scan counting regular-file bytes"
```

---

## Task 3: `diskstore.WriteParallel`

**Files:**
- Create: `diskstore/parallel.go`
- Test: `diskstore/parallel_test.go`

- [ ] **Step 1: Write the failing test**

Create `diskstore/parallel_test.go`:

```go
package diskstore_test

import (
	"bytes"
	"errors"
	"iter"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
)

func TestWriteParallel_MixedRetrievable(t *testing.T) {
	s := openTemp(t)

	small1 := []byte("small one")
	small2 := []byte("small two")
	large := bytes.Repeat([]byte("L"), diskstore.DefaultInlineThreshold+1)
	objs := []diskstore.Object{
		{Key: mkKey(t, small1), Data: small1},
		{Key: mkKey(t, large), Data: large},
		{Key: mkKey(t, small2), Data: small2},
	}

	if err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 4}); err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}

	for _, o := range objs {
		got, err := s.Get(o.Key)
		if err != nil {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
		if !bytes.Equal(got, o.Data) {
			t.Fatalf("Get(%s) = %d bytes, want %d", o.Key, len(got), len(o.Data))
		}
	}
}

func TestWriteParallel_DedupLargeWritesOneFile(t *testing.T) {
	dir := t.TempDir()
	s, err := diskstore.Open(dir, diskstore.WithSync(false))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	large := bytes.Repeat([]byte("D"), diskstore.DefaultInlineThreshold+1)
	k := mkKey(t, large)
	dup := func(yield func(diskstore.Object, error) bool) {
		for range 3 {
			if !yield(diskstore.Object{Key: k, Data: large}, nil) {
				return
			}
		}
	}
	if err := s.WriteParallel(dup, diskstore.WriteOpts{Writers: 4}); err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}
	if n := countBlobFiles(t, dir); n != 1 {
		t.Fatalf("blob files after 3 identical objects = %d, want 1", n)
	}
}

func TestWriteParallel_SurfacesIteratorError(t *testing.T) {
	s := openTemp(t)
	boom := errors.New("producer blew up")
	d := []byte("first")
	seq := func(yield func(diskstore.Object, error) bool) {
		if !yield(diskstore.Object{Key: mkKey(t, d), Data: d}, nil) {
			return
		}
		yield(diskstore.Object{}, boom)
	}
	err := s.WriteParallel(seq, diskstore.WriteOpts{Writers: 2})
	if !errors.Is(err, boom) {
		t.Fatalf("WriteParallel err = %v, want %v", err, boom)
	}
}

var _ iter.Seq2[diskstore.Object, error] = seqOf // keep iter import used
var _ = key.Size
```

(The two `var _` lines keep the `iter` and `key` imports referenced; remove either if its package is already used after you finish editing.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./diskstore/ -run TestWriteParallel -v`
Expected: FAIL — `s.WriteParallel undefined`, `diskstore.WriteOpts undefined`.

- [ ] **Step 3: Implement parallel.go**

Create `diskstore/parallel.go`:

```go
package diskstore

import (
	"context"
	"iter"
	"runtime"
	"sync"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize is the byte threshold at which a writer commits its current
// Pebble batch and starts a fresh one.
const DefaultBatchSize = 16 << 20 // 16 MiB

// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int // concurrent batch writers; <= 0 means GOMAXPROCS
	BatchSize int // commit when a batch reaches this many bytes; <= 0 means DefaultBatchSize
}

// WriteParallel stores every object the iterator yields using multiple
// concurrent batch writers. Each writer accumulates objects into its own Pebble
// batch and commits when the batch reaches BatchSize bytes (and once more when
// the input is exhausted); commits run concurrently. External (large) objects
// are written to disk durably before the commit that references them. Objects
// already in the store, or already seen earlier in this run, are skipped.
//
// Unlike WriteBatch, WriteParallel is NOT atomic: on error or crash, objects
// from already-committed batches remain alongside harmless orphan blob files.
// Because the store is content-addressed and idempotent, a re-run converges. If
// the iterator yields an error, WriteParallel stops and returns it.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) error {
	writers := opts.Writers
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan Object, writers*2)
	seen := newSeenSet()
	eg := &errgroup.Group{}

	// Distributor: forward objects from the iterator to the writer pool. It does
	// no Has/commit work, so it never bottlenecks. A yielded error cancels the
	// run and propagates as the group's error.
	eg.Go(func() error {
		defer close(ch)
		for obj, err := range seq {
			if err != nil {
				cancel()
				return err
			}
			select {
			case ch <- obj:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	for range writers {
		eg.Go(func() error {
			err := s.runWriter(ctx, ch, seen, batchSize)
			if err != nil {
				cancel() // stop the distributor and sibling writers
			}
			return err
		})
	}

	return eg.Wait()
}

// runWriter consumes objects, accumulating them into a Pebble batch it commits
// when the batch reaches batchSize bytes and once more when the channel closes.
// On ctx cancellation it returns without committing (the run is being aborted;
// partial commits are safe in a content-addressed store).
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int) (err error) {
	b := s.db.NewBatch()
	defer func() {
		if b != nil {
			b.Close()
		}
	}()
	commit := func() error {
		if b.Empty() {
			return nil
		}
		if err := b.Commit(s.writeOpts); err != nil {
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case obj, ok := <-ch:
			if !ok {
				return commit()
			}
			if !seen.addIfAbsent(obj.Key) {
				continue
			}
			has, err := s.Has(obj.Key)
			if err != nil {
				return err
			}
			if has {
				continue
			}
			if len(obj.Data) > s.threshold {
				if err := s.writeExternal(obj.Key, obj.Data); err != nil {
					return err
				}
				if err := b.Set(obj.Key[:], []byte{tagExternal}, nil); err != nil {
					return err
				}
			} else {
				val := make([]byte, 1+len(obj.Data))
				val[0] = tagInline
				copy(val[1:], obj.Data)
				if err := b.Set(obj.Key[:], val, nil); err != nil {
					return err
				}
			}
			if b.Len() >= batchSize {
				if err := commit(); err != nil {
					return err
				}
			}
		}
	}
}

// seenSet is a concurrency-safe set of keys, sharded on the key's last byte
// (uniformly distributed) to spread lock contention across writers.
type seenSet struct {
	shards [256]seenShard
}

type seenShard struct {
	mu sync.Mutex
	m  map[key.Key]struct{}
}

func newSeenSet() *seenSet {
	s := &seenSet{}
	for i := range s.shards {
		s.shards[i].m = make(map[key.Key]struct{})
	}
	return s
}

// addIfAbsent records k and reports true if it was not already present.
func (s *seenSet) addIfAbsent(k key.Key) bool {
	sh := &s.shards[k[key.Size-1]]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[k]; ok {
		return false
	}
	sh.m[k] = struct{}{}
	return true
}
```

- [ ] **Step 4: Run the test to verify it passes (with the race detector)**

Run: `go test ./diskstore/ -run TestWriteParallel -race -v`
Expected: PASS (all three tests), no race warnings.

- [ ] **Step 5: Commit**

```bash
git add diskstore/parallel.go diskstore/parallel_test.go
git commit -m "feat(diskstore): WriteParallel with concurrent bounded batches"
```

---

## Task 4: Instrument the build with Progress

**Files:**
- Modify: `cmd/amber-store/driver.go` (driver struct, `buildFile`, `buildEntry`)
- Modify: `cmd/amber-store/ingest.go` (`ingestObjects` signature, pbuilder driver construction)
- Modify: `cmd/amber-store/ingest_test.go` (existing `ingestObjects` call sites)
- Test: `cmd/amber-store/ingest_test.go` (new test)

- [ ] **Step 1: Write the failing test**

Add to `cmd/amber-store/ingest_test.go`:

```go
// TestIngestObjects_ReportsProgress checks the instrumented build feeds the
// Progress tracker: bytesDone equals total regular-file bytes and filesDone
// equals the file count.
func TestIngestObjects_ReportsProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}

	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	p := NewProgress(2, 11)
	var root key.Key
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, 2, p, &root)
	if err := store.WriteBatch(seq); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if got := p.bytesDone.Load(); got != 11 {
		t.Errorf("bytesDone = %d, want 11", got)
	}
	if got := p.filesDone.Load(); got != 2 {
		t.Errorf("filesDone = %d, want 2", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestIngestObjects_ReportsProgress -v`
Expected: FAIL — compile error: not enough arguments in call to `ingestObjects` (the `p` parameter does not exist yet).

- [ ] **Step 3: Add the Progress field to driver and emit progress events**

In `cmd/amber-store/driver.go`, extend the `driver` struct (around line 55):

```go
type driver struct {
	ic             chunkers.ItemChunker
	byteOpts       *chunkers.ByteOpts
	xattrInlineMax int
	p              *Progress // nil for pack; set for ingest
}
```

In `buildEntry`, the regular-file case (around line 102) records a completed file:

```go
	case unix.S_IFREG:
		ck, err := d.buildFile(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
		d.p.FileDone()
```

In `buildFile`, the chunk callback (around line 165) records bytes read:

```go
	err = chunkers.SplitBytes(f, d.byteOpts, func(chunk []byte) error {
		d.p.AddBytes(len(chunk))
		saw = true
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}
		return ib.AddChild(emit, obj.Key, nil)
	})
```

- [ ] **Step 4: Thread Progress through ingestObjects**

In `cmd/amber-store/ingest.go`, change the `ingestObjects` signature (line 35) to insert `p *Progress` before `root`:

```go
func ingestObjects(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, p *Progress, root *key.Key) iter.Seq2[diskstore.Object, error] {
```

And pass it into the pbuilder's driver (around line 59):

```go
			b := &pbuilder{
				d:    &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax, p: p},
				emit: emit,
				sem:  make(chan struct{}, jobs),
			}
```

(`pack` in `driver.go` constructs its `driver` without `p`, leaving it nil — no change needed there, and the nil-safe Progress methods make it a no-op.)

- [ ] **Step 5: Update existing ingestObjects call sites in ingest_test.go**

In `cmd/amber-store/ingest_test.go`, update the two existing calls to pass `nil` for progress:

Line ~50:
```go
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, 4, nil, &root)
```

Line ~142:
```go
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, jobs, nil, &root)
```

- [ ] **Step 6: Run the cmd package tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run 'TestIngestObjects' -v`
Expected: PASS — including the new `TestIngestObjects_ReportsProgress` and the existing parity tests.

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/driver.go cmd/amber-store/ingest.go cmd/amber-store/ingest_test.go
git commit -m "feat(ingest): report bytes and files to the progress tracker"
```

---

## Task 5: Wire pre-scan, progress, and WriteParallel into runIngest

**Files:**
- Modify: `cmd/amber-store/main.go` (ingestConfig fields, ingest flags)
- Modify: `cmd/amber-store/ingest.go` (`runIngest`)
- Test: `cmd/amber-store/ingest_test.go` (new tests)

- [ ] **Step 1: Write the failing tests**

Add to `cmd/amber-store/ingest_test.go` (and add `"strings"` to its import block):

```go
// TestRunIngest_PrintsRootToStdout asserts the ingest command writes the root
// key (and only the root key) to the app writer (stdout).
func TestRunIngest_PrintsRootToStdout(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	if err := app.Run([]string{"amber-store", "ingest", "--store", t.TempDir(), "--no-progress", src}); err != nil {
		t.Fatal(err)
	}

	var pb bytes.Buffer
	root, err := pack(src, &pb, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(buf.String()); got != root.String() {
		t.Fatalf("stdout = %q, want root %q", got, root.String())
	}
}

// TestRunIngest_DeterministicAcrossWriters asserts the resolved root is
// independent of writer-pool size.
func TestRunIngest_DeterministicAcrossWriters(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)

	roots := make([]string, 0, 2)
	for _, w := range []string{"1", "8"} {
		var buf bytes.Buffer
		app := newApp()
		app.Writer = &buf
		args := []string{"amber-store", "ingest", "--store", t.TempDir(), "--no-progress", "--writers", w, src}
		if err := app.Run(args); err != nil {
			t.Fatalf("--writers %s: %v", w, err)
		}
		roots = append(roots, strings.TrimSpace(buf.String()))
	}
	if roots[0] != roots[1] {
		t.Fatalf("root differs across --writers: %q vs %q", roots[0], roots[1])
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'TestRunIngest_PrintsRootToStdout|TestRunIngest_DeterministicAcrossWriters' -v`
Expected: FAIL — `flag provided but not defined: -no-progress` (and `-writers`), and root not written to the buffer.

- [ ] **Step 3: Add CLI flags and config fields**

In `cmd/amber-store/main.go`, extend `ingestConfig` (around line 124):

```go
type ingestConfig struct {
	chunk           chunkConfig
	store           string
	inlineThreshold int
	sync            bool
	jobs            int
	writers         int
	noProgress      bool
}
```

Add two flags inside `ingestCommand`'s `flags := append(...)` list (after the `jobs` flag, before the closing `)`):

```go
		&cli.IntFlag{
			Name:        "writers",
			Aliases:     []string{"w"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "number of concurrent batch writers committing to the store (default: number of CPUs)",
			Destination: &cfg.writers,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &cfg.noProgress,
		},
```

(`runtime` is already imported in `main.go`.)

- [ ] **Step 4: Rewrite runIngest**

In `cmd/amber-store/ingest.go`, replace the body of `runIngest` (lines ~151-180) with:

```go
// Handles the 'ingest' command.
func runIngest(c *cli.Context, cfg *ingestConfig) error {
	dir, err := dirArg(c, "ingest")
	if err != nil {
		return err
	}

	byteOpts, err := cfg.chunk.byteOpts()
	if err != nil {
		return err
	}

	store, err := diskstore.Open(
		cfg.store,
		diskstore.WithInlineThreshold(cfg.inlineThreshold),
		diskstore.WithSync(cfg.sync),
	)
	if err != nil {
		return err
	}
	defer store.Close()

	// Pre-scan to size the progress bar (cheap: readdir + lstat only).
	totalFiles, totalBytes, err := scanTree(dir, cfg.jobs)
	if err != nil {
		return err
	}

	var prog *Progress
	var pwg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	if !cfg.noProgress {
		prog = NewProgress(totalFiles, totalBytes)
		isTTY := isTerminal(os.Stderr)
		start := time.Now()
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			prog.Run(ctx, os.Stderr, start, isTTY)
		}()
	}

	var root key.Key
	seq := ingestObjects(dir, cfg.chunk.itemChunker(), byteOpts, cfg.chunk.xattrInlineMax, cfg.jobs, prog, &root)
	writeErr := store.WriteParallel(seq, diskstore.WriteOpts{Writers: cfg.writers})

	cancel()
	pwg.Wait()

	if writeErr != nil {
		return writeErr
	}

	fmt.Fprintf(c.App.Writer, "%s\n", root.String())
	return nil
}
```

Add `"time"` to the import block in `cmd/amber-store/ingest.go` (it already imports `context`, `fmt`, `os`, and `sync`).

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run 'TestRunIngest_PrintsRootToStdout|TestRunIngest_DeterministicAcrossWriters' -v`
Expected: PASS.

- [ ] **Step 6: Run the whole cmd package to confirm nothing regressed**

Run: `go test ./cmd/amber-store/ -v`
Expected: PASS — all ingest, pack, restore, decode, and meta tests.

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/main.go cmd/amber-store/ingest.go cmd/amber-store/ingest_test.go
git commit -m "feat(ingest): pre-scan + progress bar + parallel batch writes, root to stdout"
```

---

## Task 6: Full verification and manual smoke test

**Files:** none (verification only)

- [ ] **Step 1: Run the entire test suite with the race detector**

Run: `go test ./... -race`
Expected: PASS for every package, no race warnings.

- [ ] **Step 2: Run go vet**

Run: `go vet ./...`
Expected: no diagnostics.

- [ ] **Step 3: Build the binary and smoke-test ingest on a real tree with the progress bar**

Run:
```bash
go build -o /tmp/amber-store ./cmd/amber-store
/tmp/amber-store ingest --store /tmp/amber-smoke-store . ; echo "exit=$?"
```
Expected: a live progress line on stderr (percentage, sizes, rate, elapsed, eta, file counts) that ends cleared, then the 64-hex root key printed on its own line to stdout, and `exit=0`. (If stderr is redirected to a file, expect periodic plain progress lines instead of an in-place bar.)

- [ ] **Step 4: Confirm determinism and dedup on a re-run**

Run:
```bash
/tmp/amber-store ingest --store /tmp/amber-smoke-store . | tee /tmp/root2.txt
```
Expected: the same root key as Step 3 (re-ingest is a near-instant no-op — every object is already present, so the progress bar fills fast and nothing new is written).

- [ ] **Step 5: Remove the smoke-test binary and store**

Run:
```bash
rm -rf /tmp/amber-store /tmp/amber-smoke-store /tmp/root2.txt
```
(Per the global rule: remove binaries you generated.)

- [ ] **Step 6: Final commit if any docs/cleanup remain**

If `go vet` or the smoke test prompted any fixes, commit them:
```bash
git add -A
git commit -m "chore(ingest): verification fixes"
```
Otherwise this task produces no commit.

---

## Self-Review

**Spec coverage:**
- Pre-scan for totals → Task 2 (`scanTree`), wired in Task 5. ✓
- Progress measured by bytes; files shown alongside → Task 1 (`render`), Task 4 (instrumentation). ✓
- Parallel build instrumented, not restructured → Task 4 (driver field + two call sites only). ✓
- Parallel bounded batches, parallel commits, shared sharded seen-set, external-before-marker, relaxed atomicity → Task 3 (`WriteParallel`). ✓
- Bidirectional cancellation (build error stops writers; commit error stops build) → Task 3 (errgroup + shared ctx + cancel). ✓
- Progress renderer: ~150ms TTY redraw, plain lines off-TTY, `--no-progress`, finalizing state, line cleared before root → Task 1 (`Run`/`render`), Task 5 (wiring). ✓
- `--writers`/`-w` and `--no-progress` flags; `--jobs` retained → Task 5. ✓
- Root printed to stdout → Task 5 (`c.App.Writer`), asserted in `TestRunIngest_PrintsRootToStdout`. ✓
- Testing: scanTree cases, WriteParallel mixed/dedup/error, determinism across writers, progress math, root-on-stdout → Tasks 1-5. ✓

**Deviations from the spec (intentional, noted):**
- The spec sketched an explicit `Finalizing()` method and EWMA-smoothed rate. The plan instead (a) derives the finalizing state inside `render` when `bytesDone >= totalBytes` (no separate method/state to coordinate), and (b) uses the overall average rate (`bytesDone/elapsed`) for throughput and ETA, which is naturally smooth and keeps `render` pure and deterministically testable. Both are simpler and within the spec's intent.
- TTY detection uses `os.ModeCharDevice` (stdlib) instead of adding `golang.org/x/term`, keeping the dependency set unchanged.

**Placeholder scan:** none — every step has complete code or an exact command.

**Type consistency:** `Progress` (`AddBytes(int)`, `FileDone()`, `render(time.Duration)`, `Run(ctx, io.Writer, time.Time, bool)`), `scanTree(string, int) (int64, int64, error)`, `WriteOpts{Writers, BatchSize}`, `WriteParallel(iter.Seq2[Object,error], WriteOpts) error`, `seenSet.addIfAbsent(key.Key) bool`, and the new `ingestObjects(..., p *Progress, root *key.Key)` signature are used identically across all tasks and tests.
```
