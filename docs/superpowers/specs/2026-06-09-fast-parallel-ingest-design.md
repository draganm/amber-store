# fast parallel ingest with progress reporting

**Status:** design approved 2026-06-09.

## Purpose

Make `amber-store ingest` as fast as possible for trees with many files, and
give the user live feedback while it runs. Two thrusts:

1. **More parallelism.** The tree build is already concurrent (`pbuilder`), but
   the write path serializes: a single goroutine consumes the object stream,
   does serial `Has()` lookups, and accumulates *one* Pebble batch committed
   atomically at the very end. Replace that with multiple bounded batches
   committed in parallel, and parallelize the membership checks. Also
   parallelize the new pre-scan walk.
2. **Progress reporting.** Show a progress bar with percentage done, elapsed
   time, throughput, and ETA, measured against bytes.

## Decisions (from brainstorming)

- **Total for the progress bar comes from a pre-scan pass.** A quick parallel
  walk (`ReadDir` + `Lstat` only, no content reads) counts total regular-file
  bytes and total entries before ingest starts. Cheap relative to
  read+chunk+hash.
- **Percentage and ETA are measured by bytes.** The heavy work (I/O + CDC +
  hashing) scales with bytes. Every file is fully read during ingest — dedup
  happens at *write* time, not read time — so bytes-read during ingest equals
  the pre-scan byte total exactly, and the percentage is accurate. File counts
  are displayed alongside but do not drive the percentage.
- **Atomicity is relaxed.** Batches commit independently and in parallel. In a
  content-addressed store a crash mid-ingest leaves only harmless orphan
  objects: a re-run dedups and completes. The root key is printed only after
  every writer has flushed, so nothing ever references an incomplete tree.
- **Root key prints to stdout.** (Previously stderr.) Progress renders to
  stderr, so `amber-store ingest … | …` cleanly captures the root.

## Components

### 1. Pre-scan — `scanTree`

`cmd/amber-store/scan.go`:

```go
// scanTree walks dir concurrently (ReadDir + Lstat only) and returns the
// number of regular files and the sum of their sizes. Used to size the
// progress bar. Symlinks, dirs, and special files count toward neither total
// (only regular-file content is read during ingest).
func scanTree(dir string, jobs int) (files int64, bytes int64, err error)
```

Reuses the same bounded-pool fan-out pattern as `pbuilder.buildDir`
(non-blocking semaphore send, inline fallback when the pool is full, so the
recursion cannot deadlock). Counters are `atomic.Int64`.

### 2. Instrumented parallel build

The existing `pbuilder` / `driver` build is kept as-is structurally. A
`*Progress` is threaded through so the hot path reports work:

- `driver.buildFile`'s chunk callback adds `len(chunk)` to `Progress.bytesDone`.
- Each completed regular file bumps `Progress.filesDone`.

Both are `atomic.Uint64` increments — lock-free; the renderer only reads them.

### 3. Parallel bounded-batch writer — `diskstore.WriteParallel`

New method; the existing atomic `WriteBatch` is left intact for its current
callers and tests.

```go
type WriteOpts struct {
    Writers   int // batch-writer goroutines (default: GOMAXPROCS)
    BatchSize int // commit when a batch's byte size crosses this (default: 16 MiB)
}

// WriteParallel stores every object the iterator yields using Writers
// concurrent batch writers. Each writer accumulates objects into its own
// Pebble batch and commits when the batch crosses BatchSize bytes; commits run
// concurrently. External (large) objects are written to disk durably before
// the commit that references them. Objects already present (or already seen in
// this run) are skipped. Unlike WriteBatch this is NOT atomic: a crash can
// leave committed objects from finished batches plus harmless orphan blob
// files; a re-run converges.
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) error
```

Structure:

- One **distributor** goroutine ranges `seq` and sends objects over an internal
  channel to the writer pool. Forwarding only — no `Has`/commit — so it never
  bottlenecks. If `seq` yields an error, the distributor stops and the error
  propagates (cancelling the writers).
- **W writer** goroutines, each:
  1. pull an object,
  2. skip if its key is in the **shared sharded `seen` set** (a fixed array of
     `map[key.Key]struct{}` each behind its own mutex, sharded on a key byte) —
     this avoids redundant `Has()` calls across writers; a race that lets two
     writers both write the same new key is harmless (idempotent),
  3. else `Has()` (now parallel across writers); skip if present,
  4. external object → write its blob file durably now, stage `Set(k,{0x01})`
     into the local batch; inline → stage `Set(k, 0x00‖data)`,
  5. when the local batch's byte size crosses `BatchSize`, commit it (Sync per
     the store's option) and open a fresh batch,
  6. on channel close, commit the final partial batch.
- Coordination via `errgroup` + a shared `context`: any writer's error cancels
  the distributor and siblings; the distributor's error cancels the writers.
  `WriteParallel` returns the first error or nil after all writers have
  committed.

Memory is bounded by `Writers × BatchSize` (plus in-flight objects), fixing the
unbounded single-batch growth of the old path.

`runIngest` returns and prints the root key only after `WriteParallel` returns —
i.e. after every batch is durably committed.

### 4. Progress renderer

`cmd/amber-store/progress.go`:

```go
type Progress struct { /* totals + atomic counters + start time */ }

func NewProgress(totalFiles, totalBytes int64) *Progress
func (p *Progress) AddBytes(n uint64)
func (p *Progress) FileDone()
func (p *Progress) Run(ctx context.Context, w io.Writer) // render loop
func (p *Progress) Finalizing()                          // switch to draining state
```

A goroutine redraws every ~150 ms:

```
ingest  47% ████████░░░░░░░  3.2/6.8 GiB  142 MiB/s  elapsed 0:23  eta 0:31  files 12043/50112
```

- **ETA** from bytes: `elapsed * (total-done)/max(done,1)`, lightly smoothed
  (EWMA on the byte rate) to avoid jitter.
- **TTY-aware:** on a terminal, redraw in place with `\r` and clear-to-EOL; on a
  pipe/non-TTY, emit a plain progress line every few seconds (no carriage
  returns) so logs stay readable. `--no-progress` suppresses output entirely.
- **Finalizing state:** bytes-read can reach 100% while writers are still
  draining their last batches. At that point `Finalizing()` switches the line to
  `finalizing…`; when ingest completes the renderer clears its line before the
  root key is printed to stdout.
- The start time is injected (passed in), since `time.Now()` is taken once in
  `runIngest`, keeping the renderer testable.

### 5. CLI flags

Add to the ingest command:

- `--writers` / `-w` (default `GOMAXPROCS`) — batch-writer parallelism.
- `--no-progress` — disable the progress bar.

Keep `--jobs` / `-j` for build (and pre-scan) parallelism.

## Control flow of `runIngest`

```
1. validate DIR, build chunk opts, open store.
2. files, bytes := scanTree(dir, jobs)            // pre-scan
3. p := NewProgress(files, bytes); go p.Run(...)   // unless --no-progress
4. seq := ingestObjects(dir, …, jobs, p, &root)    // build, instrumented
5. err := store.WriteParallel(seq, WriteOpts{Writers, BatchSize})
6. p.Finalizing() before WriteParallel drains; stop renderer, clear line.
7. on success: print root.String() to STDOUT.
```

## Crash & atomicity semantics

- Each batch commit is atomic and (with `WithSync`) fsync-durable on its own.
- External blob files are fsync-durable before the marker that references them
  commits — unchanged from the existing store.
- A crash can leave: objects from already-committed batches, plus orphan blob
  files staged for not-yet-committed batches. All are content-addressed and
  harmless; the root key was not printed, so nothing references a partial tree.
  A re-run skips what exists (`seen` + `Has`) and converges.

## Out of scope

- GC of orphan blobs (already out of scope for the store).
- Changing the build (`pbuilder`) fan-out strategy — it stays as is, only
  instrumented.
- Multi-process ingest into one store (Pebble remains single-process).

## Testing

- **`scanTree`:** temp tree with known regular-file count and byte total →
  exact match; nested dirs; symlinks/special files excluded from byte total;
  empty dir → `(0, 0)`.
- **`WriteParallel`:** mix of inline and external objects with `Writers > 1`,
  every key retrievable after return; duplicate keys within the stream written
  once (assert one blob file for repeated externals); an iterator that yields an
  error mid-stream surfaces the error; equivalence test — a tree ingested via
  `WriteParallel` yields the same set of stored keys (and same root) as the
  sequential `pack` walk.
- **Determinism:** ingest of a fixed tree produces the same root key across
  several runs and across `--writers` values.
- **`Progress`:** ETA/percentage math on synthetic counters (injected start
  time, no real clock); `--no-progress` produces no stderr progress output;
  non-TTY writer emits plain lines (no `\r`).
- **Root on stdout:** end-to-end ingest test asserts the root key appears on
  **stdout** and that stderr carries only progress.
```
