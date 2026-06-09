# Amber Daemon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a long-running `amber-store daemon` that owns the diskstore exclusively and serves CLI clients over HTTP on a unix socket: clients build the tree, stream a compact pack-write format for verified bulk storage, and fetch directory tars for `dump`/`restore`. The `pack` command and `castar` package are removed.

**Architecture:** Client = build (chunk + BLAKE3 + fstree, reusing the existing `driver`/`pbuilder`), daemon = own + verify + serve. The pack-write format is a rootless, possibly-partial bag of CAS objects. The daemon verifies every key against its payload in the storage path and streams PAX tars built by traversing the CAS. The same `http.Handler` will serve TCP later — only the listener differs.

**Tech Stack:** Go 1.26, stdlib `net/http` (method-pattern `ServeMux`, unix listener), `archive/tar` (PAX), `encoding/binary` (uvarint), `github.com/zeebo/blake3`, `golang.org/x/sys/unix`, `github.com/urfave/cli/v2`. Spec: `docs/superpowers/specs/2026-06-09-amber-daemon-design.md`.

**Conventions:** Tests use stdlib `testing` (no testify), table-free helper style matching the repo. All commits end with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer (per CLAUDE.md). Work happens on the existing `feat/amber-daemon` branch. Run `go test ./...` and `go vet ./...` before the final commit of each task.

**Canonical types (defined once, used everywhere):**
- `fstree.Object{ Key key.Key; Bytes []byte }` — the in-transit CAS object (already exists). `amberpack` serializes these; `ingestObjects` yields these.
- `diskstore.Object{ Key key.Key; Data []byte }` — the storage object (already exists). The daemon adapts `fstree.Object` → `diskstore.Object` at the storage boundary.
- `amberpack` magic = the 8 bytes `AMBERPK\x01`; record tags `0x01` (object) and `0x00` (end of stream).

---

## File Structure

New packages:
- `amberpack/pack.go` — pack-write format: `Writer` (serialize `fstree.Object`s), `Reader` (`iter.Seq2[fstree.Object, error]`), `ErrMalformed`. Depends only on `bufio`, `encoding/binary`, `errors`, `io`, `iter`, `key`, `fstree`.
- `amberpack/pack_test.go` — round-trip, truncation, bad magic, corrupt-frame.
- `tarexport/tarexport.go` — server-side traversal: CAS dir key → PAX tar (`Write(w, root, get)`). Depends on `archive/tar`, `fstree`, `key`, `internal/cborx`, `golang.org/x/sys/unix`.
- `tarexport/tarexport_test.go`.
- `tarextract/tarextract.go` — client-side PAX tar → filesystem with faithful metadata and deferred directory metadata (`Extract(r, destDir)`). Depends on `archive/tar`, `golang.org/x/sys/unix`.
- `tarextract/tarextract_test.go`.
- `internal/socketpath/socketpath.go` — `Resolve(flag string) string` (flag → env → XDG → TempDir/uid).
- `internal/socketpath/socketpath_test.go`.
- `client/client.go` — `Client` over a unix socket: `Ingest(ctx, io.Reader) (Stats, error)`, `Tar(ctx, key.Key) (io.ReadCloser, error)`.
- `daemon/daemon.go` — `New(*diskstore.Store) http.Handler` with `POST /v1/objects` and `GET /v1/tar/{key}`.
- `daemon/daemon_test.go` — `httptest`-driven handler tests including split upload.

Changed:
- `diskstore/parallel.go` — add `WriteStats`, `WriteOpts.Verify`; `WriteParallel` returns `(WriteStats, error)`; new `diskstore/verify.go` with `ErrVerify` + `verifyObject`.
- `diskstore/parallel_test.go` — update 3 `WriteParallel` call sites; add verify tests.
- `cmd/amber-store/main.go` — command set becomes `daemon, ingest, load, dump, restore`.
- `cmd/amber-store/ingest.go` — `ingestObjects` yields `fstree.Object`; `ingest` builds a pack and either writes `--output` or streams to the daemon.
- `cmd/amber-store/daemon.go` (new), `load.go` (new), `dump.go` (new).
- `cmd/amber-store/restore.go` — rewritten to fetch a tar and call `tarextract.Extract`; the traversal/metadata code moves out.
- Test rewrites: `driver_test.go`, `ingest_test.go`, `main_test.go`, `restore_test.go`.

Removed:
- `castar/` (package + tests), `cmd/amber-store/driver.go`'s `pack()` function, the `pack` command.

---

## Task 1: `amberpack` — the pack-write format

**Files:**
- Create: `amberpack/pack.go`
- Test: `amberpack/pack_test.go`

- [ ] **Step 1: Write the failing test**

```go
package amberpack

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// mkObj builds a canonical Blob object from data (Blob length == byte length).
func mkObj(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func collect(t *testing.T, r *Reader) ([]fstree.Object, error) {
	t.Helper()
	var out []fstree.Object
	for o, err := range r.All() {
		if err != nil {
			return out, err
		}
		out = append(out, o)
	}
	return out, nil
}

func TestWriterReader_RoundTrip(t *testing.T) {
	objs := []fstree.Object{
		mkObj(t, []byte("alpha")),
		mkObj(t, []byte("")),
		mkObj(t, bytes.Repeat([]byte("x"), 5000)),
	}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := collect(t, NewReader(&buf))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(objs) {
		t.Fatalf("read %d objects, want %d", len(got), len(objs))
	}
	for i, o := range objs {
		if got[i].Key != o.Key || !bytes.Equal(got[i].Bytes, o.Bytes) {
			t.Errorf("object %d mismatch", i)
		}
	}
}

func TestWriterReader_EmptyStreamIsValid(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := collect(t, NewReader(&buf))
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("read %d objects from empty stream, want 0", len(got))
	}
}

func TestReader_BadMagic(t *testing.T) {
	_, err := collect(t, NewReader(bytes.NewReader([]byte("NOTAMBER..."))))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
}

func TestReader_TruncatedMissingEndMarker(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Add(mkObj(t, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	// Flush the buffered object bytes WITHOUT writing the end marker.
	if err := w.bw.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err := collect(t, NewReader(&buf))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (missing end marker)", err)
	}
}

func TestReader_NonCanonicalKeyRejected(t *testing.T) {
	// A frame whose 32 key bytes are all zero: type 0, length-size 1, length 0 is
	// canonical, but the reserved bit / type checks live in key.Parse — use a key
	// byte with the reserved bit set to force a parse error.
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(tagObject)
	var k [key.Size]byte
	k[0] = 0x08 // reserved bit set -> key.Parse fails
	buf.Write(k[:])
	buf.WriteByte(0) // uvarint payload length 0
	_, err := collect(t, NewReader(&buf))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed (bad key)", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./amberpack/ -run TestWriterReader -v`
Expected: FAIL — `undefined: NewWriter` (package does not compile yet).

- [ ] **Step 3: Write the implementation**

```go
// Package amberpack defines the pack-write format: a flat, sequential,
// stream-friendly serialization of CAS objects, used both as the body uploaded
// to the daemon and as a standalone on-disk artifact. A stream is a
// possibly-partial, unordered set of objects (like a git pack) and carries no
// root key. Layout:
//
//	Header   "AMBERPK\x01"                  8 bytes  magic + version
//	Records  repeat: 0x01 key[32] uvarint(len) payload
//	End      0x00
//
// The 32-byte key already encodes the object's type and logical length, so no
// separate fields are needed. The Reader validates framing and that each key is
// canonical; it does NOT verify the payload hash — that happens in the storage
// path (diskstore verification).
package amberpack

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// magic identifies the format and its version (the trailing byte).
const magic = "AMBERPK\x01"

const (
	tagObject byte = 0x01 // an object record follows
	tagEnd    byte = 0x00 // end of stream
)

// ErrMalformed wraps every error from a structurally invalid stream (bad magic,
// bad record tag, non-canonical key, or truncation). Callers distinguish it with
// errors.Is to map to a client error.
var ErrMalformed = errors.New("amberpack: malformed stream")

// Writer serializes fstree.Objects into the pack-write format. It is not safe
// for concurrent use; a client wanting parallel uploads creates one Writer per
// stream.
type Writer struct {
	bw          *bufio.Writer
	wroteHeader bool
}

// NewWriter returns a Writer emitting to w. The caller owns w and must close it;
// Writer.Close only flushes and writes the end marker.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) ensureHeader() error {
	if w.wroteHeader {
		return nil
	}
	if _, err := w.bw.WriteString(magic); err != nil {
		return err
	}
	w.wroteHeader = true
	return nil
}

// Add appends one object record.
func (w *Writer) Add(o fstree.Object) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagObject); err != nil {
		return err
	}
	if _, err := w.bw.Write(o.Key[:]); err != nil {
		return err
	}
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(o.Bytes)))
	if _, err := w.bw.Write(lb[:n]); err != nil {
		return err
	}
	_, err := w.bw.Write(o.Bytes)
	return err
}

// Close writes the header (if no object was added) and the end marker, then
// flushes. It does not close the underlying writer.
func (w *Writer) Close() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagEnd); err != nil {
		return err
	}
	return w.bw.Flush()
}

// Reader decodes a pack-write stream.
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// All iterates over the objects in the stream. It yields exactly one error (and
// stops) on any structural problem; on a clean stream it yields every object and
// returns after the end marker.
func (r *Reader) All() iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		var hdr [len(magic)]byte
		if _, err := io.ReadFull(r.br, hdr[:]); err != nil {
			yield(fstree.Object{}, fmt.Errorf("%w: reading header: %v", ErrMalformed, err))
			return
		}
		if string(hdr[:]) != magic {
			yield(fstree.Object{}, fmt.Errorf("%w: bad magic", ErrMalformed))
			return
		}
		for {
			tag, err := r.br.ReadByte()
			if err != nil {
				yield(fstree.Object{}, fmt.Errorf("%w: truncated before end marker: %v", ErrMalformed, err))
				return
			}
			switch tag {
			case tagEnd:
				return
			case tagObject:
				var kb [key.Size]byte
				if _, err := io.ReadFull(r.br, kb[:]); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading key: %v", ErrMalformed, err))
					return
				}
				k, err := key.Parse(kb[:])
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: %v", ErrMalformed, err))
					return
				}
				n, err := binary.ReadUvarint(r.br)
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading length: %v", ErrMalformed, err))
					return
				}
				payload := make([]byte, n)
				if _, err := io.ReadFull(r.br, payload); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading payload: %v", ErrMalformed, err))
					return
				}
				if !yield(fstree.Object{Key: k, Bytes: payload}, nil) {
					return
				}
			default:
				yield(fstree.Object{}, fmt.Errorf("%w: bad record tag %#x", ErrMalformed, tag))
				return
			}
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./amberpack/ -v`
Expected: PASS (all five tests).

- [ ] **Step 5: Commit**

```bash
git add amberpack/
git commit -m "feat(amberpack): pack-write format Writer and Reader"
```

---

## Task 2: `diskstore` — verified, stat-reporting parallel writes

**Files:**
- Create: `diskstore/verify.go`
- Modify: `diskstore/parallel.go` (add `WriteStats`, `WriteOpts.Verify`; change `WriteParallel` signature and `runWriter`)
- Modify: `diskstore/parallel_test.go` (3 call sites + 2 new tests)

- [ ] **Step 1: Write the failing tests** (append to `diskstore/parallel_test.go`)

```go
func TestWriteParallel_Stats(t *testing.T) {
	s := openTemp(t)
	a := []byte("aaaa")
	b := []byte("bbbbbbbb")
	objs := []diskstore.Object{
		{Key: mkKey(t, a), Data: a},
		{Key: mkKey(t, b), Data: b},
		{Key: mkKey(t, a), Data: a}, // duplicate within the stream
	}
	stats, err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 1})
	if err != nil {
		t.Fatalf("WriteParallel: %v", err)
	}
	if stats.Stored != 2 || stats.Deduped != 1 {
		t.Fatalf("stats = %+v, want Stored=2 Deduped=1", stats)
	}
	if stats.BytesStored != int64(len(a)+len(b)) {
		t.Fatalf("BytesStored = %d, want %d", stats.BytesStored, len(a)+len(b))
	}

	// Re-writing the same objects dedups everything.
	stats2, err := s.WriteParallel(seqOf(objs...), diskstore.WriteOpts{Writers: 1})
	if err != nil {
		t.Fatalf("WriteParallel (re-run): %v", err)
	}
	if stats2.Stored != 0 || stats2.Deduped != 3 {
		t.Fatalf("re-run stats = %+v, want Stored=0 Deduped=3", stats2)
	}
}

func TestWriteParallel_VerifyRejectsTamperedKey(t *testing.T) {
	s := openTemp(t)
	data := []byte("honest payload")
	good := mkKey(t, data)
	// Flip one hash byte to make a key that does not match the payload.
	bad := good
	bad[key.Size-1] ^= 0xFF
	seq := seqOf(diskstore.Object{Key: bad, Data: data})

	_, err := s.WriteParallel(seq, diskstore.WriteOpts{Writers: 1, Verify: true})
	if !errors.Is(err, diskstore.ErrVerify) {
		t.Fatalf("err = %v, want ErrVerify", err)
	}
	has, _ := s.Has(bad)
	if has {
		t.Fatalf("tampered object must not be stored")
	}
}

func TestWriteParallel_VerifyAcceptsHonestObjects(t *testing.T) {
	s := openTemp(t)
	data := []byte("honest payload")
	_, err := s.WriteParallel(seqOf(diskstore.Object{Key: mkKey(t, data), Data: data}),
		diskstore.WriteOpts{Writers: 1, Verify: true})
	if err != nil {
		t.Fatalf("WriteParallel verify: %v", err)
	}
}
```

Add `"github.com/draganm/amber-store/key"` to the test file's imports (for `key.Size`).

- [ ] **Step 2: Update the 3 existing `WriteParallel` call sites** in `diskstore/parallel_test.go`

Change each `if err := s.WriteParallel(...); err != nil {` to `if _, err := s.WriteParallel(...); err != nil {`, and the `TestWriteParallel_SurfacesIteratorError` site `err := s.WriteParallel(...)` to `_, err := s.WriteParallel(...)`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./diskstore/ -run TestWriteParallel -v`
Expected: FAIL — compile error: `WriteParallel` returns 1 value / `ErrVerify` undefined.

- [ ] **Step 4: Create `diskstore/verify.go`**

```go
package diskstore

import (
	"errors"
	"fmt"

	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// ErrVerify is returned (wrapped) when an object's key does not match its
// payload. Callers distinguish it with errors.Is to map to a client error.
var ErrVerify = errors.New("diskstore: object verification failed")

// verifyObject recomputes o.Key from o.Data and reports ErrVerify on mismatch.
// For Blob and XattrSet — whose key length is the serialized byte length — it
// also checks the length field. Aggregate types (FileNode/DirLeaf/DirNode) carry
// a logical length the store cannot recompute without parsing, so only their
// hash is checked.
func verifyObject(o Object) error {
	sum := blake3.Sum256(o.Data)
	want, err := key.NewFromHash(o.Key.Type(), o.Key.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
	}
	if want != o.Key {
		return fmt.Errorf("%w: payload hashes to %s, not %s", ErrVerify, want, o.Key)
	}
	switch o.Key.Type() {
	case key.Blob, key.XattrSet:
		if o.Key.Length() != uint64(len(o.Data)) {
			return fmt.Errorf("%w: %s length field %d != payload %d", ErrVerify, o.Key, o.Key.Length(), len(o.Data))
		}
	}
	return nil
}
```

- [ ] **Step 5: Modify `diskstore/parallel.go`**

Add to the top-level declarations (near `WriteOpts`):

```go
// WriteStats summarizes one WriteParallel run.
type WriteStats struct {
	Stored      int   // objects newly written
	Deduped     int   // objects skipped (already present, or duplicated in the stream)
	BytesStored int64 // payload bytes of newly-written objects
}
```

Add a `Verify` field to `WriteOpts`:

```go
// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int  // concurrent batch writers; <= 0 means GOMAXPROCS
	BatchSize int  // commit when a batch reaches this many bytes; <= 0 means DefaultBatchSize
	Verify    bool // recompute and check each new object's key before storing it
}
```

Change `WriteParallel`'s signature and body to accumulate stats (add `"sync/atomic"` to the imports):

```go
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error) {
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
	var stored, deduped, bytesStored atomic.Int64

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
			err := s.runWriter(ctx, ch, seen, batchSize, opts.Verify, &stored, &deduped, &bytesStored)
			if err != nil {
				cancel()
			}
			return err
		})
	}

	err := eg.Wait()
	return WriteStats{
		Stored:      int(stored.Load()),
		Deduped:     int(deduped.Load()),
		BytesStored: bytesStored.Load(),
	}, err
}
```

Change `runWriter`'s signature and the three accounting points:

```go
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int, verify bool, stored, deduped, bytesStored *atomic.Int64) (err error) {
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
				deduped.Add(1)
				continue
			}
			has, err := s.Has(obj.Key)
			if err != nil {
				return err
			}
			if has {
				deduped.Add(1)
				continue
			}
			if verify {
				if err := verifyObject(obj); err != nil {
					return err
				}
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
			stored.Add(1)
			bytesStored.Add(int64(len(obj.Data)))
			if b.Len() >= batchSize {
				if err := commit(); err != nil {
					return err
				}
			}
		}
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./diskstore/ -race -v`
Expected: PASS (existing + new tests; `-race` exercises the atomic counters across writers).

- [ ] **Step 7: Commit**

```bash
git add diskstore/
git commit -m "feat(diskstore): WriteParallel verification and write stats"
```

---

## Task 3: `tarexport` — CAS directory key → PAX tar

**Files:**
- Create: `tarexport/tarexport.go`
- Test: `tarexport/tarexport_test.go`

- [ ] **Step 1: Write the failing test**

```go
package tarexport_test

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/tarexport"
)

// buildStore ingests three blobs + a single-leaf directory referencing two
// regular files, returning the store and the directory root key. It encodes the
// objects directly with fstree to avoid depending on the CLI.
func TestWrite_RegularFilesAndDir(t *testing.T) {
	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	put := func(o fstree.Object) {
		t.Helper()
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	// Two files: "a" -> "alpha", "b" -> "beta" (each a single Blob).
	ablob, _ := fstree.EncodeBlob([]byte("alpha"))
	bblob, _ := fstree.EncodeBlob([]byte("beta"))
	put(ablob)
	put(bblob)

	// A directory leaf with two regular-file entries (mode 0o100644).
	entries := []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: ablob.Key[:]},
		{Name: []byte("b"), Mode: 0o100644, Mtime: 2, ContentKey: bblob.Key[:]},
	}
	leaf, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	put(leaf)

	var buf bytes.Buffer
	if err := tarexport.Write(&buf, leaf.Key, store.Get); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := map[string]string{}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if got["a"] != "alpha" || got["b"] != "beta" {
		t.Fatalf("tar contents = %v, want a=alpha b=beta", got)
	}
}

func TestWrite_RejectsNonDirectoryRoot(t *testing.T) {
	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blob, _ := fstree.EncodeBlob([]byte("x"))
	if err := store.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tarexport.Write(&buf, blob.Key, store.Get); err == nil {
		t.Fatalf("expected error exporting a non-directory root (type %v)", key.Blob)
	}
}
```

Confirm `fstree.EncodeDirLeaf(entries []fstree.Entry) (fstree.Object, error)` exists with that signature (it is the encoder paired with `DecodeDirLeaf`). If the real signature differs, adapt the test's leaf construction accordingly.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tarexport/ -v`
Expected: FAIL — `undefined: tarexport.Write`.

- [ ] **Step 3: Write the implementation**

```go
// Package tarexport traverses an Amber-Store CAS from a directory key and writes
// a PAX-format tar of the filesystem tree. PAX is required to faithfully carry
// nanosecond mtimes, extended attributes (SCHILY.xattr.*), long names, and device
// nodes. Sockets cannot be archived and are skipped. The tree's root directory
// itself is not emitted (its metadata is not stored); only its descendants are.
package tarexport

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// Getter fetches the bytes stored under a key.
type Getter func(key.Key) ([]byte, error)

// Write streams a PAX tar of the directory tree rooted at root to w. root must
// be a directory object (DirLeaf or DirNode).
func Write(w io.Writer, root key.Key, get Getter) error {
	if root.Type() != key.DirLeaf && root.Type() != key.DirNode {
		return fmt.Errorf("tarexport: root %s is not a directory object (type %v)", root, root.Type())
	}
	tw := tar.NewWriter(w)
	e := &exporter{tw: tw, get: get}
	if err := e.dir(root, ""); err != nil {
		return err
	}
	return tw.Close()
}

type exporter struct {
	tw  *tar.Writer
	get Getter
}

// dir writes every entry of the directory object dirKey, prefixing names with
// prefix (the path of the directory relative to the export root, "" for root).
func (e *exporter) dir(dirKey key.Key, prefix string) error {
	entries, err := e.collectEntries(dirKey)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if err := e.entry(prefix, ent); err != nil {
			return err
		}
	}
	return nil
}

// collectEntries returns the entries reachable from k, descending DirNode index
// levels into the DirLeaves that hold them.
func (e *exporter) collectEntries(k key.Key) ([]fstree.Entry, error) {
	data, err := e.get(k)
	if err != nil {
		return nil, fmt.Errorf("tarexport: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		return fstree.DecodeDirLeaf(data)
	case key.DirNode:
		pairs, err := fstree.DecodeDirNode(data)
		if err != nil {
			return nil, err
		}
		var out []fstree.Entry
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, err
			}
			sub, err := e.collectEntries(ck)
			if err != nil {
				return nil, err
			}
			out = append(out, sub...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("tarexport: %s is not a directory object (type %v)", k, k.Type())
	}
}

func (e *exporter) entry(prefix string, ent fstree.Entry) error {
	name := path.Join(prefix, string(ent.Name))
	hdr := &tar.Header{
		Name:    name,
		Mode:    int64(ent.Mode & 0o7777),
		Uid:     int(ent.UID),
		Gid:     int(ent.GID),
		ModTime: time.Unix(0, ent.Mtime),
		Format:  tar.FormatPAX,
	}
	if err := e.setXattrs(hdr, ent); err != nil {
		return err
	}

	switch ent.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		hdr.Typeflag = tar.TypeDir
		hdr.Name = name + "/"
		if err := e.tw.WriteHeader(hdr); err != nil {
			return err
		}
		ck, err := key.Parse(ent.ContentKey)
		if err != nil {
			return err
		}
		return e.dir(ck, name)

	case unix.S_IFREG:
		ck, err := key.Parse(ent.ContentKey)
		if err != nil {
			return err
		}
		hdr.Typeflag = tar.TypeReg
		hdr.Size = int64(ck.Length()) // FileNode/Blob length == content byte count
		if err := e.tw.WriteHeader(hdr); err != nil {
			return err
		}
		return e.writeContent(ck)

	case unix.S_IFLNK:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Linkname = string(ent.LinkTarget)
		return e.tw.WriteHeader(hdr)

	case unix.S_IFIFO:
		hdr.Typeflag = tar.TypeFifo
		return e.tw.WriteHeader(hdr)

	case unix.S_IFCHR, unix.S_IFBLK:
		if len(ent.Rdev) != 2 {
			return fmt.Errorf("tarexport: %s: device entry missing [major, minor]", name)
		}
		if ent.Mode&unix.S_IFMT == unix.S_IFCHR {
			hdr.Typeflag = tar.TypeChar
		} else {
			hdr.Typeflag = tar.TypeBlock
		}
		hdr.Devmajor = int64(ent.Rdev[0])
		hdr.Devminor = int64(ent.Rdev[1])
		return e.tw.WriteHeader(hdr)

	case unix.S_IFSOCK:
		// Sockets cannot be archived; skip (consistent with restore).
		return nil

	default:
		return fmt.Errorf("tarexport: %s: unsupported file type %#o", name, ent.Mode&unix.S_IFMT)
	}
}

// writeContent writes the bytes addressed by k to the current tar member,
// descending FileNode index levels and concatenating Blob leaves in order.
func (e *exporter) writeContent(k key.Key) error {
	data, err := e.get(k)
	if err != nil {
		return fmt.Errorf("tarexport: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.Blob:
		_, err := e.tw.Write(data)
		return err
	case key.FileNode:
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := e.writeContent(ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("tarexport: %s is not a file-content object (type %v)", k, k.Type())
	}
}

// setXattrs decodes an entry's extended attributes (inline or spilled) into the
// header's PAX records under the SCHILY.xattr.* namespace.
func (e *exporter) setXattrs(hdr *tar.Header, ent fstree.Entry) error {
	var (
		m   map[string][]byte
		err error
	)
	switch {
	case len(ent.XattrsIn) > 0:
		m, err = cborx.DecodeXattrs(ent.XattrsIn)
	case len(ent.XattrsKey) == key.Size:
		xk, perr := key.Parse(ent.XattrsKey)
		if perr != nil {
			return perr
		}
		data, gerr := e.get(xk)
		if gerr != nil {
			return gerr
		}
		m, err = cborx.DecodeXattrs(data)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}
	if hdr.PAXRecords == nil {
		hdr.PAXRecords = make(map[string]string, len(m))
	}
	for k, v := range m {
		hdr.PAXRecords["SCHILY.xattr."+k] = string(v)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tarexport/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tarexport/
git commit -m "feat(tarexport): stream a PAX tar from a CAS directory key"
```

---

## Task 4: `tarextract` — PAX tar → filesystem with faithful metadata

**Files:**
- Create: `tarextract/tarextract.go`
- Test: `tarextract/tarextract_test.go`

- [ ] **Step 1: Write the failing test**

```go
package tarextract_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/tarextract"
)

func TestExtract_FilesDirsAndDeferredDirMeta(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 222_000_000)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(h *tar.Header, body string) {
		h.Format = tar.FormatPAX
		h.ModTime = mtime
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A read-only directory listed BEFORE its child file. If dir mode were applied
	// immediately, writing the child would fail; deferral makes it work.
	write(&tar.Header{Name: "ro/", Typeflag: tar.TypeDir, Mode: 0o500}, "")
	write(&tar.Header{Name: "ro/child.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5}, "hello")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "ro", "child.txt"))
	if err != nil {
		t.Fatalf("reading child: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("child content = %q, want hello", got)
	}
	di, err := os.Lstat(filepath.Join(dest, "ro"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o500 {
		t.Errorf("dir perm = %v, want 0500 (applied after children)", di.Mode().Perm())
	}
	if di.ModTime().UnixNano() != mtime.UnixNano() {
		t.Errorf("dir mtime = %d, want %d", di.ModTime().UnixNano(), mtime.UnixNano())
	}
	// Restore writability so t.TempDir()'s RemoveAll can delete the 0o500 dir.
	_ = os.Chmod(filepath.Join(dest, "ro"), 0o700)
}

func TestExtract_RejectsUnsafeName(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	h := &tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1, Format: tar.FormatPAX}
	if err := tw.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	tw.Write([]byte("x"))
	tw.Close()

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err == nil {
		t.Fatalf("expected error extracting an escaping path")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tarextract/ -v`
Expected: FAIL — `undefined: tarextract.Extract`.

- [ ] **Step 3: Write the implementation**

```go
// Package tarextract extracts a PAX tar (as produced by tarexport) into a
// directory, restoring permissions, ownership (when running as root), extended
// attributes (best-effort), nanosecond mtimes, symlinks, fifos, and device
// nodes. Directory permissions and mtimes are applied after all members are
// written, so a read-only or past-dated directory does not block writing its
// children — and creating children does not disturb a restored directory's
// mtime.
package tarextract

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const xattrPrefix = "SCHILY.xattr."

// Extract reads a tar from r and materializes it under destDir, which is created
// if necessary. destDir itself keeps default attributes (the export root's own
// metadata is not part of the archive).
func Extract(r io.Reader, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	tr := tar.NewReader(r)
	var dirs []*tar.Header // directories, for deferred metadata
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target, err := safeJoin(destDir, h.Name)
		if err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
			dirs = append(dirs, h)
		case tar.TypeReg, tar.TypeRegA:
			if err := writeRegular(target, tr); err != nil {
				return err
			}
			if err := applyMeta(target, h, false); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.Symlink(h.Linkname, target); err != nil {
				return err
			}
			if err := applyMeta(target, h, true); err != nil {
				return err
			}
		case tar.TypeFifo:
			if err := unix.Mkfifo(target, uint32(h.Mode&0o7777)); err != nil {
				return fmt.Errorf("%s: mkfifo: %w", target, err)
			}
			if err := applyMeta(target, h, false); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock:
			mode := uint32(h.Mode & 0o7777)
			if h.Typeflag == tar.TypeChar {
				mode |= unix.S_IFCHR
			} else {
				mode |= unix.S_IFBLK
			}
			dev := int(unix.Mkdev(uint32(h.Devmajor), uint32(h.Devminor)))
			if err := unix.Mknod(target, mode, dev); err != nil {
				if isPrivilegeError(err) {
					fmt.Fprintf(os.Stderr, "amber-store: skipping device node %s: %v\n", target, err)
					continue
				}
				return fmt.Errorf("%s: mknod: %w", target, err)
			}
			if err := applyMeta(target, h, false); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s: unsupported tar type %q", target, h.Typeflag)
		}
	}
	// Apply directory metadata last so child writes do not disturb it and a
	// read-only mode does not block them.
	for _, h := range dirs {
		target, err := safeJoin(destDir, h.Name)
		if err != nil {
			return err
		}
		if err := applyMeta(target, h, false); err != nil {
			return err
		}
	}
	return nil
}

// safeJoin joins name under dest, rejecting any name that escapes dest. A ".."
// component is rejected outright (filepath.Clean would otherwise silently
// collapse "../escape" to a path inside dest, hiding the corruption).
func safeJoin(dest, name string) (string, error) {
	for _, part := range strings.Split(filepath.ToSlash(name), "/") {
		if part == ".." {
			return "", fmt.Errorf("refusing unsafe entry name %q", name)
		}
	}
	clean := filepath.Clean("/" + name)
	target := filepath.Join(dest, clean)
	if target != dest && !strings.HasPrefix(target, dest+string(os.PathSeparator)) {
		return "", fmt.Errorf("refusing unsafe entry name %q", name)
	}
	return target, nil
}

func writeRegular(target string, r io.Reader) error {
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// applyMeta restores permissions, ownership, xattrs, and mtime for target.
// Permissions and mtime are faithful; ownership only when running as root;
// xattrs best-effort. mtime is set last. Symlink targets are not chmod'd and
// their xattrs are skipped.
func applyMeta(target string, h *tar.Header, isSymlink bool) error {
	if !isSymlink {
		if err := unix.Chmod(target, uint32(h.Mode&0o7777)); err != nil {
			return fmt.Errorf("%s: chmod: %w", target, err)
		}
	}
	if os.Geteuid() == 0 {
		if err := os.Lchown(target, h.Uid, h.Gid); err != nil {
			return fmt.Errorf("%s: chown: %w", target, err)
		}
	}
	if !isSymlink {
		for k, v := range h.PAXRecords {
			if !strings.HasPrefix(k, xattrPrefix) {
				continue
			}
			name := strings.TrimPrefix(k, xattrPrefix)
			if err := unix.Lsetxattr(target, name, []byte(v), 0); err != nil {
				if isPrivilegeError(err) || err == unix.ENOTSUP {
					fmt.Fprintf(os.Stderr, "amber-store: skipping xattr %q on %s: %v\n", name, target, err)
					continue
				}
				return fmt.Errorf("setting xattr %q on %s: %w", name, target, err)
			}
		}
	}
	return setMtime(target, h.ModTime.UnixNano(), isSymlink)
}

func setMtime(target string, ns int64, isSymlink bool) error {
	ts := unix.NsecToTimespec(ns)
	flags := 0
	if isSymlink {
		flags = unix.AT_SYMLINK_NOFOLLOW
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, target, []unix.Timespec{ts, ts}, flags); err != nil {
		return fmt.Errorf("%s: set mtime: %w", target, err)
	}
	return nil
}

func isPrivilegeError(err error) bool {
	return err == unix.EPERM || err == unix.EACCES || err == unix.EOPNOTSUPP
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tarextract/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tarextract/
git commit -m "feat(tarextract): extract a PAX tar with faithful metadata"
```

---

## Task 5: `internal/socketpath` — cross-platform socket resolver

**Files:**
- Create: `internal/socketpath/socketpath.go`
- Test: `internal/socketpath/socketpath_test.go`

- [ ] **Step 1: Write the failing test**

```go
package socketpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolve_FlagWins(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got := Resolve("/from/flag.sock"); got != "/from/flag.sock" {
		t.Fatalf("Resolve(flag) = %q, want /from/flag.sock", got)
	}
}

func TestResolve_EnvBeatsDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got := Resolve(""); got != "/from/env.sock" {
		t.Fatalf("Resolve(\"\") = %q, want /from/env.sock", got)
	}
}

func TestResolve_XDGDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	want := "/run/user/1000/amber-store.sock"
	if got := Resolve(""); got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}

func TestResolve_TempDirFallback(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "") // unset -> fall back to os.TempDir (macOS path)
	got := Resolve("")
	want := filepath.Join(os.TempDir(), perUserName())
	if got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}

func TestResolve_IgnoresRelativeXDG(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "relative/dir") // not absolute -> ignored
	got := Resolve("")
	want := filepath.Join(os.TempDir(), perUserName())
	if got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/socketpath/ -v`
Expected: FAIL — `undefined: Resolve`.

- [ ] **Step 3: Write the implementation**

```go
// Package socketpath resolves the unix-socket path that the daemon and its CLI
// clients agree on. Resolution order: explicit flag, AMBER_STORE_SOCKET env, then
// a platform default — $XDG_RUNTIME_DIR/amber-store.sock when XDG_RUNTIME_DIR is
// set and absolute (typical on Linux), otherwise os.TempDir()/amber-store-<uid>.sock.
// On macOS XDG_RUNTIME_DIR is unset, so os.TempDir() (the secure per-user $TMPDIR)
// is used; the per-uid suffix keeps the Linux /tmp fallback from colliding across
// users.
package socketpath

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnvVar is the environment variable that overrides the default socket path.
const EnvVar = "AMBER_STORE_SOCKET"

// Resolve returns the socket path given a possibly-empty flag value.
func Resolve(flag string) string {
	if flag != "" {
		return flag
	}
	if env := os.Getenv(EnvVar); env != "" {
		return env
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "amber-store.sock")
	}
	return filepath.Join(os.TempDir(), perUserName())
}

func perUserName() string {
	return fmt.Sprintf("amber-store-%d.sock", os.Getuid())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/socketpath/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/socketpath/
git commit -m "feat(socketpath): cross-platform unix socket path resolver"
```

---

## Task 6: `client` — HTTP client over the unix socket

**Files:**
- Create: `client/client.go`
- Test: deferred to Task 7 (`daemon_test.go` exercises the client end-to-end against the real handler over a socket).

- [ ] **Step 1: Write the implementation**

```go
// Package client talks to the amber-store daemon over a unix socket using
// HTTP/1.1. The same calls work against a TCP daemon by changing the dialer.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/draganm/amber-store/key"
)

// Stats mirrors the daemon's POST /v1/objects response.
type Stats struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// Client issues requests to a daemon listening on a unix socket.
type Client struct {
	hc       *http.Client
	sockPath string
}

// New returns a Client dialing the unix socket at sockPath.
func New(sockPath string) *Client {
	return &Client{
		sockPath: sockPath,
		hc: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
				},
			},
		},
	}
}

// baseURL is a fixed authority; the host is ignored for unix sockets.
const baseURL = "http://amber"

// dialHint augments a connection error with a reminder to start the daemon.
func (c *Client) dialHint(err error) error {
	return fmt.Errorf("contacting daemon at %s: %w (is the daemon running? try: amber-store daemon --store DIR)", c.sockPath, err)
}

// Ingest uploads a pack-write stream and returns the daemon's store stats.
func (c *Client) Ingest(ctx context.Context, body io.Reader) (Stats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/objects", body)
	if err != nil {
		return Stats{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.hc.Do(req)
	if err != nil {
		return Stats{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return Stats{}, fmt.Errorf("ingest failed: %s: %s", resp.Status, msg)
	}
	var s Stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Stats{}, fmt.Errorf("decoding ingest response: %w", err)
	}
	return s, nil
}

// Tar requests the directory tar for k. The caller must close the returned
// reader. A non-2xx status is drained and returned as an error.
func (c *Client) Tar(ctx context.Context, k key.Key) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/tar/"+k.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		resp.Body.Close()
		return nil, fmt.Errorf("tar request failed: %s: %s", resp.Status, msg)
	}
	return resp.Body, nil
}
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./client/`
Expected: success (no output).

- [ ] **Step 3: Commit**

```bash
git add client/
git commit -m "feat(client): HTTP client over the unix socket"
```

---

## Task 7: `daemon` — POST /v1/objects (verify + store + stats)

**Files:**
- Create: `daemon/daemon.go`
- Test: `daemon/daemon_test.go`

- [ ] **Step 1: Write the failing test**

```go
package daemon_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// serveOnSocket starts the daemon handler on a fresh unix socket under a temp
// dir and returns a client for it. The listener closes at test end.
func serveOnSocket(t *testing.T, store *diskstore.Store) *client.Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.New(store)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return client.New(sock)
}

func openStore(t *testing.T) *diskstore.Store {
	t.Helper()
	s, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// packOf serializes objects into a pack-write stream.
func packOf(t *testing.T, objs ...fstree.Object) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	w := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func mustBlob(t *testing.T, s string) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestPostObjects_StoresAndReportsStats(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	a, b := mustBlob(t, "alpha"), mustBlob(t, "beta")
	stats, err := c.Ingest(context.Background(), packOf(t, a, b, a))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if stats.ObjectsStored != 2 || stats.ObjectsDeduped != 1 {
		t.Fatalf("stats = %+v, want stored=2 deduped=1", stats)
	}
	if got, _ := store.Get(a.Key); string(got) != "alpha" {
		t.Fatalf("stored blob = %q, want alpha", got)
	}
}

func TestPostObjects_RejectsTamperedKey(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	good := mustBlob(t, "honest")
	bad := good
	bad.Key[key.Size-1] ^= 0xFF // key no longer matches payload

	_, err := c.Ingest(context.Background(), packOf(t, bad))
	if err == nil {
		t.Fatalf("expected error uploading a tampered object")
	}
	if has, _ := store.Has(bad.Key); has {
		t.Fatalf("tampered object must not be stored")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./daemon/ -run TestPostObjects -v`
Expected: FAIL — `undefined: daemon.New`.

- [ ] **Step 3: Write `daemon/daemon.go` (handler + POST route)**

```go
// Package daemon serves the amber-store CAS over HTTP. The same http.Handler is
// transport-agnostic: a unix listener today, a TCP listener later. Routes are
// versioned under /v1.
package daemon

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/tarexport"
)

type handler struct {
	store *diskstore.Store
}

// New returns an http.Handler serving the store.
func New(store *diskstore.Store) http.Handler {
	h := &handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	return mux
}

type ingestResponse struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// postObjects decodes a pack-write stream, verifies and stores its objects, and
// returns store stats. Malformed-stream and verification failures are client
// errors (422); other failures are 500.
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request) {
	rd := amberpack.NewReader(r.Body)
	seq := func(yield func(diskstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(diskstore.Object{}, err)
				return
			}
			if !yield(diskstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	stats, err := h.store.WriteParallel(seq, diskstore.WriteOpts{Verify: true})
	if err != nil {
		if errors.Is(err, amberpack.ErrMalformed) || errors.Is(err, diskstore.ErrVerify) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ingestResponse{
		ObjectsStored:  stats.Stored,
		ObjectsDeduped: stats.Deduped,
		BytesStored:    stats.BytesStored,
	})
}

// getTar is implemented in Task 8.
func (h *handler) getTar(w http.ResponseWriter, r *http.Request) {
	_ = tarexport.Write // referenced fully in Task 8
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
```

Note: the `_ = tarexport.Write` line keeps the import live until Task 8 replaces `getTar`. If you prefer, implement Task 8 immediately after and drop this placeholder line.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./daemon/ -run TestPostObjects -race -v`
Expected: PASS (client → unix socket → handler → store, including the tamper rejection).

- [ ] **Step 5: Commit**

```bash
git add daemon/
git commit -m "feat(daemon): POST /v1/objects with verification and stats"
```

---

## Task 8: `daemon` — GET /v1/tar/{key}

**Files:**
- Modify: `daemon/daemon.go` (replace `getTar`)
- Modify: `daemon/daemon_test.go` (add tar + split-upload tests)

- [ ] **Step 1: Write the failing tests** (append to `daemon/daemon_test.go`)

```go
import additions: "archive/tar", "io" at the top of the test file.

func TestGetTar_RoundTripAfterSplitUpload(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	// Build a one-leaf directory with two files, then upload the objects in TWO
	// separate packs (the blobs in one, the leaf in another) to exercise partial
	// uploads.
	a, b := mustBlob(t, "alpha"), mustBlob(t, "beta")
	entries := []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: a.Key[:]},
		{Name: []byte("b"), Mode: 0o100644, Mtime: 2, ContentKey: b.Key[:]},
	}
	leaf, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.Ingest(context.Background(), packOf(t, a, b)); err != nil {
		t.Fatalf("upload blobs: %v", err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, leaf)); err != nil {
		t.Fatalf("upload leaf: %v", err)
	}

	body, err := c.Tar(context.Background(), leaf.Key)
	if err != nil {
		t.Fatalf("Tar: %v", err)
	}
	defer body.Close()

	got := map[string]string{}
	tr := tar.NewReader(body)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if got["a"] != "alpha" || got["b"] != "beta" {
		t.Fatalf("tar = %v, want a=alpha b=beta", got)
	}
}

func TestGetTar_MissingRootIs404(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)
	// A well-formed directory key that was never stored.
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("x"), Mode: 0o100644, ContentKey: mustBlob(t, "x").Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Tar(context.Background(), leaf.Key)
	if err == nil {
		t.Fatalf("expected error fetching an absent root")
	}
}

func TestGetTar_NonDirectoryKeyIs400(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)
	blob := mustBlob(t, "data")
	if _, err := c.Ingest(context.Background(), packOf(t, blob)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tar(context.Background(), blob.Key); err == nil {
		t.Fatalf("expected error fetching a tar for a blob key")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./daemon/ -run TestGetTar -v`
Expected: FAIL — `501 Not Implemented` surfaced as a client error / wrong status.

- [ ] **Step 3: Replace `getTar` in `daemon/daemon.go`**

Add imports `"encoding/hex"`, `"github.com/draganm/amber-store/key"`, `"github.com/draganm/amber-store/tarexport"` (remove the placeholder `_ = tarexport.Write` line), then:

```go
// getTar streams a PAX tar of the directory tree rooted at the {key} path value.
func (h *handler) getTar(w http.ResponseWriter, r *http.Request) {
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if k.Type() != key.DirLeaf && k.Type() != key.DirNode {
		http.Error(w, "key is not a directory object", http.StatusBadRequest)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "root object not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarexport.Write(w, k, h.store.Get); err != nil {
		// The response status is already 200 and bytes may be in flight; we cannot
		// change it. Log and let the truncated archive surface as a tar read error
		// on the client.
		log.Printf("amber-store daemon: tar export of %s aborted: %v", k, err)
	}
}

// parseHexKey decodes a lowercase-hex key path segment into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return key.Parse(raw)
}
```

Add `"fmt"` and `"log"` to the imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./daemon/ -race -v`
Expected: PASS (all daemon tests, including the split-upload round-trip).

- [ ] **Step 5: Commit**

```bash
git add daemon/
git commit -m "feat(daemon): GET /v1/tar/{key} streaming PAX export"
```

---

## Task 9: CLI `daemon` command

**Files:**
- Create: `cmd/amber-store/daemon.go`
- Modify: `cmd/amber-store/main.go` (register the command — full rewiring in Task 13; here just add `daemonCommand()`)
- Test: `cmd/amber-store/daemon_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/amberpack"
	"bytes"
)

// startDaemon runs the daemon command in a goroutine on a temp store + socket and
// waits until the socket accepts connections. It returns the socket path.
func startDaemon(t *testing.T) string {
	t.Helper()
	storeDir := t.TempDir()
	sock := filepath.Join(t.TempDir(), "d.sock")

	app := newApp()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = app.RunContext(ctx, []string{"amber-store", "daemon", "--store", storeDir, "--socket", sock})
	}()
	t.Cleanup(func() { cancel(); <-done })

	// Wait for the socket file to appear and accept a connection.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			return sock
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("daemon socket did not appear")
	return ""
}

func TestDaemon_ServesIngest(t *testing.T) {
	sock := startDaemon(t)
	c := client.New(sock)

	o, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w := amberpack.NewWriter(&buf)
	if err := w.Add(o); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats, err := c.Ingest(context.Background(), &buf)
	if err != nil {
		t.Fatalf("Ingest against daemon command: %v", err)
	}
	if stats.ObjectsStored != 1 {
		t.Fatalf("stored = %d, want 1", stats.ObjectsStored)
	}
}
```

This test calls `app.RunContext`; `urfave/cli/v2`'s `App.RunContext(ctx, args)` exists. The daemon command must honor context cancellation for shutdown.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestDaemon_ServesIngest -v`
Expected: FAIL — unknown command `daemon` (or compile error if `daemonCommand` is missing).

- [ ] **Step 3: Write `cmd/amber-store/daemon.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

type daemonConfig struct {
	store           string
	socket          string
	inlineThreshold int
	sync            bool
}

func daemonCommand() *cli.Command {
	cfg := &daemonConfig{}
	return &cli.Command{
		Name:  "daemon",
		Usage: "run the store-owning daemon, serving clients over a unix socket",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "store",
				Aliases:     []string{"s"},
				Usage:       "diskstore directory (created if missing)",
				Required:    true,
				Destination: &cfg.store,
			},
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "unix socket path (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.IntFlag{
				Name:        "inline-threshold",
				Value:       diskstore.DefaultInlineThreshold,
				Usage:       "objects larger than this many bytes are stored as external blob files",
				Destination: &cfg.inlineThreshold,
			},
			&cli.BoolFlag{
				Name:        "sync",
				Value:       true,
				Usage:       "fsync writes for crash durability",
				Destination: &cfg.sync,
			},
		},
		Action: func(c *cli.Context) error { return runDaemon(c, cfg) },
	}
}

func runDaemon(c *cli.Context, cfg *daemonConfig) error {
	store, err := diskstore.Open(cfg.store,
		diskstore.WithInlineThreshold(cfg.inlineThreshold),
		diskstore.WithSync(cfg.sync),
	)
	if err != nil {
		return err
	}
	defer store.Close()

	sock := socketpath.Resolve(cfg.socket)
	// Remove a stale socket from a previous run before binding.
	if err := os.Remove(sock); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing stale socket %s: %w", sock, err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", sock, err)
	}
	defer os.Remove(sock)

	srv := &http.Server{Handler: daemon.New(store)}

	// Shut down gracefully on context cancellation (tests) or SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	fmt.Fprintf(os.Stderr, "amber-store daemon listening on %s\n", sock)
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
```

- [ ] **Step 4: Register the command in `main.go`**

Add `daemonCommand(),` to the `Commands` slice in `newApp()` (keep the others for now; they are rewired in later tasks).

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestDaemon_ServesIngest -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/daemon.go cmd/amber-store/daemon_test.go cmd/amber-store/main.go
git commit -m "feat(cli): daemon command serving over a unix socket"
```

---

## Task 10: Rewire `ingest` — build a pack, stream to daemon or write a file

**Files:**
- Modify: `cmd/amber-store/ingest.go` (retype `ingestObjects` to `fstree.Object`; rewrite `ingestConfig`, `ingestCommand`, `runIngest`)
- Modify: `cmd/amber-store/ingest_test.go` (replace `pack()`-based parity with a sequential-driver reference; rewrite CLI tests around `--output`)

- [ ] **Step 1: Retype `ingestObjects` to yield `fstree.Object`**

In `cmd/amber-store/ingest.go`, change the channel type and emit so the iterator yields `fstree.Object` instead of `diskstore.Object`. Replace the function signature and the two object-construction sites:

```go
func ingestObjects(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, p *Progress, root *key.Key) iter.Seq2[fstree.Object, error] {
	if jobs < 1 {
		jobs = 1
	}
	return func(yield func(fstree.Object, error) bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan fstree.Object, jobs*2)
		var buildErr error

		go func() {
			defer close(ch)
			emit := func(o fstree.Object) error {
				select {
				case ch <- o:
					return nil
				case <-ctx.Done():
					return errIngestStopped
				}
			}
			b := &pbuilder{
				d:    &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax, p: p},
				emit: emit,
				sem:  make(chan struct{}, jobs),
			}
			rk, err := b.buildDir(dir, emit)
			if err != nil {
				if !errors.Is(err, errIngestStopped) {
					buildErr = err
				}
				return
			}
			*root = rk
		}()

		for o := range ch {
			if !yield(o, nil) {
				cancel()
				for range ch {
				}
				return
			}
		}
		if buildErr != nil {
			yield(fstree.Object{}, buildErr)
		}
	}
}
```

Remove the now-unused `diskstore` import from `ingest.go` if nothing else there uses it.

- [ ] **Step 2: Rewrite `ingestConfig`, `ingestCommand`, `runIngest`**

Replace the `ingestConfig` struct and `ingestCommand`/`runIngest` (the chunk/jobs/progress machinery stays; `--store`, `--writers`, `--inline-threshold`, `--sync` move to the daemon; add `--socket` and `--output`):

```go
type ingestConfig struct {
	chunk      chunkConfig
	socket     string
	output     string
	jobs       int
	noProgress bool
}

func ingestCommand() *cli.Command {
	cfg := &ingestConfig{}
	flags := append(chunkFlags(&cfg.chunk),
		&cli.StringFlag{
			Name:        "socket",
			Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
			Destination: &cfg.socket,
		},
		&cli.StringFlag{
			Name:        "output",
			Aliases:     []string{"o"},
			Usage:       "write the pack to FILE instead of streaming to the daemon",
			Destination: &cfg.output,
		},
		&cli.IntFlag{
			Name:        "jobs",
			Aliases:     []string{"j"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "concurrent workers building the tree (default: number of CPUs)",
			Destination: &cfg.jobs,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &cfg.noProgress,
		},
	)
	return &cli.Command{
		Name:      "ingest",
		Usage:     "build the content-addressed tree for DIR and store it via the daemon (or write a pack file with --output)",
		ArgsUsage: "DIR",
		Flags:     flags,
		Action:    func(c *cli.Context) error { return runIngest(c, cfg) },
	}
}

// writePack builds the tree at dir and serializes every object into dst as a
// pack-write stream, returning the resolved root key.
func writePack(dst io.Writer, dir string, cc *chunkConfig, jobs int, p *Progress) (key.Key, error) {
	byteOpts, err := cc.byteOpts()
	if err != nil {
		return key.Key{}, err
	}
	pw := amberpack.NewWriter(dst)
	var root key.Key
	for o, err := range ingestObjects(dir, cc.itemChunker(), byteOpts, cc.xattrInlineMax, jobs, p, &root) {
		if err != nil {
			return key.Key{}, err
		}
		if err := pw.Add(o); err != nil {
			return key.Key{}, err
		}
	}
	if err := pw.Close(); err != nil {
		return key.Key{}, err
	}
	return root, nil
}

func runIngest(c *cli.Context, cfg *ingestConfig) error {
	dir, err := dirArg(c, "ingest")
	if err != nil {
		return err
	}

	// Progress (client-side) is sized by a cheap pre-scan, unless disabled.
	var prog *Progress
	var pwg sync.WaitGroup
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()
	if !cfg.noProgress {
		totalFiles, totalBytes, err := scanTree(dir, cfg.jobs)
		if err != nil {
			return err
		}
		prog = NewProgress(totalFiles, totalBytes)
		isTTY := isTerminal(os.Stderr)
		start := time.Now()
		pwg.Go(func() { prog.Run(ctx, os.Stderr, start, isTTY) })
	}

	var root key.Key
	if cfg.output != "" {
		// Offline: write the pack to a file; do not contact the daemon.
		f, err := os.Create(cfg.output)
		if err != nil {
			return err
		}
		root, err = writePack(f, dir, &cfg.chunk, cfg.jobs, prog)
		if err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	} else {
		// Stream the pack to the daemon: build into a pipe consumed as the request
		// body, capturing the build's root and error out-of-band.
		pr, pw := io.Pipe()
		type result struct {
			root key.Key
			err  error
		}
		resCh := make(chan result, 1)
		go func() {
			r, err := writePack(pw, dir, &cfg.chunk, cfg.jobs, prog)
			if err != nil {
				pw.CloseWithError(err)
			} else {
				pw.Close()
			}
			resCh <- result{r, err}
		}()
		_, ingErr := client.New(socketpath.Resolve(cfg.socket)).Ingest(ctx, pr)
		res := <-resCh
		if res.err != nil {
			return res.err
		}
		if ingErr != nil {
			return ingErr
		}
		root = res.root
	}

	cancel()
	pwg.Wait()
	fmt.Fprintf(c.App.Writer, "%s\n", root.String())
	return nil
}
```

Update `ingest.go`'s imports: add `io`, `github.com/draganm/amber-store/amberpack`, `github.com/draganm/amber-store/client`, `github.com/draganm/amber-store/internal/socketpath`; keep `context`, `errors`, `fmt`, `iter`, `os`, `path/filepath`, `sync`, `time`, `runtime`, `chunkers`, `fstree`, `key`, `cli`. Remove `diskstore` if unused.

- [ ] **Step 3: Rewrite the parity + CLI tests in `ingest_test.go`**

Replace the entire file body's `pack()`-dependent tests. New reference helper + tests:

```go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// collectSequential builds the tree at dir with the sequential driver and returns
// the root plus a map of every emitted object's key to its bytes. This is the
// reference oracle that pack() used to provide.
func collectSequential(t *testing.T, dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	emit := func(o fstree.Object) error {
		objs[o.Key] = append([]byte(nil), o.Bytes...)
		return nil
	}
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}
	root, err := d.buildDir(dir, emit)
	if err != nil {
		t.Fatalf("sequential build: %v", err)
	}
	return root, objs
}

// collectParallel drains ingestObjects into a key->bytes map and returns the root.
func collectParallel(t *testing.T, dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax, jobs int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	var root key.Key
	for o, err := range ingestObjects(dir, ic, byteOpts, xattrInlineMax, jobs, nil, &root) {
		if err != nil {
			t.Fatalf("parallel build: %v", err)
		}
		objs[o.Key] = append([]byte(nil), o.Bytes...)
	}
	return root, objs
}

func assertSameObjects(t *testing.T, want, got map[key.Key][]byte) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("object count: want %d, got %d", len(want), len(got))
	}
	for k, wb := range want {
		gb, ok := got[k]
		if !ok {
			t.Errorf("missing object %s", k)
			continue
		}
		if !bytes.Equal(wb, gb) {
			t.Errorf("object %s bytes differ", k)
		}
	}
}

func TestIngestObjects_ParityWithSequential(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, ic, nil, 256)
	parRoot, parObjs := collectParallel(t, dir, ic, nil, 256, 4)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	assertSameObjects(t, seqObjs, parObjs)
}

func TestIngestObjects_ParallelParityDeepTree(t *testing.T) {
	dir := t.TempDir()
	writeDeepTree(t, dir)
	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, ic, nil, 256)
	jobs := max(runtime.NumCPU(), 4)
	parRoot, parObjs := collectParallel(t, dir, ic, nil, 256, jobs)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	if len(seqObjs) < 50 {
		t.Fatalf("deep tree produced only %d objects; expected a large fan-out", len(seqObjs))
	}
	assertSameObjects(t, seqObjs, parObjs)
}

// packRoots decodes a pack file and returns the set of object keys it contains.
func packKeys(t *testing.T, path string) map[key.Key]bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	keys := map[key.Key]bool{}
	for o, err := range amberpack.NewReader(f).All() {
		if err != nil {
			t.Fatalf("reading pack: %v", err)
		}
		keys[o.Key] = true
	}
	return keys
}

func TestRunIngest_OutputFileContainsRoot(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "tree.amberpack")

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatal(err)
	}
	rootHex := strings.TrimSpace(buf.String())
	raw, err := hex.DecodeString(rootHex)
	if err != nil {
		t.Fatalf("root not hex: %v", err)
	}
	root, err := key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !packKeys(t, out)[root] {
		t.Errorf("pack file does not contain the printed root %s", root)
	}
}

func TestRunIngest_DeterministicAcrossJobs(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)
	roots := make([]string, 0, 2)
	for _, j := range []string{"1", "8"} {
		var buf bytes.Buffer
		app := newApp()
		app.Writer = &buf
		out := filepath.Join(t.TempDir(), "p.amberpack")
		args := []string{"amber-store", "ingest", "--no-progress", "--jobs", j, "-o", out, src}
		if err := app.Run(args); err != nil {
			t.Fatalf("--jobs %s: %v", j, err)
		}
		roots = append(roots, strings.TrimSpace(buf.String()))
	}
	if roots[0] != roots[1] {
		t.Fatalf("root differs across --jobs: %q vs %q", roots[0], roots[1])
	}
}

func TestRunIngest_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	out := filepath.Join(t.TempDir(), "p.amberpack")
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, f}); err == nil {
		t.Errorf("expected error ingesting a non-directory")
	}
}

func TestIngestObjects_ReportsProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}
	p := NewProgress(2, 11)
	var root key.Key
	for _, err := range ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, 2, p, &root) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := p.bytesDone.Load(); got != 11 {
		t.Errorf("bytesDone = %d, want 11", got)
	}
	if got := p.filesDone.Load(); got != 2 {
		t.Errorf("filesDone = %d, want 2", got)
	}
}
```

Add `"encoding/hex"` to the imports. Keep `writeDeepTree`, `fillPseudoRandom` (used above); they no longer need `diskstore`/`hex` themselves but the file now imports `hex` for the root-decode test. Remove `cliDefaultByteOpts` (only the dropped pack-comparison tests used it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run 'TestIngest|TestRunIngest' -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/ingest.go cmd/amber-store/ingest_test.go
git commit -m "feat(cli): ingest builds a pack, streams to daemon or writes a file"
```

---

## Task 11: CLI `load` and `dump` commands

**Files:**
- Create: `cmd/amber-store/load.go`, `cmd/amber-store/dump.go`
- Modify: `cmd/amber-store/main.go` (register both)
- Test: `cmd/amber-store/loaddump_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadDump_RoundTrip ingests a tree to a pack file, loads it into a running
// daemon, then dumps the tar by root key and checks a known file is present.
func TestLoadDump_RoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hi there"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Build a pack file offline and capture the root key.
	out := filepath.Join(t.TempDir(), "tree.amberpack")
	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatal(err)
	}
	root := strings.TrimSpace(rootBuf.String())

	sock := startDaemon(t)

	// load the pack file into the daemon.
	app = newApp()
	if err := app.Run([]string{"amber-store", "load", "--socket", sock, out}); err != nil {
		t.Fatalf("load: %v", err)
	}

	// dump the tar to a file and verify it contains hello.txt with the content.
	tarPath := filepath.Join(t.TempDir(), "tree.tar")
	app = newApp()
	if err := app.Run([]string{"amber-store", "dump", "--socket", sock, "-o", tarPath, root}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	data, err := os.ReadFile(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("hi there")) {
		t.Errorf("dumped tar does not contain the file content")
	}
}
```

This reuses `startDaemon` from `daemon_test.go` (same package `main`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestLoadDump_RoundTrip -v`
Expected: FAIL — unknown commands `load`/`dump`.

- [ ] **Step 3: Write `cmd/amber-store/load.go`**

```go
package main

import (
	"fmt"
	"os"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

type loadConfig struct {
	socket string
}

func loadCommand() *cli.Command {
	cfg := &loadConfig{}
	return &cli.Command{
		Name:      "load",
		Usage:     "upload a prebuilt pack file to the daemon",
		ArgsUsage: "FILE",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
		},
		Action: func(c *cli.Context) error { return runLoad(c, cfg) },
	}
}

func runLoad(c *cli.Context, cfg *loadConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("load requires exactly one FILE argument, got %d", c.NArg())
	}
	f, err := os.Open(c.Args().First())
	if err != nil {
		return err
	}
	defer f.Close()

	stats, err := client.New(socketpath.Resolve(cfg.socket)).Ingest(c.Context, f)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "stored %d objects (%d deduped, %d bytes)\n",
		stats.ObjectsStored, stats.ObjectsDeduped, stats.BytesStored)
	return nil
}
```

- [ ] **Step 4: Write `cmd/amber-store/dump.go`**

```go
package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/key"
	"github.com/urfave/cli/v2"
)

type dumpConfig struct {
	socket string
	output string
}

func dumpCommand() *cli.Command {
	cfg := &dumpConfig{}
	return &cli.Command{
		Name:      "dump",
		Usage:     "fetch the directory tar for KEY from the daemon",
		ArgsUsage: "KEY",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "output tar file (default: stdout)",
				Destination: &cfg.output,
			},
		},
		Action: func(c *cli.Context) error { return runDump(c, cfg) },
	}
}

func runDump(c *cli.Context, cfg *dumpConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("dump requires exactly one KEY argument, got %d", c.NArg())
	}
	k, err := parseHexKey(c.Args().First())
	if err != nil {
		return err
	}

	body, err := client.New(socketpath.Resolve(cfg.socket)).Tar(c.Context, k)
	if err != nil {
		return err
	}
	defer body.Close()

	var out io.Writer = os.Stdout
	if cfg.output != "" {
		f, err := os.Create(cfg.output)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	_, err = io.Copy(out, body)
	return err
}

// parseHexKey decodes a lowercase-hex key argument into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	k, err := key.Parse(raw)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return k, nil
}
```

Note: `parseHexKey` is defined here and currently also exists in `restore.go`. Task 12 removes the copy in `restore.go` so this is the single definition. If implementing Task 12 first, skip redefining it.

- [ ] **Step 5: Register both in `main.go`**

Add `loadCommand(),` and `dumpCommand(),` to `newApp()`'s `Commands`.

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestLoadDump_RoundTrip -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/load.go cmd/amber-store/dump.go cmd/amber-store/loaddump_test.go cmd/amber-store/main.go
git commit -m "feat(cli): load and dump commands"
```

---

## Task 12: Rewrite `restore` over the daemon; remove its old internals

**Files:**
- Modify: `cmd/amber-store/restore.go` (fetch tar via client, extract via `tarextract`; drop the store-opening restorer, traversal, and metadata code now living in `tarexport`/`tarextract`; drop the duplicate `parseHexKey`)
- Modify: `cmd/amber-store/restore_test.go` (route through a daemon; reuse `compareTrees`/`buildSourceTree`)

- [ ] **Step 1: Replace `cmd/amber-store/restore.go` entirely**

```go
package main

import (
	"fmt"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/tarextract"
	"github.com/urfave/cli/v2"
)

type restoreConfig struct {
	socket string
}

func restoreCommand() *cli.Command {
	cfg := &restoreConfig{}
	return &cli.Command{
		Name:      "restore",
		Usage:     "restore the filesystem tree rooted at KEY (fetched from the daemon) into DIR",
		ArgsUsage: "KEY DIR",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
		},
		Action: func(c *cli.Context) error { return runRestore(c, cfg) },
	}
}

func runRestore(c *cli.Context, cfg *restoreConfig) error {
	if c.NArg() != 2 {
		return fmt.Errorf("restore requires exactly two arguments KEY and DIR, got %d", c.NArg())
	}
	k, err := parseHexKey(c.Args().Get(0))
	if err != nil {
		return err
	}
	outDir := c.Args().Get(1)

	body, err := client.New(socketpath.Resolve(cfg.socket)).Tar(c.Context, k)
	if err != nil {
		return err
	}
	defer body.Close()

	return tarextract.Extract(body, outDir)
}
```

This removes `restorer`, `collectEntries`, `restoreEntry`, `writeRegular`, `writeContent`, `applyMeta`, `restoreXattrs`, `setMtime`, `writeXattrs`, `isPrivilegeError`, and the duplicate `parseHexKey` from `restore.go` (the metadata/extraction logic now lives in `tarextract`; the traversal in `tarexport`; `parseHexKey` in `dump.go`).

- [ ] **Step 2: Handle the platform xattr/mtime helpers**

`writeXattrs`, `setMtime`, and `isPrivilegeError` were in `restore.go` and are now in `tarextract`. Check whether `cmd/amber-store/xattr_darwin.go`, `xattr_linux.go`, `xattr_common.go`, or `meta.go` still reference them. The build side keeps `readXattrs` (used by `driver.go`) and `entryMeta`/`deviceNumbers`/`setMtime` only if still referenced. Run `go build ./cmd/amber-store/` and resolve any "declared and not used"/"undefined" errors by deleting the now-orphaned helpers. `restore_test.go` used `writeXattrs` to seed a source xattr — replace that call (Step 3) with a direct `unix.Lsetxattr`.

- [ ] **Step 3: Rewrite `restore_test.go` to route through a daemon**

Keep `fixedTime`, `setTimes`, `buildSourceTree`, `compareTrees` unchanged. Replace `ingestTree`, the round-trip, xattr, and arg-validation tests:

```go
// ingestViaDaemon builds src into a pack file, starts a daemon, loads the pack,
// and returns the daemon socket and the tree root key.
func ingestViaDaemon(t *testing.T, src string) (sock string, root key.Key) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "tree.amberpack")
	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(rootBuf.String()))
	if err != nil {
		t.Fatal(err)
	}
	root, err = key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	sock = startDaemon(t)
	app = newApp()
	if err := app.Run([]string{"amber-store", "load", "--socket", sock, out}); err != nil {
		t.Fatalf("load: %v", err)
	}
	return sock, root
}

func TestRestore_RoundTrip(t *testing.T) {
	src := buildSourceTree(t)
	sock, root := ingestViaDaemon(t, src)

	out := filepath.Join(t.TempDir(), "restored")
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root.String(), out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	compareTrees(t, src, out)
}

func TestRestore_Xattrs(t *testing.T) {
	src := t.TempDir()
	f := filepath.Join(src, "file")
	if err := os.WriteFile(f, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := []byte("hello world")
	if err := unix.Lsetxattr(f, "user.comment", want, 0); err != nil {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}

	sock, root := ingestViaDaemon(t, src)
	out := filepath.Join(t.TempDir(), "restored")
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root.String(), out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := getXattr(t, filepath.Join(out, "file"), "user.comment")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("restored xattr = %q, want %q", got, want)
	}
}

// getXattr reads a single xattr value via unix.Lgetxattr.
func getXattr(t *testing.T, path, name string) ([]byte, error) {
	t.Helper()
	buf := make([]byte, 1024)
	n, err := unix.Lgetxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func TestRunRestore_RejectsBadKey(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "not-a-key", t.TempDir()}); err == nil {
		t.Errorf("expected error for malformed key")
	}
}

func TestRunRestore_RequiresTwoArgs(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "deadbeef"}); err == nil {
		t.Errorf("expected error with only one positional argument")
	}
}
```

Update `restore_test.go` imports: it now needs `bytes`, `encoding/hex`, `os`, `path/filepath`, `strings`, `testing`, `time`, `golang.org/x/sys/unix`, `github.com/draganm/amber-store/key`. Drop `chunkers`, `diskstore` (no longer used here). `TestRunRestore_RejectsBadKey` must run before the key is parsed, so it does not need a daemon — `parseHexKey` fails first.

- [ ] **Step 4: Build and run tests**

Run: `go build ./cmd/amber-store/ && go test ./cmd/amber-store/ -run 'TestRestore|TestRunRestore' -v`
Expected: PASS. Fix any orphaned-helper compile errors from Step 2 by deleting the dead functions they report.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/restore.go cmd/amber-store/restore_test.go cmd/amber-store/*.go
git commit -m "feat(cli): restore fetches a tar from the daemon and extracts it"
```

---

## Task 13: Remove `pack` and `castar`; finalize wiring; end-to-end test

**Files:**
- Modify: `cmd/amber-store/main.go` (final command set; remove pack)
- Modify: `cmd/amber-store/driver.go` (remove `pack()`; keep `driver`/`buildDir`/`buildFile`/`buildEntry`)
- Delete: `cmd/amber-store/main_test.go` (pack command tests), `cmd/amber-store/driver_test.go`'s pack tests, `castar/`
- Create: `cmd/amber-store/e2e_test.go`

- [ ] **Step 1: Remove `pack()` from `driver.go`**

Delete the `pack(...)` function (the first function, which builds the tar via `castar.NewSink`) and the `castar` import. Keep `driver`, `buildDir`, `buildFile`, `buildEntry` intact (still used by `ingestObjects`).

- [ ] **Step 2: Finalize `main.go`**

```go
func newApp() *cli.App {
	return &cli.App{
		Name:  "amber-store",
		Usage: "content-addressed filesystem tree store",
		Commands: []*cli.Command{
			daemonCommand(),
			ingestCommand(),
			loadCommand(),
			dumpCommand(),
			restoreCommand(),
		},
	}
}
```

Remove `packCommand()`, `packConfig`, and `runPack` (in `main.go`) and the now-unused imports there.

- [ ] **Step 3: Delete `castar/` and the pack tests**

```bash
git rm -r castar/
git rm cmd/amber-store/main_test.go
```

Edit `cmd/amber-store/driver_test.go`: delete `TestPack_SmallTreeRootIsLast`, `TestPack_DeduplicatesIdenticalFiles`, `TestPack_Deterministic`, `TestPack_FailFastOnUnreadableFile`, and the now-unused `readTar` helper (and its `archive/tar`, `bytes`, `io`, `encoding/hex` imports if nothing else uses them). If `driver_test.go` ends up empty, `git rm` it. Replace the fail-fast coverage with a sequential-build test so the behavior stays covered:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
)

func TestBuildDir_FailFastOnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "secret")
	if err := os.WriteFile(p, []byte("data"), 0o000); err != nil {
		t.Fatal(err)
	}
	d := &driver{ic: chunkers.NewItemChunker(7), xattrInlineMax: 256}
	emit := func(fstree.Object) error { return nil }
	if _, err := d.buildDir(dir, emit); err == nil {
		t.Errorf("expected buildDir to fail on an unreadable file")
	}
}
```

- [ ] **Step 4: Write the end-to-end test** `cmd/amber-store/e2e_test.go`

```go
package main

import (
	"path/filepath"
	"testing"
)

// TestEndToEnd_IngestStreamThenRestore runs the full path: a daemon owns a store,
// `ingest` streams a tree to it over the socket, and `restore` reconstructs the
// tree faithfully from a `dump`-equivalent tar.
func TestEndToEnd_IngestStreamThenRestore(t *testing.T) {
	src := buildSourceTree(t)
	sock := startDaemon(t)

	// Stream-ingest (no --output): the client builds and uploads to the daemon,
	// printing the root to its writer.
	var rootBuf bytesBuffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := trimSpace(rootBuf.String())

	out := filepath.Join(t.TempDir(), "restored")
	app = newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root, out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	compareTrees(t, src, out)
}
```

Replace `bytesBuffer`/`trimSpace` with the real `bytes.Buffer` and `strings.TrimSpace` and add the `bytes`/`strings` imports — they are written here as placeholders only to flag the two helpers you need; use the standard library directly.

- [ ] **Step 5: Full build, vet, and test**

Run:
```bash
go build ./... && go vet ./... && go test ./... -race
```
Expected: PASS across every package. Resolve any remaining dead-code/import errors (most likely orphaned helpers in `cmd/amber-store` from the restore refactor) by deleting them.

- [ ] **Step 6: Remove the spec's `pack` references if stale, and commit**

```bash
git add -A
git commit -m "feat: remove pack command and castar; end-to-end daemon test

Drops the pack command and the castar tar-of-chunks package. Adds an
end-to-end test exercising daemon + streaming ingest + restore."
```

---

## Self-Review

**Spec coverage:**
- Daemon owns store, HTTP/1.1 over unix socket, transport-agnostic handler → Task 9 (command), Task 7 (`daemon.New`).
- `POST /v1/objects`: verify each key in the write path, store via WriteParallel, JSON stats, 422 on bad pack/verification → Task 2 (verify), Task 7.
- `GET /v1/tar/{key}`: PAX tar, 404 missing root, 400 non-directory → Task 8.
- Pack-write format (magic, object tag, key, uvarint, payload, end marker), rootless, partial/parallel uploads → Task 1; split-upload exercised in Task 8.
- `diskstore` verify + Blob/XattrSet length check → Task 2.
- `tarexport` PAX with nsec mtime, xattrs, devices, fifo, socket-skip → Task 3.
- `tarextract` faithful metadata + deferred directory metadata + unsafe-name rejection → Task 4.
- Socket resolver (flag → env → XDG → TempDir/uid), macOS correctness, tests → Task 5.
- `client` Ingest/Tar with connection-refused hint → Task 6.
- CLI: `daemon`, `ingest` (+`--output`), `load`, `dump`, `restore`; client-side root printing; remove `pack`; remove `castar`; socket discovery → Tasks 9–13.
- Graceful shutdown, stale-socket removal, concurrent clients (handler is per-request, store is concurrency-safe) → Task 9.
- Progress bar stays client-side → Task 10.
- Tests: amberpack round-trip/truncation/bad-magic/bad-key (Task 1), verify (Task 2), tarexport (Task 3), tarextract (Task 4), resolver (Task 5), daemon httptest+socket+split upload (Tasks 7–8), CLI (Tasks 9–12), e2e over a real socket (Task 13).

**Placeholder scan:** Two intentional, clearly-flagged placeholders requiring action: the `_ = tarexport.Write` stub in Task 7 (replaced in Task 8) and the `bytesBuffer`/`trimSpace` markers in Task 13's e2e test (the step text instructs using `bytes.Buffer`/`strings.TrimSpace`). No "TBD"/"add error handling"/"similar to" placeholders.

**Type consistency:** `fstree.Object{Key,Bytes}` flows client → amberpack → daemon; the daemon adapts to `diskstore.Object{Key,Data}` at the storage boundary; `ingestObjects` retyped to `iter.Seq2[fstree.Object,error]` (Task 10) consistent with `amberpack.Writer.Add(fstree.Object)` (Task 1) and `writePack` (Task 10). `WriteParallel` returns `(WriteStats,error)` everywhere after Task 2 (test call sites updated). `parseHexKey` exists once in `dump.go` (Task 11) and is reused by `daemon` (its own copy, Task 8) and `restore.go` (Task 12). `tarexport.Getter`/`store.Get` and `tarextract.Extract(io.Reader,string)` signatures match their callers in `daemon`. `client.Stats` JSON tags match the daemon's `ingestResponse` tags.
