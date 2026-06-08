# fstree `pack` Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `amber-store pack DIR` command that traverses `DIR` depth-first, builds the content-addressed Merkle/prolly tree from [architecture/fstree.md](../../../architecture/fstree.md), and streams every CAS chunk into a tar (stdout or `-o FILE`), with the root object as the last member.

**Architecture:** Layered packages. `internal/cborx` (low-level canonical CBOR for bstr-keyed xattr maps), `fstree` (object encoders, length rules, and the streaming bottom-up tree builders), `chunkers` (ultracdc byte chunker + BLAKE3 item chunker), `castar` (dedup'ing tar sink), and `cmd/amber-store` (CLI + depth-first filesystem driver). The driver emits objects children-before-parents; a delayed-by-one sink writes each object via dedup `Put` and force-writes the final object (the root) via `PutRoot` so the root is the last tar member, written once.

**Tech Stack:** Go 1.26, `github.com/zeebo/blake3` (existing), `github.com/fxamacker/cbor/v2`, `github.com/PlakarKorp/go-cdc-chunkers`, `github.com/urfave/cli/v2`, `golang.org/x/sys/unix`.

**Reference spec:** [docs/superpowers/specs/2026-06-08-fstree-pack-design.md](../specs/2026-06-08-fstree-pack-design.md)

---

## Key design facts the implementer must keep in mind

- **Entry CBOR map** uses integer keys 0–9, declared in ascending key order in the struct so the encoding is canonical regardless of fxamacker's struct-field sorting. Required keys (0 name, 1 mode, 2 uid, 3 gid, 4 mtime) are **never** `omitempty` (uid/gid/mtime/mode are legitimately 0). Only optional payload keys 5–9 are `omitempty`.
- **`mode` is the raw POSIX `st_mode`** from `syscall.Stat_t.Mode` (it encodes both type and perms), NOT Go's `fs.FileMode`.
- **xattr maps need byte-string (bstr) keys**, which fxamacker cannot produce from a Go `map[string]...` (it emits text strings). We hand-roll a §4.2-canonical encoder in `internal/cborx` and embed the result as `cbor.RawMessage` (inline, key 8) or as the `XattrSet` object body (key 9 reference).
- **The root is emitted last.** Each builder emits an object only after all of its children, so the global top directory's root is the last object emitted. The driver verifies this (`pending.Key == rootKey`) and force-writes it via `PutRoot`.
- **ultracdc requires the blank import** `_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"` or `NewChunker("ultracdc", …)` returns an "unknown algorithm" error.
- **Copy each byte chunk** before retaining it: the chunker reuses its internal buffer across `Next()` calls.

## File map

| File | Responsibility |
|------|----------------|
| `internal/cborx/cborx.go` | `appendHead`, `appendBStr`, `EncodeXattrs` (canonical bstr-keyed CBOR map) |
| `internal/cborx/cborx_test.go` | KATs for the above |
| `fstree/object.go` | `Object`, `Emit` types |
| `fstree/encode.go` | `encMode`, `Entry`, `DirPair`, `EncodeBlob/FileNode/DirLeaf/DirNode/XattrSet` |
| `fstree/encode_test.go` | encoder KATs + length-field tests |
| `fstree/index_builder.go` | generic `IndexBuilder` (FileNode/DirNode promotion) |
| `fstree/index_builder_test.go` | promotion + root-detection tests |
| `fstree/dir_builder.go` | `DirBuilder` (entries → DirLeaves → dir index) |
| `fstree/dir_builder_test.go` | dir build tests |
| `chunkers/item.go` | `ItemChunker`, `NewItemChunker`, `IsBoundary` |
| `chunkers/item_test.go` | boundary determinism + bounds |
| `chunkers/byte.go` | `SplitBytes` (ultracdc wrapper, copies chunks) |
| `chunkers/byte_test.go` | streaming + copy-safety tests |
| `castar/sink.go` | `Sink`, `Put`, `PutRoot`, `Close` |
| `castar/sink_test.go` | dedup + root-last tests |
| `cmd/amber-store/meta.go` | `entryMeta`, device numbers, type detection (cross-platform) |
| `cmd/amber-store/xattr_linux.go` | `readXattrs` via `Llistxattr`/`Lgetxattr` |
| `cmd/amber-store/xattr_darwin.go` | `readXattrs` via `Listxattr`/`Getxattr` |
| `cmd/amber-store/xattr_common.go` | `splitXattrNames`, `readAllXattrs` |
| `cmd/amber-store/driver.go` | `buildFile`, `buildDir`, `buildEntry`, `pack` |
| `cmd/amber-store/main.go` | urfave/cli/v2 app + `pack` subcommand |
| `cmd/amber-store/pack_test.go` | end-to-end: member set, root-last, dedup, determinism, fail-fast, in-test decoder |

---

## Task 1: Add dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the modules**

Run:
```bash
cd /Users/dragan/draganm/amber-store
go get github.com/fxamacker/cbor/v2@latest
go get github.com/PlakarKorp/go-cdc-chunkers@latest
go get github.com/urfave/cli/v2@latest
go get golang.org/x/sys/unix@latest
```
Expected: `go.mod` gains the four requires; `go.sum` updated. No build yet (no code uses them).

- [ ] **Step 2: Verify the module graph resolves**

Run: `go mod verify`
Expected: `all modules verified`

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build: add cbor, go-cdc-chunkers, urfave/cli, x/sys deps"
```

---

## Task 2: `internal/cborx` — canonical bstr-keyed CBOR map

**Files:**
- Create: `internal/cborx/cborx.go`
- Test: `internal/cborx/cborx_test.go`

- [ ] **Step 1: Write the failing test**

```go
package cborx

import (
	"bytes"
	"testing"
)

func TestAppendHead_ShortestForm(t *testing.T) {
	cases := []struct {
		major byte
		n     uint64
		want  []byte
	}{
		{2, 0, []byte{0x40}},              // bstr, len 0
		{2, 5, []byte{0x45}},              // bstr, len 5
		{2, 23, []byte{0x57}},             // bstr, len 23 (last 1-byte head)
		{2, 24, []byte{0x58, 0x18}},       // bstr, len 24 (needs 1 length byte)
		{2, 255, []byte{0x58, 0xff}},      // bstr, len 255
		{2, 256, []byte{0x59, 0x01, 0x00}},// bstr, len 256
		{5, 2, []byte{0xa2}},              // map, 2 pairs
	}
	for _, c := range cases {
		got := appendHead(nil, c.major, c.n)
		if !bytes.Equal(got, c.want) {
			t.Errorf("appendHead(%d,%d) = %x, want %x", c.major, c.n, got, c.want)
		}
	}
}

func TestEncodeXattrs_CanonicalSorted(t *testing.T) {
	// Keys must sort by their bstr encoding: shorter-length keys first, then bytewise.
	m := map[string][]byte{
		"bb":            []byte("2"),
		"a":             []byte("1"),
		"user.selinux":  []byte("x"),
	}
	got := EncodeXattrs(m)
	// map(3) | bstr "a" -> bstr "1" | bstr "bb" -> bstr "2" | bstr "user.selinux" -> bstr "x"
	want := []byte{0xa3}
	want = append(want, appendBStr(nil, []byte("a"))...)
	want = append(want, appendBStr(nil, []byte("1"))...)
	want = append(want, appendBStr(nil, []byte("bb"))...)
	want = append(want, appendBStr(nil, []byte("2"))...)
	want = append(want, appendBStr(nil, []byte("user.selinux"))...)
	want = append(want, appendBStr(nil, []byte("x"))...)
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeXattrs = %x, want %x", got, want)
	}
}

func TestEncodeXattrs_Empty(t *testing.T) {
	got := EncodeXattrs(map[string][]byte{})
	if !bytes.Equal(got, []byte{0xa0}) { // empty map
		t.Errorf("EncodeXattrs(empty) = %x, want a0", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cborx/`
Expected: FAIL — `undefined: appendHead` / `EncodeXattrs`.

- [ ] **Step 3: Write the implementation**

```go
// Package cborx provides the minimal canonical-CBOR encoding the fstree needs
// for byte-string-keyed maps (extended attributes), which fxamacker cannot
// produce from a Go map[string]... value because it emits text-string keys.
// Output follows RFC 8949 section 4.2 core deterministic encoding.
package cborx

import (
	"bytes"
	"sort"
)

// appendHead appends a CBOR head (major type in the high 3 bits) carrying the
// argument n in the shortest form, per RFC 8949 section 4.2.
func appendHead(b []byte, major byte, n uint64) []byte {
	h := major << 5
	switch {
	case n < 24:
		return append(b, h|byte(n))
	case n < 1<<8:
		return append(b, h|24, byte(n))
	case n < 1<<16:
		return append(b, h|25, byte(n>>8), byte(n))
	case n < 1<<32:
		return append(b, h|26, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	default:
		return append(b, h|27,
			byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

// appendBStr appends s as a CBOR byte string (major type 2).
func appendBStr(b, s []byte) []byte {
	b = appendHead(b, 2, uint64(len(s)))
	return append(b, s...)
}

// EncodeXattrs encodes m as a canonical CBOR map with byte-string keys and
// values, keys sorted by the bytewise lexicographic order of their CBOR
// encodings (RFC 8949 section 4.2). Used both inline (DirLeaf key 8) and as the
// XattrSet object body (key 9 target).
func EncodeXattrs(m map[string][]byte) []byte {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool {
		return bytes.Compare(
			appendBStr(nil, []byte(names[i])),
			appendBStr(nil, []byte(names[j])),
		) < 0
	})
	out := appendHead(nil, 5, uint64(len(m)))
	for _, n := range names {
		out = appendBStr(out, []byte(n))
		out = appendBStr(out, m[n])
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/cborx/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cborx/
git commit -m "feat(cborx): canonical bstr-keyed CBOR map for xattrs"
```

---

## Task 3: `fstree` object + Emit types

**Files:**
- Create: `fstree/object.go`

- [ ] **Step 1: Write the file (no test — pure type decls used by later tasks)**

```go
// Package fstree encodes the Amber-Store filesystem tree objects (FileNode,
// DirLeaf, DirNode, XattrSet, Blob) as deterministic CBOR per
// architecture/fstree.md, and builds files and directories bottom-up by
// streaming. See architecture/types.md for the length-field semantics.
package fstree

import "github.com/draganm/amber-store/key"

// Object is a built CAS object: its key and its serialized bytes.
type Object struct {
	Key   key.Key
	Bytes []byte
}

// Emit is called once per built object, in children-before-parents order. The
// consumer (the pack driver) is responsible for writing the object to the sink.
type Emit func(Object) error
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./fstree/`
Expected: builds (no symbols used yet is fine).

- [ ] **Step 3: Commit**

```bash
git add fstree/object.go
git commit -m "feat(fstree): Object and Emit types"
```

---

## Task 4: `fstree` encoders + length rules

**Files:**
- Create: `fstree/encode.go`
- Test: `fstree/encode_test.go`

- [ ] **Step 1: Write the failing test**

```go
package fstree

import (
	"testing"

	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
)

func mustBlob(t *testing.T, data []byte) Object {
	t.Helper()
	o, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestEncodeBlob_LengthIsByteCount(t *testing.T) {
	o := mustBlob(t, []byte("hello"))
	if o.Key.Type() != key.Blob {
		t.Errorf("type = %v, want Blob", o.Key.Type())
	}
	if o.Key.Length() != 5 {
		t.Errorf("length = %d, want 5", o.Key.Length())
	}
}

func TestEncodeFileNode_LengthIsSumOfChildren(t *testing.T) {
	a := mustBlob(t, make([]byte, 100))
	b := mustBlob(t, make([]byte, 250))
	o, err := EncodeFileNode([]key.Key{a.Key, b.Key})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.FileNode {
		t.Errorf("type = %v, want FileNode", o.Key.Type())
	}
	if o.Key.Length() != 350 { // excludes the FileNode's own bytes
		t.Errorf("length = %d, want 350", o.Key.Length())
	}
}

func TestEncodeDirLeaf_LengthIsOwnBytesPlusContentKeys(t *testing.T) {
	child := mustBlob(t, make([]byte, 1000)) // a file with content size 1000
	e := Entry{
		Name:       []byte("f"),
		Mode:       0o100644,
		UID:        0,
		GID:        0,
		Mtime:      0,
		ContentKey: child.Key[:],
	}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.DirLeaf {
		t.Errorf("type = %v, want DirLeaf", o.Key.Type())
	}
	want := uint64(len(o.Bytes)) + 1000
	if o.Key.Length() != want {
		t.Errorf("length = %d, want %d (ownbytes %d + 1000)", o.Key.Length(), want, len(o.Bytes))
	}
}

func TestEncodeDirLeaf_SymlinkAddsOnlyOwnBytes(t *testing.T) {
	e := Entry{Name: []byte("l"), Mode: 0o120777, LinkTarget: []byte("target/path")}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Length() != uint64(len(o.Bytes)) {
		t.Errorf("length = %d, want own bytes %d", o.Key.Length(), len(o.Bytes))
	}
}

func TestEncodeDirNode_LengthIsOwnBytesPlusChildren(t *testing.T) {
	// Two DirLeaf children, each with a known length field.
	c1, _ := EncodeDirLeaf([]Entry{{Name: []byte("a"), Mode: 0o40755}})
	c2, _ := EncodeDirLeaf([]Entry{{Name: []byte("b"), Mode: 0o40755}})
	o, err := EncodeDirNode([]DirPair{
		{SepName: []byte("a"), ChildKey: c1.Key[:]},
		{SepName: []byte("b"), ChildKey: c2.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(len(o.Bytes)) + c1.Key.Length() + c2.Key.Length()
	if o.Key.Length() != want {
		t.Errorf("length = %d, want %d", o.Key.Length(), want)
	}
}

func TestEncodeXattrSet_LengthIsOwnBytes(t *testing.T) {
	m := map[string][]byte{"user.a": []byte("v")}
	o, err := EncodeXattrSet(m)
	if err != nil {
		t.Fatal(err)
	}
	if o.Key.Type() != key.XattrSet {
		t.Errorf("type = %v, want XattrSet", o.Key.Type())
	}
	if o.Key.Length() != uint64(len(o.Bytes)) {
		t.Errorf("length = %d, want %d", o.Key.Length(), len(o.Bytes))
	}
	if string(o.Bytes) != string(cborx.EncodeXattrs(m)) {
		t.Errorf("XattrSet body must equal EncodeXattrs output")
	}
}

func TestEncodeDirLeaf_InlineXattrsEmbeddedVerbatim(t *testing.T) {
	inline := cborx.EncodeXattrs(map[string][]byte{"user.x": []byte("y")})
	e := Entry{Name: []byte("f"), Mode: 0o100644, XattrsIn: inline}
	o, err := EncodeDirLeaf([]Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	// The inline xattr bytes must appear verbatim somewhere in the leaf encoding.
	if !contains(o.Bytes, inline) {
		t.Errorf("inline xattrs not embedded verbatim")
	}
}

func contains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
outer:
	for i := 0; i+len(needle) <= len(haystack); i++ {
		for j := range needle {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return true
	}
	return false
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/`
Expected: FAIL — `undefined: EncodeBlob`, `Entry`, etc.

- [ ] **Step 3: Write the implementation**

```go
package fstree

import (
	"fmt"

	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"github.com/fxamacker/cbor/v2"
)

// encMode is the RFC 8949 section 4.2 core-deterministic CBOR encoder shared by
// every object encoder. Built once at init. NilContainerAsEmpty makes a nil or
// empty slice encode as an empty array (0x80) rather than null (0xf6) — required
// so an empty directory's DirLeaf is the canonical empty array.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("fstree: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Entry is a single directory entry, encoded as a canonical CBOR map with
// integer keys 0-9 (architecture/fstree.md, DirLeaf). Fields are declared in
// ascending key order so the encoding is canonical. Required keys (0-4) are
// never omitempty because uid/gid/mtime/mode are legitimately zero. The payload
// keys 5-9 are mutually constrained by the caller per the entry's type.
type Entry struct {
	Name       []byte          `cbor:"0,keyasint"`
	Mode       uint64          `cbor:"1,keyasint"` // raw POSIX st_mode (type + perms)
	UID        uint64          `cbor:"2,keyasint"`
	GID        uint64          `cbor:"3,keyasint"`
	Mtime      int64           `cbor:"4,keyasint"` // ns since the Unix epoch (may be negative)
	ContentKey []byte          `cbor:"5,keyasint,omitempty"` // S_IFREG / S_IFDIR
	LinkTarget []byte          `cbor:"6,keyasint,omitempty"` // S_IFLNK
	Rdev       []uint64        `cbor:"7,keyasint,omitempty"` // S_IFCHR / S_IFBLK: [major, minor]
	XattrsIn   cbor.RawMessage `cbor:"8,keyasint,omitempty"` // inline xattrs (pre-encoded bstr map)
	XattrsKey  []byte          `cbor:"9,keyasint,omitempty"` // spilled XattrSet key
}

// DirPair is one [sepName, childKey] element of a DirNode, encoded as a 2-element
// CBOR array of byte strings.
type DirPair struct {
	_        struct{} `cbor:",toarray"`
	SepName  []byte
	ChildKey []byte
}

// EncodeBlob wraps raw file-content bytes as a Blob object (no CBOR framing).
func EncodeBlob(data []byte) (Object, error) {
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: data}, nil
}

// EncodeFileNode encodes a file index node: a CBOR array of child keys. Its
// length field is the sum of child content sizes (excludes its own bytes).
func EncodeFileNode(children []key.Key) (Object, error) {
	arr := make([][]byte, len(children))
	var sum uint64
	for i, c := range children {
		arr[i] = c[:]
		sum += c.Length()
	}
	b, err := encMode.Marshal(arr)
	if err != nil {
		return Object{}, err
	}
	k, err := key.New(key.FileNode, sum, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeDirLeaf encodes a run of directory entries (already sorted by name) as a
// CBOR array of entry maps. Its length field is its own serialized bytes plus
// the content-key length of each regular-file/directory entry plus the
// xattrs-key length of each entry whose xattrs were spilled to an XattrSet.
func EncodeDirLeaf(entries []Entry) (Object, error) {
	b, err := encMode.Marshal(entries)
	if err != nil {
		return Object{}, err
	}
	var sub uint64
	for _, e := range entries {
		if len(e.ContentKey) == key.Size {
			ck, err := key.Parse(e.ContentKey)
			if err != nil {
				return Object{}, fmt.Errorf("entry %q content key: %w", e.Name, err)
			}
			sub += ck.Length()
		}
		if len(e.XattrsKey) == key.Size {
			xk, err := key.Parse(e.XattrsKey)
			if err != nil {
				return Object{}, fmt.Errorf("entry %q xattrs key: %w", e.Name, err)
			}
			sub += xk.Length()
		}
	}
	k, err := key.New(key.DirLeaf, uint64(len(b))+sub, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeDirNode encodes a directory index node as a CBOR array of
// [sepName, childKey] pairs (sorted by sepName). Its length field is its own
// serialized bytes plus the cumulative length of every child.
func EncodeDirNode(pairs []DirPair) (Object, error) {
	b, err := encMode.Marshal(pairs)
	if err != nil {
		return Object{}, err
	}
	var sub uint64
	for _, p := range pairs {
		ck, err := key.Parse(p.ChildKey)
		if err != nil {
			return Object{}, fmt.Errorf("dir node child key: %w", err)
		}
		sub += ck.Length()
	}
	k, err := key.New(key.DirNode, uint64(len(b))+sub, b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}

// EncodeXattrSet encodes a spilled extended-attribute set. Its length field is
// its own serialized byte length.
func EncodeXattrSet(m map[string][]byte) (Object, error) {
	b := cborx.EncodeXattrs(m)
	k, err := key.New(key.XattrSet, uint64(len(b)), b)
	if err != nil {
		return Object{}, err
	}
	return Object{Key: k, Bytes: b}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fstree/`
Expected: PASS.

- [ ] **Step 5: Add a determinism KAT and re-run**

Append to `fstree/encode_test.go`:
```go
func TestEncoders_Deterministic(t *testing.T) {
	e := Entry{Name: []byte("z"), Mode: 0o100644, UID: 1000, GID: 1000, Mtime: -5}
	a, _ := EncodeDirLeaf([]Entry{e})
	b, _ := EncodeDirLeaf([]Entry{e})
	if a.Key != b.Key {
		t.Errorf("DirLeaf encoding not deterministic: %s vs %s", a.Key, b.Key)
	}
}
```
Run: `go test ./fstree/ -run Deterministic`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fstree/encode.go fstree/encode_test.go
git commit -m "feat(fstree): object encoders and length-field rules"
```

---

## Task 5: `chunkers.ItemChunker`

**Files:**
- Create: `chunkers/item.go`
- Test: `chunkers/item_test.go`

- [ ] **Step 1: Write the failing test**

```go
package chunkers

import "testing"

func TestNewItemChunker_DerivesBounds(t *testing.T) {
	c := NewItemChunker(7) // avg 128
	if c.MinRun != 32 || c.MaxRun != 512 {
		t.Errorf("bounds = min %d max %d, want 32/512", c.MinRun, c.MaxRun)
	}
}

func TestIsBoundary_BelowMinNeverBoundary(t *testing.T) {
	c := NewItemChunker(7)
	for runLen := 1; runLen < c.MinRun; runLen++ {
		if c.IsBoundary([]byte("anything"), runLen) {
			t.Fatalf("boundary at runLen %d below MinRun %d", runLen, c.MinRun)
		}
	}
}

func TestIsBoundary_AtMaxAlwaysBoundary(t *testing.T) {
	c := NewItemChunker(7)
	if !c.IsBoundary([]byte("x"), c.MaxRun) {
		t.Fatalf("no forced boundary at MaxRun %d", c.MaxRun)
	}
}

func TestIsBoundary_Deterministic(t *testing.T) {
	c := NewItemChunker(5)
	enc := []byte("some item encoding")
	if c.IsBoundary(enc, 100) != c.IsBoundary(enc, 100) {
		t.Fatal("IsBoundary not deterministic")
	}
}

func TestIsBoundary_HitsOnLowBitsZero(t *testing.T) {
	// With k=0 the mask is 0, so every item at or above MinRun is a boundary.
	c := NewItemChunker(0)
	if !c.IsBoundary([]byte("x"), c.MinRun) {
		t.Fatal("k=0 should make every item (>= MinRun) a boundary")
	}
	if !c.IsBoundary([]byte("anything else"), c.MinRun+5) {
		t.Fatal("k=0 should make every item (>= MinRun) a boundary")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./chunkers/ -run Item`
Expected: FAIL — `undefined: NewItemChunker`.

- [ ] **Step 3: Write the implementation**

```go
// Package chunkers provides the two content-defined chunkers the fstree uses:
// a byte chunker (ultracdc) for file content, and an item chunker for the
// index/entry streams (architecture/fstree.md, "Content-defined chunking").
package chunkers

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// ItemChunker decides chunk boundaries between whole items (a child key, a
// directory entry, or a [sepName, childKey] pair). A boundary ends the current
// run when the low k bits of BLAKE3(item encoding) are zero (average run 2^k),
// bounded by MinRun and MaxRun so an item is never split and variance is capped.
type ItemChunker struct {
	MinRun int
	MaxRun int
	mask   uint64
}

// NewItemChunker returns an item chunker with average run 2^bits and derived
// bounds MinRun = 2^bits / 4 (at least 2) and MaxRun = 2^bits * 4.
func NewItemChunker(bits int) ItemChunker {
	avg := 1 << bits
	minRun := avg / 4
	if minRun < 2 {
		minRun = 2
	}
	var mask uint64
	if bits > 0 {
		mask = uint64(1)<<bits - 1
	}
	return ItemChunker{MinRun: minRun, MaxRun: avg * 4, mask: mask}
}

// IsBoundary reports whether the run should end after the item whose canonical
// encoding is enc, given the current run length (including this item).
func (c ItemChunker) IsBoundary(enc []byte, runLen int) bool {
	if runLen >= c.MaxRun {
		return true
	}
	if runLen < c.MinRun {
		return false
	}
	sum := blake3.Sum256(enc)
	return binary.LittleEndian.Uint64(sum[:8])&c.mask == 0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./chunkers/ -run Item`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add chunkers/item.go chunkers/item_test.go
git commit -m "feat(chunkers): item chunker boundary oracle"
```

---

## Task 6: `chunkers.SplitBytes` (ultracdc wrapper)

**Files:**
- Create: `chunkers/byte.go`
- Test: `chunkers/byte_test.go`

- [ ] **Step 1: Write the failing test**

```go
package chunkers

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSplitBytes_ReassemblesInput(t *testing.T) {
	in := make([]byte, 5*1024*1024)
	if _, err := rand.Read(in); err != nil {
		t.Fatal(err)
	}
	var got []byte
	n := 0
	err := SplitBytes(bytes.NewReader(in), nil, func(chunk []byte) error {
		n++
		got = append(got, chunk...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, in) {
		t.Fatal("reassembled bytes differ from input")
	}
	if n < 2 {
		t.Fatalf("expected multiple chunks for 5 MiB, got %d", n)
	}
}

func TestSplitBytes_EmptyInputYieldsNoChunks(t *testing.T) {
	n := 0
	err := SplitBytes(bytes.NewReader(nil), nil, func([]byte) error { n++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("empty input must yield 0 chunks, got %d", n)
	}
}

func TestSplitBytes_ChunksAreCopiesSafeToRetain(t *testing.T) {
	in := make([]byte, 2*1024*1024)
	for i := range in {
		in[i] = byte(i)
	}
	var retained [][]byte
	err := SplitBytes(bytes.NewReader(in), nil, func(chunk []byte) error {
		retained = append(retained, chunk) // retain without copying
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, c := range retained {
		got = append(got, c...)
	}
	if !bytes.Equal(got, in) {
		t.Fatal("retained chunks were mutated by later Next() calls — SplitBytes must copy")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./chunkers/ -run SplitBytes`
Expected: FAIL — `undefined: SplitBytes`.

- [ ] **Step 3: Write the implementation**

```go
package chunkers

import (
	"io"

	gocdc "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc" // register "ultracdc"
)

// SplitBytes runs the ultracdc content-defined chunker over r and calls fn once
// per chunk, in order. Each chunk passed to fn is a fresh copy, so fn may retain
// it (the underlying chunker reuses its read buffer between calls). A nil opts
// uses ultracdc's default sizes (min 2 KiB, normal 10 KiB, max 64 KiB). An empty
// reader yields zero chunks.
func SplitBytes(r io.Reader, opts *gocdc.ChunkerOpts, fn func(chunk []byte) error) error {
	c, err := gocdc.NewChunker("ultracdc", r, opts)
	if err != nil {
		return err
	}
	for {
		chunk, err := c.Next()
		if len(chunk) > 0 {
			cp := make([]byte, len(chunk))
			copy(cp, chunk)
			if ferr := fn(cp); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// ByteOpts is re-exported so callers can build sizes without importing the
// upstream package directly.
type ByteOpts = gocdc.ChunkerOpts
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./chunkers/`
Expected: PASS (all chunker tests).

- [ ] **Step 5: Commit**

```bash
git add chunkers/byte.go chunkers/byte_test.go
git commit -m "feat(chunkers): ultracdc byte chunker wrapper with copy-safe chunks"
```

---

## Task 7: `fstree.IndexBuilder` (FileNode/DirNode promotion)

**Files:**
- Create: `fstree/index_builder.go`
- Test: `fstree/index_builder_test.go`

- [ ] **Step 1: Write the failing test**

```go
package fstree

import (
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

// collector records emitted objects and is the test's Emit.
type collector struct{ objs []Object }

func (c *collector) emit(o Object) error { c.objs = append(c.objs, o); return nil }

func blobKey(t *testing.T, n int) key.Key {
	t.Helper()
	o, err := EncodeBlob(make([]byte, n))
	if err != nil {
		t.Fatal(err)
	}
	return o.Key
}

func TestFileIndex_SingleChildIsRootNoNode(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(7))
	k := blobKey(t, 10)
	if err := ib.AddChild(c.emit, k, nil); err != nil {
		t.Fatal(err)
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root != k {
		t.Errorf("root = %s, want the single blob %s", root, k)
	}
	if len(c.objs) != 0 {
		t.Errorf("no FileNode should be emitted for a single child, got %d objects", len(c.objs))
	}
}

func TestFileIndex_MultipleChildrenProduceFileNodeRoot(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(7))
	var sum uint64
	for _, n := range []int{100, 200, 300} {
		k := blobKey(t, n)
		sum += k.Length()
		if err := ib.AddChild(c.emit, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.FileNode {
		t.Errorf("root type = %v, want FileNode", root.Type())
	}
	if root.Length() != sum {
		t.Errorf("root length = %d, want %d", root.Length(), sum)
	}
	// The root must be the LAST emitted object.
	if len(c.objs) == 0 || c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}

func TestFileIndex_ManyChildrenMultiLevel(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(2)) // tiny avg → forces multiple levels
	for i := 0; i < 2000; i++ {
		if err := ib.AddChild(c.emit, blobKey(t, i+1), nil); err != nil {
			t.Fatal(err)
		}
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.FileNode {
		t.Errorf("root type = %v, want FileNode", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/ -run FileIndex`
Expected: FAIL — `undefined: NewFileIndexBuilder`.

- [ ] **Step 3: Write the implementation**

```go
package fstree

import (
	"errors"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

// IndexBuilder builds the index levels above a leaf level by streaming child
// references and item-chunking them into FileNode (files) or DirNode (dirs)
// objects, level by level, until one object remains (the root). Objects are
// emitted children-before-parents; the root is emitted last. Memory is
// O(levels x MaxRun) — it never holds a whole level's bytes.
type IndexBuilder struct {
	ic     chunkers.ItemChunker
	isDir  bool
	levels []*idxLevel
}

type idxLevel struct {
	keys   []key.Key
	seps   [][]byte // populated only for directory indexes
	runLen int
}

// NewFileIndexBuilder builds FileNode levels over Blob/FileNode child keys.
func NewFileIndexBuilder(ic chunkers.ItemChunker) *IndexBuilder {
	return &IndexBuilder{ic: ic, isDir: false}
}

// newDirIndexBuilder builds DirNode levels over DirLeaf/DirNode child keys, each
// carrying the greatest entry name (sepName) in its subtree.
func newDirIndexBuilder(ic chunkers.ItemChunker) *IndexBuilder {
	return &IndexBuilder{ic: ic, isDir: true}
}

func (ib *IndexBuilder) ensure(l int) *idxLevel {
	for len(ib.levels) <= l {
		ib.levels = append(ib.levels, &idxLevel{})
	}
	return ib.levels[l]
}

// hasAny reports whether any child has been added (level 0 exists).
func (ib *IndexBuilder) hasAny() bool { return len(ib.levels) > 0 }

// AddChild adds one leaf-level child reference. sep is the child subtree's
// greatest entry name for directory indexes, and is ignored for file indexes.
func (ib *IndexBuilder) AddChild(emit Emit, childKey key.Key, sep []byte) error {
	return ib.add(emit, 0, childKey, sep)
}

func (ib *IndexBuilder) add(emit Emit, l int, ck key.Key, sep []byte) error {
	L := ib.ensure(l)
	L.keys = append(L.keys, ck)
	if ib.isDir {
		L.seps = append(L.seps, sep)
	}
	L.runLen++
	enc, err := ib.itemEnc(ck, sep)
	if err != nil {
		return err
	}
	if ib.ic.IsBoundary(enc, L.runLen) {
		return ib.closeLevel(emit, l)
	}
	return nil
}

func (ib *IndexBuilder) itemEnc(ck key.Key, sep []byte) ([]byte, error) {
	if !ib.isDir {
		return ck[:], nil
	}
	return encMode.Marshal(DirPair{SepName: sep, ChildKey: ck[:]})
}

func (ib *IndexBuilder) closeLevel(emit Emit, l int) error {
	obj, sep, err := ib.buildNode(ib.levels[l])
	if err != nil {
		return err
	}
	ib.reset(l)
	if err := emit(obj); err != nil {
		return err
	}
	return ib.add(emit, l+1, obj.Key, sep)
}

func (ib *IndexBuilder) reset(l int) {
	L := ib.levels[l]
	L.keys = nil
	L.seps = nil
	L.runLen = 0
}

// buildNode builds the index object for a level's current run, returning the
// object and the run's greatest sepName (nil for file indexes).
func (ib *IndexBuilder) buildNode(L *idxLevel) (Object, []byte, error) {
	if ib.isDir {
		pairs := make([]DirPair, len(L.keys))
		for i := range L.keys {
			pairs[i] = DirPair{SepName: L.seps[i], ChildKey: L.keys[i][:]}
		}
		obj, err := EncodeDirNode(pairs)
		return obj, L.seps[len(L.seps)-1], err
	}
	obj, err := EncodeFileNode(L.keys)
	return obj, nil, err
}

// Finish collapses all open runs bottom-up and returns the root key. The single
// object that reaches the top with no parent is the root; a single child is
// returned without wrapping it in a degenerate one-child node.
func (ib *IndexBuilder) Finish(emit Emit) (key.Key, error) {
	for l := 0; l < len(ib.levels); l++ {
		L := ib.levels[l]
		isTop := l == len(ib.levels)-1
		switch {
		case L.runLen == 0:
			continue
		case L.runLen == 1:
			ck := L.keys[0]
			var sep []byte
			if ib.isDir {
				sep = L.seps[0]
			}
			ib.reset(l)
			if isTop {
				return ck, nil // already emitted when created
			}
			if err := ib.add(emit, l+1, ck, sep); err != nil {
				return key.Key{}, err
			}
		default:
			obj, sep, err := ib.buildNode(L)
			if err != nil {
				return key.Key{}, err
			}
			ib.reset(l)
			if err := emit(obj); err != nil {
				return key.Key{}, err
			}
			if err := ib.add(emit, l+1, obj.Key, sep); err != nil {
				return key.Key{}, err
			}
		}
	}
	return key.Key{}, errors.New("fstree: IndexBuilder.Finish with no children")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fstree/ -run FileIndex`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fstree/index_builder.go fstree/index_builder_test.go
git commit -m "feat(fstree): streaming index builder for FileNode/DirNode levels"
```

---

## Task 8: `fstree.DirBuilder`

**Files:**
- Create: `fstree/dir_builder.go`
- Test: `fstree/dir_builder_test.go`

- [ ] **Step 1: Write the failing test**

```go
package fstree

import (
	"fmt"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

func TestDir_EmptyDirIsSingleEmptyLeaf(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(7))
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirLeaf {
		t.Errorf("empty dir root type = %v, want DirLeaf", root.Type())
	}
	if len(c.objs) != 1 || c.objs[0].Key != root {
		t.Errorf("empty dir must emit exactly one DirLeaf that is the root")
	}
	// Empty DirLeaf is the CBOR empty array 0x80.
	if len(c.objs[0].Bytes) != 1 || c.objs[0].Bytes[0] != 0x80 {
		t.Errorf("empty DirLeaf bytes = %x, want 80", c.objs[0].Bytes)
	}
}

func TestDir_SingleEntryIsLeafRoot(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(7))
	if err := db.AddEntry(c.emit, Entry{Name: []byte("a"), Mode: 0o100644}); err != nil {
		t.Fatal(err)
	}
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirLeaf {
		t.Errorf("root type = %v, want DirLeaf", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}

func TestDir_ManyEntriesProduceDirNodeRootLast(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(2)) // tiny avg → multi-level
	for i := 0; i < 5000; i++ {
		name := []byte(fmt.Sprintf("%06d", i)) // already sorted
		if err := db.AddEntry(c.emit, Entry{Name: name, Mode: 0o100644}); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirNode {
		t.Errorf("root type = %v, want DirNode", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
	// du-style length must be positive and >= own bytes of the root.
	if root.Length() == 0 {
		t.Errorf("DirNode root length should be non-zero")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/ -run Dir`
Expected: FAIL — `undefined: NewDirBuilder`.

- [ ] **Step 3: Write the implementation**

```go
package fstree

import (
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

// DirBuilder builds one directory's prolly tree by streaming its entries (which
// the caller supplies already sorted bytewise by name). It chunks entries into
// DirLeaf objects and promotes their keys through a DirNode index. Objects are
// emitted children-before-parents; the directory root is emitted last.
type DirBuilder struct {
	ic      chunkers.ItemChunker
	idx     *IndexBuilder
	leaf    []Entry
	leafMax []byte
	runLen  int
}

// NewDirBuilder returns a DirBuilder using ic for both the entry stream and the
// DirNode index stream.
func NewDirBuilder(ic chunkers.ItemChunker) *DirBuilder {
	return &DirBuilder{ic: ic, idx: newDirIndexBuilder(ic)}
}

// AddEntry appends one directory entry (in sorted order).
func (db *DirBuilder) AddEntry(emit Emit, e Entry) error {
	enc, err := encMode.Marshal(e)
	if err != nil {
		return err
	}
	db.leaf = append(db.leaf, e)
	db.leafMax = e.Name
	db.runLen++
	if db.ic.IsBoundary(enc, db.runLen) {
		return db.closeLeaf(emit)
	}
	return nil
}

func (db *DirBuilder) closeLeaf(emit Emit) error {
	obj, err := EncodeDirLeaf(db.leaf)
	if err != nil {
		return err
	}
	sep := db.leafMax
	db.leaf = nil
	db.leafMax = nil
	db.runLen = 0
	if err := emit(obj); err != nil {
		return err
	}
	return db.idx.AddChild(emit, obj.Key, sep)
}

// Finish closes the trailing leaf run (emitting an empty DirLeaf for an empty
// directory) and returns the directory's root key.
func (db *DirBuilder) Finish(emit Emit) (key.Key, error) {
	if db.runLen > 0 || !db.idx.hasAny() {
		if err := db.closeLeaf(emit); err != nil {
			return key.Key{}, err
		}
	}
	return db.idx.Finish(emit)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fstree/`
Expected: PASS (all fstree tests).

- [ ] **Step 5: Commit**

```bash
git add fstree/dir_builder.go fstree/dir_builder_test.go
git commit -m "feat(fstree): streaming directory builder (entries to prolly tree)"
```

---

## Task 9: `castar` dedup tar sink

**Files:**
- Create: `castar/sink.go`
- Test: `castar/sink_test.go`

- [ ] **Step 1: Write the failing test**

```go
package castar

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	"github.com/draganm/amber-store/key"
)

func mkKey(t *testing.T, tp key.Type, n int) key.Key {
	t.Helper()
	k, err := key.New(tp, uint64(n), []byte{byte(n)})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func readNames(t *testing.T, b []byte) []string {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(b))
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	return names
}

func TestSink_DedupsAndKeepsRootLast(t *testing.T) {
	var buf bytes.Buffer
	s := NewSink(&buf)
	a := mkKey(t, key.Blob, 1)
	b := mkKey(t, key.Blob, 2)
	root := mkKey(t, key.DirLeaf, 3)
	if err := s.Put(a, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(b, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(a, []byte("a")); err != nil { // duplicate, must be skipped
		t.Fatal(err)
	}
	if err := s.PutRoot(root, []byte("r")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	names := readNames(t, buf.Bytes())
	want := []string{a.String(), b.String(), root.String()}
	if len(names) != len(want) {
		t.Fatalf("members = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("member %d = %s, want %s", i, names[i], want[i])
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./castar/`
Expected: FAIL — `undefined: NewSink`.

- [ ] **Step 3: Write the implementation**

```go
// Package castar writes content-addressed objects into a tar archive. Each
// object becomes one member named by the hex of its key. Put deduplicates
// (each key written once); PutRoot writes unconditionally so the root object is
// the final member.
package castar

import (
	"archive/tar"
	"io"
	"time"

	"github.com/draganm/amber-store/key"
)

// Sink writes objects to a tar archive with deduplication.
type Sink struct {
	tw   *tar.Writer
	seen map[key.Key]struct{}
}

// NewSink returns a Sink writing tar members to w.
func NewSink(w io.Writer) *Sink {
	return &Sink{tw: tar.NewWriter(w), seen: make(map[key.Key]struct{})}
}

// Put writes the object unless its key has already been written.
func (s *Sink) Put(k key.Key, data []byte) error {
	if _, ok := s.seen[k]; ok {
		return nil
	}
	s.seen[k] = struct{}{}
	return s.write(k, data)
}

// PutRoot writes the object unconditionally (bypassing dedup) so it is the last
// member of the archive.
func (s *Sink) PutRoot(k key.Key, data []byte) error {
	return s.write(k, data)
}

func (s *Sink) write(k key.Key, data []byte) error {
	h := &tar.Header{
		Name:     k.String(),
		Mode:     0o644,
		Size:     int64(len(data)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0).UTC(),
		Format:   tar.FormatUSTAR,
	}
	if err := s.tw.WriteHeader(h); err != nil {
		return err
	}
	_, err := s.tw.Write(data)
	return err
}

// Close flushes and closes the tar archive.
func (s *Sink) Close() error { return s.tw.Close() }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./castar/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add castar/sink.go castar/sink_test.go
git commit -m "feat(castar): dedup tar sink with root-last PutRoot"
```

---

## Task 10: `cmd/amber-store` metadata reading (cross-platform)

**Files:**
- Create: `cmd/amber-store/meta.go`
- Create: `cmd/amber-store/xattr_common.go`
- Create: `cmd/amber-store/xattr_linux.go`
- Create: `cmd/amber-store/xattr_darwin.go`
- Test: `cmd/amber-store/meta_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestEntryMeta_RegularFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("hi"), 0o640); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	m := entryMeta(info)
	if m.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Errorf("type bits = %o, want S_IFREG", m.Mode&unix.S_IFMT)
	}
	if m.Mode&0o777 != 0o640 {
		t.Errorf("perm bits = %o, want 640", m.Mode&0o777)
	}
}

func TestReadXattrs_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(p, "user.greeting", []byte("hello"), 0); err != nil {
		t.Skipf("xattrs unsupported on this filesystem: %v", err)
	}
	m, err := readXattrs(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(m["user.greeting"]) != "hello" {
		t.Errorf("xattr value = %q, want hello", m["user.greeting"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/`
Expected: FAIL — `undefined: entryMeta`, `readXattrs`.

- [ ] **Step 3: Write `meta.go`**

```go
package main

import (
	"io/fs"
	"syscall"

	"golang.org/x/sys/unix"
)

// meta is the POSIX metadata pulled from an lstat result.
type meta struct {
	Mode  uint64 // raw st_mode (type + perms)
	UID   uint64
	GID   uint64
	Mtime int64 // ns since the Unix epoch
}

// entryMeta extracts the raw POSIX metadata from an lstat FileInfo.
func entryMeta(info fs.FileInfo) meta {
	sys := info.Sys().(*syscall.Stat_t)
	return meta{
		Mode:  uint64(sys.Mode),
		UID:   uint64(sys.Uid),
		GID:   uint64(sys.Gid),
		Mtime: info.ModTime().UnixNano(),
	}
}

// deviceNumbers returns the major/minor device numbers for a device-node
// lstat FileInfo.
func deviceNumbers(info fs.FileInfo) (uint64, uint64) {
	sys := info.Sys().(*syscall.Stat_t)
	rdev := uint64(sys.Rdev)
	return uint64(unix.Major(rdev)), uint64(unix.Minor(rdev))
}
```

- [ ] **Step 4: Write `xattr_common.go`**

```go
package main

import "bytes"

// splitXattrNames splits a NUL-separated xattr name list into names, dropping
// empty entries.
func splitXattrNames(buf []byte) []string {
	var names []string
	for _, n := range bytes.Split(buf, []byte{0}) {
		if len(n) > 0 {
			names = append(names, string(n))
		}
	}
	return names
}

// readAllXattrs fetches each named attribute's value using get, returning a map.
// get has the signature of unix.Lgetxattr / unix.Getxattr: (path, attr, dest).
func readAllXattrs(path string, nameBuf []byte, get func(string, string, []byte) (int, error)) (map[string][]byte, error) {
	names := splitXattrNames(nameBuf)
	if len(names) == 0 {
		return nil, nil
	}
	m := make(map[string][]byte, len(names))
	for _, name := range names {
		sz, err := get(path, name, nil)
		if err != nil {
			return nil, err
		}
		val := make([]byte, sz)
		sz, err = get(path, name, val)
		if err != nil {
			return nil, err
		}
		m[name] = val[:sz]
	}
	return m, nil
}
```

- [ ] **Step 5: Write `xattr_linux.go`**

```go
//go:build linux

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes without following
// symlinks. Called only for non-symlink entries.
func readXattrs(path string) (map[string][]byte, error) {
	sz, err := unix.Llistxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = unix.Llistxattr(path, buf)
	if err != nil {
		return nil, err
	}
	return readAllXattrs(path, buf[:sz], unix.Lgetxattr)
}
```

- [ ] **Step 6: Write `xattr_darwin.go`**

```go
//go:build darwin

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes. Called only for
// non-symlink entries, so the follow-symlink behavior of the plain calls does
// not matter.
func readXattrs(path string) (map[string][]byte, error) {
	sz, err := unix.Listxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	return readAllXattrs(path, buf[:sz], unix.Getxattr)
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `go test ./cmd/amber-store/`
Expected: PASS (xattr round-trip may `t.Skip` on filesystems without xattr support; that is acceptable).

- [ ] **Step 8: Commit**

```bash
git add cmd/amber-store/meta.go cmd/amber-store/xattr_common.go cmd/amber-store/xattr_linux.go cmd/amber-store/xattr_darwin.go cmd/amber-store/meta_test.go
git commit -m "feat(cmd): cross-platform metadata and xattr reading"
```

---

## Task 11: `cmd/amber-store` driver (buildFile, buildEntry, buildDir, pack)

**Files:**
- Create: `cmd/amber-store/driver.go`
- Test: `cmd/amber-store/driver_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

// readTar returns a map from member name (hex key) to its bytes, plus the
// ordered list of names.
func readTar(t *testing.T, b []byte) (map[string][]byte, []string) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(b))
	members := map[string][]byte{}
	var order []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		members[h.Name] = data
		order = append(order, h.Name)
	}
	return members, order
}

func TestPack_SmallTreeRootIsLast(t *testing.T) {
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

	var buf bytes.Buffer
	ic := chunkers.NewItemChunker(7)
	root, err := pack(dir, &buf, ic, nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	members, order := readTar(t, buf.Bytes())
	if order[len(order)-1] != root.String() {
		t.Errorf("last member = %s, want root %s", order[len(order)-1], root)
	}
	if _, ok := members[root.String()]; !ok {
		t.Errorf("root member missing")
	}
	if root.Type() != key.DirLeaf && root.Type() != key.DirNode {
		t.Errorf("root type = %v, want a directory object", root.Type())
	}
}

func TestPack_DeduplicatesIdenticalFiles(t *testing.T) {
	dir := t.TempDir()
	content := bytes.Repeat([]byte("x"), 100)
	if err := os.WriteFile(filepath.Join(dir, "one"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "two"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256); err != nil {
		t.Fatal(err)
	}
	members, _ := readTar(t, buf.Bytes())
	// Both files share one Blob; count Blob-type members.
	blobs := 0
	for name := range members {
		raw, err := hex.DecodeString(name)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if k.Type() == key.Blob {
			blobs++
		}
	}
	if blobs != 1 {
		t.Errorf("blob members = %d, want 1 (deduplicated)", blobs)
	}
}

func TestPack_Deterministic(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	var b1, b2 bytes.Buffer
	r1, err := pack(dir, &b1, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := pack(dir, &b2, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Errorf("root differs across runs: %s vs %s", r1, r2)
	}
	if !bytes.Equal(b1.Bytes(), b2.Bytes()) {
		t.Errorf("tar bytes differ across runs of identical input")
	}
}

func TestPack_FailFastOnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "secret")
	if err := os.WriteFile(p, []byte("data"), 0o000); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	_, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err == nil {
		t.Errorf("expected pack to fail on an unreadable file")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run Pack`
Expected: FAIL — `undefined: pack`.

- [ ] **Step 3: Write the implementation**

```go
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/draganm/amber-store/castar"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// pack builds the content-addressed tree for the directory at root, writing
// every unique chunk to w as a tar and the root object as the final member. It
// returns the root key. Any I/O or encoding error aborts (fail fast).
func pack(root string, w io.Writer, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, error) {
	sink := castar.NewSink(w)
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}

	var pending *fstree.Object
	emit := func(o fstree.Object) error {
		if pending != nil {
			if err := sink.Put(pending.Key, pending.Bytes); err != nil {
				return err
			}
		}
		cp := o
		pending = &cp
		return nil
	}

	rootKey, err := d.buildDir(root, emit)
	if err != nil {
		return key.Key{}, err
	}
	if pending == nil {
		return key.Key{}, fmt.Errorf("pack: no objects emitted")
	}
	if pending.Key != rootKey {
		return key.Key{}, fmt.Errorf("pack: internal error, last emitted %s != root %s", pending.Key, rootKey)
	}
	if err := sink.PutRoot(pending.Key, pending.Bytes); err != nil {
		return key.Key{}, err
	}
	if err := sink.Close(); err != nil {
		return key.Key{}, err
	}
	return rootKey, nil
}

type driver struct {
	ic             chunkers.ItemChunker
	byteOpts       *chunkers.ByteOpts
	xattrInlineMax int
}

// buildDir builds the directory at path and returns its root key, emitting every
// object in its subtree (children before parents).
func (d *driver) buildDir(path string, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	db := fstree.NewDirBuilder(d.ic)
	for _, de := range ents {
		full := filepath.Join(path, de.Name())
		e, err := d.buildEntry(full, de.Name(), emit)
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

// buildEntry produces the directory entry for one path, recursing into files and
// subdirectories (and emitting their objects) and reading inline metadata for
// links and special files.
func (d *driver) buildEntry(full, name string, emit fstree.Emit) (fstree.Entry, error) {
	info, err := os.Lstat(full)
	if err != nil {
		return fstree.Entry{}, err
	}
	m := entryMeta(info)
	e := fstree.Entry{
		Name:  []byte(name),
		Mode:  m.Mode,
		UID:   m.UID,
		GID:   m.GID,
		Mtime: m.Mtime,
	}

	switch m.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		ck, err := d.buildFile(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFDIR:
		ck, err := d.buildDir(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFLNK:
		target, err := os.Readlink(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.LinkTarget = []byte(target)
	case unix.S_IFCHR, unix.S_IFBLK:
		major, minor := deviceNumbers(info)
		e.Rdev = []uint64{major, minor}
	case unix.S_IFIFO, unix.S_IFSOCK:
		// no payload key
	default:
		return fstree.Entry{}, fmt.Errorf("%s: unsupported file type %#o", full, m.Mode&unix.S_IFMT)
	}

	// Extended attributes (skip symlinks).
	if m.Mode&unix.S_IFMT != unix.S_IFLNK {
		xattrs, err := readXattrs(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		if len(xattrs) > 0 {
			enc := cborx.EncodeXattrs(xattrs)
			if len(enc) <= d.xattrInlineMax {
				e.XattrsIn = enc
			} else {
				obj, err := fstree.EncodeXattrSet(xattrs)
				if err != nil {
					return fstree.Entry{}, err
				}
				if err := emit(obj); err != nil {
					return fstree.Entry{}, err
				}
				e.XattrsKey = obj.Key[:]
			}
		}
	}
	return e, nil
}

// buildFile chunks a regular file into Blobs (emitting each), builds its FileNode
// index, and returns the file's root key.
func (d *driver) buildFile(full string, emit fstree.Emit) (key.Key, error) {
	f, err := os.Open(full)
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()

	ib := fstree.NewFileIndexBuilder(d.ic)
	saw := false
	err = chunkers.SplitBytes(f, d.byteOpts, func(chunk []byte) error {
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
	if err != nil {
		return key.Key{}, err
	}
	if !saw {
		obj, err := fstree.EncodeBlob([]byte{})
		if err != nil {
			return key.Key{}, err
		}
		if err := emit(obj); err != nil {
			return key.Key{}, err
		}
		if err := ib.AddChild(emit, obj.Key, nil); err != nil {
			return key.Key{}, err
		}
	}
	return ib.Finish(emit)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run Pack`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/driver.go cmd/amber-store/driver_test.go
git commit -m "feat(cmd): depth-first pack driver streaming chunks to a sink"
```

---

## Task 12: CLI wiring (urfave/cli/v2)

**Files:**
- Create: `cmd/amber-store/main.go`
- Test: `cmd/amber-store/main_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunPack_WritesOutputFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.tar")
	app := newApp()
	err := app.Run([]string{"amber-store", "pack", "-o", out, dir})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Errorf("output tar is empty")
	}
}

func TestRunPack_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	if err := app.Run([]string{"amber-store", "pack", f}); err == nil {
		t.Errorf("expected error packing a non-directory")
	}
}

func TestRunPack_RequiresExactlyOneArg(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "pack"}); err == nil {
		t.Errorf("expected error with no DIR argument")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run RunPack`
Expected: FAIL — `undefined: newApp`.

- [ ] **Step 3: Write the implementation**

```go
package main

import (
	"fmt"
	"os"

	"github.com/draganm/amber-store/chunkers"
	"github.com/urfave/cli/v2"
)

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "amber-store:", err)
		os.Exit(1)
	}
}

func newApp() *cli.App {
	return &cli.App{
		Name:  "amber-store",
		Usage: "content-addressed filesystem tree store",
		Commands: []*cli.Command{
			{
				Name:      "pack",
				Usage:     "build the content-addressed tree for DIR and write chunks as a tar",
				ArgsUsage: "DIR",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Usage: "output tar file (default: stdout)"},
					&cli.IntFlag{Name: "min", Usage: "ultracdc minimum chunk size in bytes"},
					&cli.IntFlag{Name: "avg", Usage: "ultracdc average (normal) chunk size in bytes"},
					&cli.IntFlag{Name: "max", Usage: "ultracdc maximum chunk size in bytes"},
					&cli.IntFlag{Name: "item-bits", Value: 7, Usage: "item chunker average run = 2^bits"},
					&cli.IntFlag{Name: "xattr-inline-max", Value: 256, Usage: "xattrs larger than this many bytes spill to an XattrSet"},
				},
				Action: runPack,
			},
		},
	}
}

func runPack(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("pack requires exactly one DIR argument, got %d", c.NArg())
	}
	dir := c.Args().First()
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	var byteOpts *chunkers.ByteOpts
	if c.Int("min") > 0 || c.Int("avg") > 0 || c.Int("max") > 0 {
		if c.Int("min") <= 0 || c.Int("avg") <= 0 || c.Int("max") <= 0 {
			return fmt.Errorf("--min, --avg and --max must all be set together")
		}
		byteOpts = &chunkers.ByteOpts{
			MinSize:    c.Int("min"),
			NormalSize: c.Int("avg"),
			MaxSize:    c.Int("max"),
		}
	}

	var out *os.File
	if path := c.String("output"); path != "" {
		out, err = os.Create(path)
		if err != nil {
			return err
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	ic := chunkers.NewItemChunker(c.Int("item-bits"))
	root, err := pack(dir, out, ic, byteOpts, c.Int("xattr-inline-max"))
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, root.String())
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/`
Expected: PASS (all cmd tests).

- [ ] **Step 5: Build the binary and smoke-test, then remove it**

Run:
```bash
go build -o /tmp/amber-store ./cmd/amber-store
/tmp/amber-store pack . > /tmp/self.tar
tar tf /tmp/self.tar | tail -1   # the root key (last member)
rm -f /tmp/amber-store /tmp/self.tar
```
Expected: a tar is produced; `tar tf` lists 64-hex-char member names; the command prints the root key to stderr. (Per user global instruction, the generated binary is removed.)

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/main.go cmd/amber-store/main_test.go
git commit -m "feat(cmd): urfave/cli pack command with output and chunking flags"
```

---

## Task 13: End-to-end structural verification (in-test decoder)

**Files:**
- Create: `cmd/amber-store/decode_test.go`

This task adds a minimal in-test decoder that walks the tree from the root and confirms the structure round-trips: file contents reassemble, directory entries are present with correct metadata, and a symlink is preserved. No shippable reader is added.

- [ ] **Step 1: Write the test**

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
	"github.com/fxamacker/cbor/v2"
	"golang.org/x/sys/unix"
)

// store is the decoded tar: key hex -> object bytes.
type store map[string][]byte

func loadStore(t *testing.T, tarBytes []byte) store {
	t.Helper()
	members, _ := readTar(t, tarBytes)
	return store(members)
}

// fileContent reassembles a file's bytes from its content key.
func (s store) fileContent(t *testing.T, k key.Key) []byte {
	t.Helper()
	data := s[k.String()]
	switch k.Type() {
	case key.Blob:
		return data
	case key.FileNode:
		var childRaw [][]byte
		if err := cbor.Unmarshal(data, &childRaw); err != nil {
			t.Fatal(err)
		}
		var out []byte
		for _, cr := range childRaw {
			ck, err := key.Parse(cr)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, s.fileContent(t, ck)...)
		}
		return out
	default:
		t.Fatalf("not a file content key: %v", k.Type())
		return nil
	}
}

// entryMap is a decoded DirLeaf entry.
type entryMap struct {
	Name       []byte `cbor:"0,keyasint"`
	Mode       uint64 `cbor:"1,keyasint"`
	ContentKey []byte `cbor:"5,keyasint,omitempty"`
	LinkTarget []byte `cbor:"6,keyasint,omitempty"`
}

// listDir collects all entries under a directory content key, descending
// DirNodes and DirLeaves.
func (s store) listDir(t *testing.T, k key.Key) []entryMap {
	t.Helper()
	data := s[k.String()]
	switch k.Type() {
	case key.DirLeaf:
		var ents []entryMap
		if err := cbor.Unmarshal(data, &ents); err != nil {
			t.Fatal(err)
		}
		return ents
	case key.DirNode:
		var pairs [][]cbor.RawMessage
		if err := cbor.Unmarshal(data, &pairs); err != nil {
			t.Fatal(err)
		}
		var out []entryMap
		for _, p := range pairs {
			var childKeyBytes []byte
			if err := cbor.Unmarshal(p[1], &childKeyBytes); err != nil {
				t.Fatal(err)
			}
			ck, err := key.Parse(childKeyBytes)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, s.listDir(t, ck)...)
		}
		return out
	default:
		t.Fatalf("not a directory key: %v", k.Type())
		return nil
	}
}

func TestEndToEnd_StructureRoundTrips(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("0123456789"), 200000) // ~2 MiB -> multi-chunk file
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "small.txt"), []byte("tiny"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("big.bin", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	root, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	s := loadStore(t, buf.Bytes())

	ents := s.listDir(t, root)
	byName := map[string]entryMap{}
	for _, e := range ents {
		byName[string(e.Name)] = e
	}

	if _, ok := byName["big.bin"]; !ok {
		t.Fatal("big.bin missing")
	}
	bigKey, err := key.Parse(byName["big.bin"].ContentKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.fileContent(t, bigKey); !bytes.Equal(got, big) {
		t.Errorf("big.bin content mismatch: got %d bytes, want %d", len(got), len(big))
	}
	if bigKey.Length() != uint64(len(big)) {
		t.Errorf("big.bin content key length = %d, want %d", bigKey.Length(), len(big))
	}

	small := byName["small.txt"]
	if small.Mode&0o777 != 0o600 {
		t.Errorf("small.txt perms = %o, want 600", small.Mode&0o777)
	}

	link := byName["link"]
	if link.Mode&unix.S_IFMT != unix.S_IFLNK {
		t.Errorf("link type = %o, want symlink", link.Mode&unix.S_IFMT)
	}
	if string(link.LinkTarget) != "big.bin" {
		t.Errorf("link target = %q, want big.bin", link.LinkTarget)
	}
}
```

- [ ] **Step 2: Run the test**

Run: `go test ./cmd/amber-store/ -run EndToEnd`
Expected: PASS.

- [ ] **Step 3: Run the full suite**

Run: `go test ./...`
Expected: PASS across all packages.

- [ ] **Step 4: Commit**

```bash
git add cmd/amber-store/decode_test.go
git commit -m "test(cmd): end-to-end structural round-trip via in-test decoder"
```

---

## Task 14: Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a usage section to README.md**

Add under the existing content (after the "Architecture" section), matching the README's existing tone:

```markdown
## Packing a directory

The `amber-store pack` command walks a directory depth-first, builds the
content-addressed tree, and writes every unique chunk into a tar (the root
object is the last member):

```sh
go run ./cmd/amber-store pack ./some/dir > tree.tar      # tar to stdout
go run ./cmd/amber-store pack -o tree.tar ./some/dir     # tar to a file
```

The root key (hex) is printed to stderr. Chunking is tunable with
`--min/--avg/--max` (ultracdc byte chunking) and `--item-bits` (index/entry
chunking); `--xattr-inline-max` controls when extended attributes spill to an
`XattrSet` object.
```

- [ ] **Step 2: Verify it builds and vets clean**

Run: `go build ./... && go vet ./...`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document the amber-store pack command"
```

---

## Final verification

- [ ] Run the complete suite: `go test ./...` → all PASS.
- [ ] `go vet ./...` → clean.
- [ ] Confirm no stray binaries remain: `git status` is clean and no `amber-store` binary is tracked.
- [ ] The branch `feat/fstree-pack` holds the full feature, ready for review/PR.
