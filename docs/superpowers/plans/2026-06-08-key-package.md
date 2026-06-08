# `key` Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the 32-byte Amber-Store lookup key — type + logical length + truncated Blake3 hash — as a standalone `key` package.

**Architecture:** A `Key` is a `[32]byte` value (comparable, usable as a map key). The high nibble of byte 0 holds the 4-bit CAS object `Type`; bit 3 is reserved (0); the low 3 bits hold `lengthSize-1`. Bytes `1..1+lengthSize` hold the big-endian length; the remaining bytes hold the leading bytes of a BLAKE3-256 digest. Construction (`New`/`NewFromHash`) always emits canonical keys; `Parse`/`Validate` reject non-canonical input; accessors assume a valid key and never error.

**Tech Stack:** Go 1.26, `github.com/zeebo/blake3` for hashing, standard `testing`.

**Reference:** [architecture/keys.md](../../../architecture/keys.md), [architecture/types.md](../../../architecture/types.md), spec [docs/superpowers/specs/2026-06-08-key-package-design.md](../specs/2026-06-08-key-package-design.md).

**Note (Nix):** Adding the `zeebo/blake3` dependency only requires `go mod tidy` for `go test`/`go build`. If the repo is later packaged with `buildGoModule`, its `vendorHash` will need updating — out of scope for this plan.

---

### Task 1: `Type` enum

**Files:**
- Create: `key/type.go`
- Test: `key/type_test.go`

- [ ] **Step 1: Write the failing tests**

Create `key/type_test.go`:

```go
package key

import "testing"

func TestType_IsValid(t *testing.T) {
	for ty := Type(0); ty <= 4; ty++ {
		if !ty.IsValid() {
			t.Errorf("Type(%d) should be valid", uint8(ty))
		}
	}
	for _, ty := range []Type{5, 6, 15, 16, 255} {
		if ty.IsValid() {
			t.Errorf("Type(%d) should be invalid", uint8(ty))
		}
	}
}

func TestType_String(t *testing.T) {
	cases := map[Type]string{
		Blob:     "Blob",
		FileNode: "FileNode",
		DirLeaf:  "DirLeaf",
		DirNode:  "DirNode",
		XattrSet: "XattrSet",
		Type(7):  "Type(7)",
	}
	for ty, want := range cases {
		if got := ty.String(); got != want {
			t.Errorf("Type(%d).String() = %q, want %q", uint8(ty), got, want)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./key/ -run TestType -v`
Expected: build failure — `undefined: Type`, `undefined: Blob`, etc.

- [ ] **Step 3: Write the implementation**

Create `key/type.go`:

```go
// Package key implements the 32-byte Amber-Store lookup key: a content address
// that encodes a CAS object type, a logical payload length, and a truncated
// BLAKE3 hash of the payload's serialized bytes. See architecture/keys.md.
package key

import "fmt"

// Type is the 4-bit CAS object type carried in the high nibble of a key's
// header byte (architecture/types.md).
type Type uint8

const (
	Blob     Type = 0 // raw file-content byte chunk (a CDC leaf)
	FileNode Type = 1 // file chunk-index node
	DirLeaf  Type = 2 // a contiguous run of directory entries
	DirNode  Type = 3 // directory index node
	XattrSet Type = 4 // spilled extended attributes
)

// IsValid reports whether t is a defined CAS object type (0..4). Types 5..15 are
// reserved and must not be emitted; values above 15 do not fit the 4-bit field.
func (t Type) IsValid() bool {
	return t <= XattrSet
}

// String returns the type name, or "Type(n)" for reserved/unknown values.
func (t Type) String() string {
	switch t {
	case Blob:
		return "Blob"
	case FileNode:
		return "FileNode"
	case DirLeaf:
		return "DirLeaf"
	case DirNode:
		return "DirNode"
	case XattrSet:
		return "XattrSet"
	default:
		return fmt.Sprintf("Type(%d)", uint8(t))
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./key/ -run TestType -v`
Expected: PASS (`TestType_IsValid`, `TestType_String`).

- [ ] **Step 5: Commit**

```bash
git add key/type.go key/type_test.go
git commit -m "feat(key): add CAS object Type enum"
```

---

### Task 2: `Key` type and accessors

Accessors are tested first against hand-built keys (known bytes → known fields), so no constructor is needed yet.

**Files:**
- Create: `key/key.go`
- Test: `key/key_test.go`

- [ ] **Step 1: Write the failing tests**

Create `key/key_test.go`:

```go
package key

import (
	"bytes"
	"testing"
)

func TestAccessors_SingleByteLength(t *testing.T) {
	// Blob, length 255 (lengthSize 1), 30-byte hash.
	var k Key
	k[0] = 0x00 // type 0, reserved 0, lengthSize-1 = 0
	k[1] = 0xFF // length = 255
	for i := 2; i < Size; i++ {
		k[i] = byte(i)
	}
	if k.Type() != Blob {
		t.Errorf("Type() = %v, want Blob", k.Type())
	}
	if k.LengthSize() != 1 {
		t.Errorf("LengthSize() = %d, want 1", k.LengthSize())
	}
	if k.Length() != 255 {
		t.Errorf("Length() = %d, want 255", k.Length())
	}
	if len(k.Hash()) != 30 {
		t.Errorf("len(Hash()) = %d, want 30", len(k.Hash()))
	}
	if !bytes.Equal(k.Hash(), k[2:]) {
		t.Errorf("Hash() = %x, want %x", k.Hash(), k[2:])
	}
}

func TestAccessors_MultiByteLength(t *testing.T) {
	// FileNode, length 65536 (lengthSize 3): header = (1<<4) | (3-1) = 0x12.
	var k Key
	k[0] = 0x12
	k[1], k[2], k[3] = 0x01, 0x00, 0x00 // 0x010000 = 65536
	if k.Type() != FileNode {
		t.Errorf("Type() = %v, want FileNode", k.Type())
	}
	if k.LengthSize() != 3 {
		t.Errorf("LengthSize() = %d, want 3", k.LengthSize())
	}
	if k.Length() != 65536 {
		t.Errorf("Length() = %d, want 65536", k.Length())
	}
	if len(k.Hash()) != 28 {
		t.Errorf("len(Hash()) = %d, want 28", len(k.Hash()))
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./key/ -run TestAccessors -v`
Expected: build failure — `undefined: Key`, `undefined: Size`.

- [ ] **Step 3: Write the implementation**

Create `key/key.go`:

```go
package key

import "encoding/binary"

// Size is the fixed byte length of every key.
const Size = 32

// Key is a 32-byte lookup key. It is a value type and is directly comparable,
// so it can be used as a Go map key. Accessors assume the key is canonical
// (produced by New, NewFromHash, or Parse).
type Key [Size]byte

// Type returns the CAS object type from the header's high nibble.
func (k Key) Type() Type {
	return Type(k[0] >> 4)
}

// LengthSize returns the number of bytes the payload-length field occupies (1..8).
func (k Key) LengthSize() int {
	return int(k[0]&0x07) + 1
}

// Length decodes the big-endian payload-length field.
func (k Key) Length() uint64 {
	ls := k.LengthSize()
	var buf [8]byte
	copy(buf[8-ls:], k[1:1+ls])
	return binary.BigEndian.Uint64(buf[:])
}

// Hash returns the truncated payload hash bytes (len == Size-1-LengthSize).
func (k Key) Hash() []byte {
	return k[1+k.LengthSize():]
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./key/ -run TestAccessors -v`
Expected: PASS (`TestAccessors_SingleByteLength`, `TestAccessors_MultiByteLength`).

- [ ] **Step 5: Commit**

```bash
git add key/key.go key/key_test.go
git commit -m "feat(key): add Key type and field accessors"
```

---

### Task 3: `NewFromHash` construction + length-size selection + errors

**Files:**
- Modify: `key/key.go` (add `lengthSizeFor`, `NewFromHash`)
- Create: `key/errors.go`
- Test: `key/key_test.go` (add tests)

- [ ] **Step 1: Write the failing tests**

Append to `key/key_test.go` (add `"errors"` to its import block):

```go
func TestNewFromHash_RoundTrip(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i + 1)
	}
	k, err := NewFromHash(DirNode, 1000, full)
	if err != nil {
		t.Fatal(err)
	}
	if k.Type() != DirNode {
		t.Errorf("Type() = %v, want DirNode", k.Type())
	}
	if k.Length() != 1000 {
		t.Errorf("Length() = %d, want 1000", k.Length())
	}
	if k.LengthSize() != 2 {
		t.Errorf("LengthSize() = %d, want 2", k.LengthSize())
	}
	if !bytes.Equal(k.Hash(), full[:Size-1-2]) {
		t.Errorf("Hash() truncation mismatch")
	}
}

func TestNewFromHash_LengthSizeBoundaries(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i)
	}
	cases := []struct {
		length uint64
		wantLS int
	}{
		{0, 1}, {1, 1}, {255, 1}, {256, 2}, {65535, 2}, {65536, 3},
		{1<<24 - 1, 3}, {1 << 24, 4}, {1<<32 - 1, 4}, {1 << 32, 5},
		{1 << 40, 6}, {1 << 48, 7}, {1<<56 - 1, 7}, {1 << 56, 8},
		{1<<64 - 1, 8},
	}
	for _, c := range cases {
		k, err := NewFromHash(Blob, c.length, full)
		if err != nil {
			t.Fatalf("length %d: %v", c.length, err)
		}
		if k.LengthSize() != c.wantLS {
			t.Errorf("length %d: LengthSize() = %d, want %d", c.length, k.LengthSize(), c.wantLS)
		}
		if k.Length() != c.length {
			t.Errorf("length %d: Length() = %d", c.length, k.Length())
		}
		wantHashLen := Size - 1 - c.wantLS
		if len(k.Hash()) != wantHashLen {
			t.Errorf("length %d: len(Hash()) = %d, want %d", c.length, len(k.Hash()), wantHashLen)
		}
		if !bytes.Equal(k.Hash(), full[:wantHashLen]) {
			t.Errorf("length %d: hash truncation mismatch", c.length)
		}
	}
}

func TestNewFromHash_ReservedType(t *testing.T) {
	var full [32]byte
	for _, ty := range []Type{5, 15, 16, 255} {
		if _, err := NewFromHash(ty, 1, full); !errors.Is(err, ErrReservedType) {
			t.Errorf("Type(%d): err = %v, want ErrReservedType", uint8(ty), err)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./key/ -run TestNewFromHash -v`
Expected: build failure — `undefined: NewFromHash`, `undefined: ErrReservedType`.

- [ ] **Step 3: Write the implementation**

Create `key/errors.go`:

```go
package key

import "errors"

// Sentinel errors returned by Parse, Validate, and the constructors. Wrap them
// with %w and match with errors.Is.
var (
	// ErrBadKeyLength means the input to Parse was not exactly Size bytes.
	ErrBadKeyLength = errors.New("key: data is not 32 bytes")
	// ErrReservedBitSet means the header's reserved bit (bit 3) is set.
	ErrReservedBitSet = errors.New("key: reserved header bit is set")
	// ErrReservedType means the object type is reserved (5..15) or out of range.
	ErrReservedType = errors.New("key: reserved object type")
	// ErrNonCanonicalLength means the length field has leading-zero padding.
	ErrNonCanonicalLength = errors.New("key: non-canonical length encoding")
)
```

Add to `key/key.go` — extend the import block to `import ("encoding/binary"; "fmt"; "math/bits")` and add:

```go
// lengthSizeFor returns the minimum number of bytes needed to hold length
// big-endian with no leading zero. Zero is the special case: a single 0x00 byte.
func lengthSizeFor(length uint64) int {
	if length == 0 {
		return 1
	}
	return (bits.Len64(length) + 7) / 8
}

// NewFromHash assembles a canonical key from a CAS object type, a logical
// payload length, and a precomputed full 256-bit BLAKE3 digest. The digest is
// truncated to its leading bytes to fill the key. length is used verbatim: for
// Blob/XattrSet it is the serialized byte length; for FileNode/DirLeaf/DirNode
// it is the logical size (see architecture/types.md). Returns ErrReservedType
// if t is not a defined type.
func NewFromHash(t Type, length uint64, fullHash [Size]byte) (Key, error) {
	if !t.IsValid() {
		return Key{}, fmt.Errorf("%w: %d", ErrReservedType, uint8(t))
	}
	ls := lengthSizeFor(length)
	var k Key
	k[0] = byte(t)<<4 | byte(ls-1)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], length)
	copy(k[1:1+ls], buf[8-ls:])
	copy(k[1+ls:], fullHash[:Size-1-ls])
	return k, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./key/ -run TestNewFromHash -v`
Expected: PASS (`TestNewFromHash_RoundTrip`, `TestNewFromHash_LengthSizeBoundaries`, `TestNewFromHash_ReservedType`).

- [ ] **Step 5: Commit**

```bash
git add key/key.go key/errors.go key/key_test.go
git commit -m "feat(key): add NewFromHash and canonical length encoding"
```

---

### Task 4: `New` (BLAKE3 hashing) + known-answer test

**Files:**
- Modify: `key/key.go` (add `New`)
- Modify: `go.mod`, `go.sum` (add `github.com/zeebo/blake3`)
- Test: `key/key_test.go` (add KAT test)

- [ ] **Step 1: Write the failing test**

Append to `key/key_test.go` (add `"encoding/hex"` and `"github.com/zeebo/blake3"` to its import block):

```go
func TestNew_KnownAnswerAndTruncation(t *testing.T) {
	// Official BLAKE3-256 hash of the empty input.
	const wantHex = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	full := blake3.Sum256(nil)
	if got := hex.EncodeToString(full[:]); got != wantHex {
		t.Fatalf("blake3.Sum256(nil) = %s, want %s", got, wantHex)
	}
	// New(Blob, 0, nil): empty blob, lengthSize 1, hashLen 30.
	k, err := New(Blob, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if k.Length() != 0 || k.LengthSize() != 1 {
		t.Errorf("Length=%d LengthSize=%d, want 0 and 1", k.Length(), k.LengthSize())
	}
	if !bytes.Equal(k.Hash(), full[:30]) {
		t.Errorf("New hash truncation = %x, want %x", k.Hash(), full[:30])
	}
	// New must equal NewFromHash on the same content's digest.
	k2, _ := NewFromHash(Blob, 0, full)
	if k != k2 {
		t.Errorf("New != NewFromHash for the same content")
	}
}

func TestKey_DeterministicAndComparable(t *testing.T) {
	content := []byte("amber-store determinism check")
	a, _ := New(FileNode, uint64(len(content)), content)
	b, _ := New(FileNode, uint64(len(content)), content)
	if a != b {
		t.Fatal("New is not deterministic for identical inputs")
	}
	// Keys must be usable as Go map keys.
	m := map[Key]int{a: 1}
	if m[b] != 1 {
		t.Errorf("equal keys do not resolve to the same map entry")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./key/ -run "TestNew_KnownAnswer|TestKey_Deterministic" -v`
Expected: build failure — `undefined: New` and unresolved import `github.com/zeebo/blake3`.

- [ ] **Step 3: Write the implementation and resolve the dependency**

Add to `key/key.go` — add `"github.com/zeebo/blake3"` to the import block and add:

```go
// New computes the BLAKE3-256 digest of serialized, then assembles a canonical
// key via NewFromHash. length is the logical payload length and is taken as
// given (it need not equal len(serialized) — see NewFromHash).
func New(t Type, length uint64, serialized []byte) (Key, error) {
	return NewFromHash(t, length, blake3.Sum256(serialized))
}
```

Then fetch the dependency:

Run: `go mod tidy`
Expected: `go.mod` gains `require github.com/zeebo/blake3 vX.Y.Z` and `go.sum` is populated.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./key/ -run "TestNew_KnownAnswer|TestKey_Deterministic" -v`
Expected: PASS (`TestNew_KnownAnswerAndTruncation`, `TestKey_DeterministicAndComparable`).

- [ ] **Step 5: Commit**

```bash
git add key/key.go key/key_test.go go.mod go.sum
git commit -m "feat(key): add New with BLAKE3 hashing"
```

---

### Task 5: `Parse` and `Validate`

**Files:**
- Modify: `key/key.go` (add `Validate`, `Parse`)
- Test: `key/key_test.go` (add tests)

- [ ] **Step 1: Write the failing tests**

Append to `key/key_test.go`:

```go
func TestParse_RoundTrip(t *testing.T) {
	var full [32]byte
	for i := range full {
		full[i] = byte(i + 1)
	}
	for _, ty := range []Type{Blob, FileNode, DirLeaf, DirNode, XattrSet} {
		k, _ := NewFromHash(ty, 12345, full)
		got, err := Parse(k[:])
		if err != nil {
			t.Fatalf("%v: Parse: %v", ty, err)
		}
		if got != k {
			t.Errorf("%v: round-trip mismatch", ty)
		}
	}
}

func TestParse_BadLength(t *testing.T) {
	for _, n := range []int{0, 31, 33, 64} {
		if _, err := Parse(make([]byte, n)); !errors.Is(err, ErrBadKeyLength) {
			t.Errorf("len %d: err = %v, want ErrBadKeyLength", n, err)
		}
	}
}

func TestValidate_ReservedBit(t *testing.T) {
	var full [32]byte
	k, _ := NewFromHash(Blob, 1, full)
	k[0] |= 0x08 // set the reserved bit
	if err := k.Validate(); !errors.Is(err, ErrReservedBitSet) {
		t.Errorf("err = %v, want ErrReservedBitSet", err)
	}
}

func TestValidate_ReservedType(t *testing.T) {
	var k Key
	k[0] = 5 << 4 // type 5, lengthSize 1
	k[1] = 0x01
	if err := k.Validate(); !errors.Is(err, ErrReservedType) {
		t.Errorf("err = %v, want ErrReservedType", err)
	}
}

func TestValidate_NonCanonicalLength(t *testing.T) {
	// Blob, lengthSize 2 (header low bits = 1), length bytes 0x00 0x05:
	// leading zero with a non-zero value -> non-canonical.
	var k Key
	k[0] = 0x01
	k[1], k[2] = 0x00, 0x05
	if err := k.Validate(); !errors.Is(err, ErrNonCanonicalLength) {
		t.Errorf("err = %v, want ErrNonCanonicalLength", err)
	}
}

func TestValidate_ZeroLengthIsCanonical(t *testing.T) {
	// Blob, lengthSize 1, length byte 0x00: value 0 is the allowed special case.
	var k Key // all zero bytes
	if err := k.Validate(); err != nil {
		t.Errorf("zero-length key should validate, got %v", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./key/ -run "TestParse|TestValidate" -v`
Expected: build failure — `undefined: Parse`, `undefined: Validate`.

- [ ] **Step 3: Write the implementation**

Add to `key/key.go`:

```go
// Validate reports whether k is canonical: the reserved bit is clear, the type
// is defined (0..4), and the length field is minimally encoded (its first byte
// is non-zero, except for the single 0x00 byte that encodes a zero length).
func (k Key) Validate() error {
	if k[0]&0x08 != 0 {
		return ErrReservedBitSet
	}
	if !k.Type().IsValid() {
		return fmt.Errorf("%w: %d", ErrReservedType, uint8(k.Type()))
	}
	if k[1] == 0 && !(k.LengthSize() == 1 && k.Length() == 0) {
		return ErrNonCanonicalLength
	}
	return nil
}

// Parse copies b into a Key and validates its canonical form. b must be exactly
// Size bytes.
func Parse(b []byte) (Key, error) {
	if len(b) != Size {
		return Key{}, fmt.Errorf("%w: got %d", ErrBadKeyLength, len(b))
	}
	var k Key
	copy(k[:], b)
	if err := k.Validate(); err != nil {
		return Key{}, err
	}
	return k, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./key/ -run "TestParse|TestValidate" -v`
Expected: PASS (`TestParse_RoundTrip`, `TestParse_BadLength`, `TestValidate_ReservedBit`, `TestValidate_ReservedType`, `TestValidate_NonCanonicalLength`, `TestValidate_ZeroLengthIsCanonical`).

- [ ] **Step 5: Commit**

```bash
git add key/key.go key/key_test.go
git commit -m "feat(key): add Parse and Validate for canonical-form checks"
```

---

### Task 6: `String` (hex)

**Files:**
- Modify: `key/key.go` (add `String`)
- Test: `key/key_test.go` (add test)

- [ ] **Step 1: Write the failing test**

Append to `key/key_test.go`:

```go
func TestString_Hex(t *testing.T) {
	var k Key
	k[0] = 0x12
	k[31] = 0xFF
	got := k.String()
	if want := hex.EncodeToString(k[:]); got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}
	if len(got) != 2*Size {
		t.Errorf("len(String()) = %d, want %d", len(got), 2*Size)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./key/ -run TestString_Hex -v`
Expected: build failure — `k.String undefined (type Key has no field or method String)`.

- [ ] **Step 3: Write the implementation**

Add to `key/key.go` — add `"encoding/hex"` to the import block and add:

```go
// String returns the lowercase hex encoding of the key, for logs and errors.
func (k Key) String() string {
	return hex.EncodeToString(k[:])
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./key/ -run TestString_Hex -v`
Expected: PASS (`TestString_Hex`).

- [ ] **Step 5: Run the full package suite and vet**

Run: `go test ./key/ -v && go vet ./key/`
Expected: all tests PASS; `go vet` reports nothing.

- [ ] **Step 6: Commit**

```bash
git add key/key.go key/key_test.go
git commit -m "feat(key): add hex String method"
```

---

## Final verification

- [ ] Run: `go test ./... && go vet ./...`
  Expected: all packages build and pass; vet clean.
- [ ] Run: `gofmt -l key/`
  Expected: no output (all files formatted).
