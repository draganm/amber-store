# `key` package design

Date: 2026-06-08

## Purpose

Implement the 32-byte Amber-Store **lookup key** described in
[architecture/keys.md](../../../architecture/keys.md) and the CAS object types in
[architecture/types.md](../../../architecture/types.md). A lookup key identifies a
piece of content in the store by encoding its **type**, a **logical length**, and a
**truncated Blake3 hash** of its serialized bytes.

The package owns both the binary key format (encode / decode / inspect / validate)
and key construction from content (compute and truncate the Blake3 hash).

- Import path: `github.com/draganm/amber-store/key`
- Files: `key.go`, `type.go`, `errors.go`, plus `key_test.go`, `type_test.go`.

## Key format (recap of keys.md)

Every key is exactly **32 bytes**, three contiguous fields:

| Field          | Size            | Purpose                                   |
|----------------|-----------------|-------------------------------------------|
| Header byte    | 1 byte          | Payload type and length-field size        |
| Payload length | 1–8 bytes       | Big-endian byte length of the content     |
| Payload hash   | remaining bytes | Truncated Blake3 hash of serialized bytes |

Header byte, most- to least-significant bit:

- **bits 7–4 — type** (4 bits): the CAS object type.
- **bit 3 — reserved**: must always be `0`.
- **bits 2–0 — length size** (3 bits): `value + 1` = number of length bytes, so the
  length field is always 1–8 bytes.

Hash length is whatever space remains: `hashLen = 32 - 1 - lengthSize = 31 - lengthSize`
→ 23 bytes (8-byte length) to 30 bytes (1-byte length).

## Core types

```go
type Key [32]byte   // comparable → usable directly as a map key; value semantics, zero-alloc

type Type uint8     // the 4-bit CAS object type carried in header bits 7-4
const (
    Blob     Type = 0
    FileNode Type = 1
    DirLeaf  Type = 2
    DirNode  Type = 3
    XattrSet Type = 4
)

func (t Type) IsValid() bool   // true for 0..4; 5..15 are reserved and must not be emitted
func (t Type) String() string  // "Blob", "FileNode", … ; "Type(n)" for reserved/unknown
```

## Construction

```go
// New computes blake3.Sum256(serialized), truncates the digest to fill the key, and
// assembles the header + big-endian length field.
func New(t Type, length uint64, serialized []byte) (Key, error)

// NewFromHash assembles a key from a precomputed full 256-bit Blake3 digest,
// truncating it to the remaining hash space. Use when the caller already hashed
// (e.g. streaming) or wants to avoid re-hashing. New is implemented as
// hashing followed by NewFromHash.
func NewFromHash(t Type, length uint64, fullHash [32]byte) (Key, error)
```

Construction rules:

- **Length-size selection** is deterministic: the *minimum* number of bytes that hold
  `length` big-endian with no leading zero.
  - `length == 0` is the **special case** → length size 1, single `0x00` byte. The
    "no leading-zero" rule is read as "minimal encoding"; `0x00` is the minimal form
    of zero. This lets an empty `Blob`/`XattrSet` be represented.
  - otherwise `lengthSize = ceil(bits.Len64(length) / 8)`.
- **`length` is taken as given.** The package does NOT assume `length == len(serialized)`.
  `Blob`/`XattrSet` callers pass `len(serialized)`; aggregate callers
  (`FileNode`/`DirLeaf`/`DirNode`) pass a *logical* size (file content size or subtree
  footprint per types.md). The hash always covers `serialized`.
- **Truncation** = take the leading `hashLen` bytes of the 256-bit digest.
- Returns an error when `!t.IsValid()`.
- Construction always emits a **canonical** key.

## Parse / Validate / accessors

```go
func Parse(b []byte) (Key, error)   // len(b) must be 32; validates canonical form on the way in
func (k Key) Validate() error       // re-checks canonical form of an existing Key

// Accessors assume a valid key (one produced by New/NewFromHash/Parse). They decode
// bytes directly and do not return errors — they are on the read hot path.
func (k Key) Type() Type            // header bits 7-4
func (k Key) LengthSize() int       // (header & 0x07) + 1, range 1..8
func (k Key) Length() uint64        // big-endian decode of the length field
func (k Key) Hash() []byte          // the truncated hash bytes, len == 31 - LengthSize
func (k Key) String() string        // hex encoding, for logs and error messages
```

`Validate`/`Parse` enforce:

- total length is exactly 32 bytes (`Parse` only — `Key` is already fixed-size),
- reserved bit (bit 3 of header) is `0`,
- type is in `0..4` (reserved `5..15` rejected),
- length field is canonical: its first byte is non-zero **unless** the encoded value
  is `0` (the single-`0x00` zero case).

## Errors

Exported sentinels in `errors.go`, wrapped with context via `fmt.Errorf("%w: …")`:

- `ErrBadKeyLength` — input to `Parse` is not exactly 32 bytes.
- `ErrReservedBitSet` — header reserved bit is 1.
- `ErrReservedType` — type is in the reserved range `5..15`.
- `ErrNonCanonicalLength` — length field has leading-zero padding (non-minimal).

## Dependency

Blake3 via `github.com/zeebo/blake3` (`blake3.Sum256([]byte) [32]byte`). Truncation is a
leading-bytes slice of the 256-bit digest.

Rationale ("fastest practical"):

- On `arm64` (the dev machine) neither major Go lib has NEON assembly — both use a
  pure-Go fallback; `zeebo` is the faster fallback / better general-purpose pick and
  also gets AVX2/SSE4.1 on x86 deployments. `lukechampine` only pulls ahead on
  AVX-512 hardware for large inputs.
- A cgo binding to the official BLAKE3 C lib (NEON/AVX-512) has the highest raw
  throughput on large inputs but adds per-call overhead that hurts a store hashing
  many *small* objects (dir entries, index nodes) and breaks the pure-Go Nix/static
  build — net loss for this workload.
- Hashing is isolated behind `New`/`NewFromHash`, so the library is swappable with a
  one-line change if a benchmark on the real deployment target says otherwise.

## Testing (TDD)

- **Round-trip:** `New` / `NewFromHash` → `Type`/`Length`/`Hash`/`LengthSize` return the
  exact inputs; `Parse(k[:])` reproduces the same `Key`.
- **Length-size boundaries:** `0, 1, 255, 256, 65535, 65536, 2^24-1, 2^24, 2^32-1,
  2^32, 2^40, 2^48, 2^56-1, 2^56, 2^64-1` → assert the expected `LengthSize` and
  `hashLen`.
- **Canonical rejection:** `Parse`/`Validate` reject a set reserved bit, reserved types
  `5..15`, leading-zero-padded length fields, and (for `Parse`) wrong total length.
- **Zero-length special case:** empty `Blob` constructs and round-trips; `Length()==0`,
  `LengthSize()==1`.
- **`New` vs `NewFromHash`:** agree for the same content; a Blake3 known-answer vector
  pins the truncation so the on-disk format can't silently drift.
- **Determinism & map usability:** identical inputs → byte-identical keys; keys work as
  Go map keys and compare with `==`.

## Out of scope

- Filesystem entry types (`S_IFMT`) — those live in directory-entry metadata, not in the
  key, and belong to the fstree/directory package.
- CBOR serialization of objects, chunking, and tree building — separate packages.
