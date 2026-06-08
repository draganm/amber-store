// Package cborx provides the minimal canonical-CBOR encoding the fstree needs
// for byte-string-keyed maps (extended attributes), which fxamacker cannot
// produce from a Go map[string]... value because it emits text-string keys.
// Output follows RFC 8949 section 4.2 core deterministic encoding.
package cborx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

// DecodeXattrs parses a canonical CBOR map of byte-string keys to byte-string
// values, the inverse of EncodeXattrs. It accepts only the definite-length
// forms this package emits and rejects any trailing bytes.
func DecodeXattrs(b []byte) (map[string][]byte, error) {
	major, n, rest, err := readHead(b)
	if err != nil {
		return nil, err
	}
	if major != 5 {
		return nil, fmt.Errorf("cborx: expected CBOR map (major 5), got major %d", major)
	}
	b = rest
	m := make(map[string][]byte, n)
	for i := range n {
		var name, val []byte
		if name, b, err = readBStr(b); err != nil {
			return nil, fmt.Errorf("cborx: xattr key %d: %w", i, err)
		}
		if val, b, err = readBStr(b); err != nil {
			return nil, fmt.Errorf("cborx: xattr value for %q: %w", name, err)
		}
		m[string(name)] = val
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("cborx: %d trailing bytes after xattr map", len(b))
	}
	return m, nil
}

// readHead reads one CBOR head, returning its major type, argument, and the
// remaining bytes. Only the shortest-form arguments emitted by appendHead are
// supported.
func readHead(b []byte) (major byte, arg uint64, rest []byte, err error) {
	if len(b) == 0 {
		return 0, 0, nil, io.ErrUnexpectedEOF
	}
	major = b[0] >> 5
	ai := b[0] & 0x1f
	b = b[1:]
	switch {
	case ai < 24:
		return major, uint64(ai), b, nil
	case ai == 24:
		if len(b) < 1 {
			return 0, 0, nil, io.ErrUnexpectedEOF
		}
		return major, uint64(b[0]), b[1:], nil
	case ai == 25:
		if len(b) < 2 {
			return 0, 0, nil, io.ErrUnexpectedEOF
		}
		return major, uint64(binary.BigEndian.Uint16(b)), b[2:], nil
	case ai == 26:
		if len(b) < 4 {
			return 0, 0, nil, io.ErrUnexpectedEOF
		}
		return major, uint64(binary.BigEndian.Uint32(b)), b[4:], nil
	case ai == 27:
		if len(b) < 8 {
			return 0, 0, nil, io.ErrUnexpectedEOF
		}
		return major, binary.BigEndian.Uint64(b), b[8:], nil
	default:
		return 0, 0, nil, fmt.Errorf("cborx: unsupported additional info %d", ai)
	}
}

// readBStr reads one CBOR byte string, returning a copy of its bytes and the
// remaining input.
func readBStr(b []byte) (val, rest []byte, err error) {
	major, n, b, err := readHead(b)
	if err != nil {
		return nil, nil, err
	}
	if major != 2 {
		return nil, nil, fmt.Errorf("cborx: expected byte string (major 2), got major %d", major)
	}
	if uint64(len(b)) < n {
		return nil, nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, b[:n])
	return out, b[n:], nil
}
