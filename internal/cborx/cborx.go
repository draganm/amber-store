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
