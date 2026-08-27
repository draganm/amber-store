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
		{2, 0, []byte{0x40}},               // bstr, len 0
		{2, 5, []byte{0x45}},               // bstr, len 5
		{2, 23, []byte{0x57}},              // bstr, len 23 (last 1-byte head)
		{2, 24, []byte{0x58, 0x18}},        // bstr, len 24 (needs 1 length byte)
		{2, 255, []byte{0x58, 0xff}},       // bstr, len 255
		{2, 256, []byte{0x59, 0x01, 0x00}}, // bstr, len 256
		{5, 2, []byte{0xa2}},               // map, 2 pairs
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
		"bb":           []byte("2"),
		"a":            []byte("1"),
		"user.selinux": []byte("x"),
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

func TestDecodeXattrs_RejectsOversizedCount(t *testing.T) {
	for _, body := range [][]byte{
		{0xba, 0xff, 0xff, 0xff, 0xff},
		{0xbb, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00},
		{0xb9, 0xff, 0xff, 0x40, 0x40},
	} {
		if _, err := DecodeXattrs(body); err == nil {
			t.Fatalf("%x: expected error", body)
		}
	}
}
