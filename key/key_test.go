package key

import (
	"bytes"
	"errors"
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
