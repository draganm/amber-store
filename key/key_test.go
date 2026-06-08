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
