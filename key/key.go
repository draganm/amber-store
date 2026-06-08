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
