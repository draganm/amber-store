package packstore

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/draganm/amber-store/key"
)

// blobObj builds a canonical Blob object for data.
func blobObj(t *testing.T, data []byte) Object {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	return Object{Key: k, Data: data}
}

// incompressible returns n deterministic pseudo-random bytes (zstd cannot shrink them).
func incompressible(n int) []byte {
	r := rand.New(rand.NewPCG(42, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

// compressible returns n highly repetitive bytes (zstd shrinks them a lot).
func compressible(n int) []byte {
	return bytes.Repeat([]byte("abcdefgh"), n/8+1)[:n]
}

func TestRecordRoundTripRaw(t *testing.T) {
	obj := blobObj(t, incompressible(4096))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.key != obj.Key {
		t.Fatalf("key mismatch: %s != %s", r.key, obj.Key)
	}
	if r.flags != 0 {
		t.Fatalf("random data must be stored raw, got flags %#x", r.flags)
	}
	if r.ulen != r.slen || int(r.slen) != len(obj.Data) {
		t.Fatalf("raw lens: ulen=%d slen=%d want %d", r.ulen, r.slen, len(obj.Data))
	}
	got, err := decodePayload(r.flags, r.ulen, rec[recHeaderSize:recHeaderSize+int(r.slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, obj.Data) {
		t.Fatal("payload mismatch")
	}
}

func TestRecordRoundTripCompressed(t *testing.T) {
	obj := blobObj(t, compressible(64<<10))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.flags != flagZstd {
		t.Fatalf("repetitive data must compress, got flags %#x", r.flags)
	}
	if r.slen >= r.ulen {
		t.Fatalf("compressed slen=%d must be < ulen=%d", r.slen, r.ulen)
	}
	got, err := decodePayload(r.flags, r.ulen, rec[recHeaderSize:recHeaderSize+int(r.slen)])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, obj.Data) {
		t.Fatal("payload mismatch after decompression")
	}
}

func TestRecordEmptyPayload(t *testing.T) {
	obj := blobObj(t, nil)
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	if r.ulen != 0 || r.slen != 0 || r.flags != 0 {
		t.Fatalf("empty payload: ulen=%d slen=%d flags=%#x", r.ulen, r.slen, r.flags)
	}
}

func TestRecordTooLarge(t *testing.T) {
	if maxPayload != math.MaxUint32 {
		t.Fatalf("maxPayload = %d, want %d", maxPayload, uint64(math.MaxUint32))
	}
}

func TestParseRecordRejectsCorruption(t *testing.T) {
	obj := blobObj(t, incompressible(1024))
	rec, err := encodeRecord(obj.Key, obj.Data)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated header", func(t *testing.T) {
		if _, err := parseRecord(rec[:recHeaderSize-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("truncated payload", func(t *testing.T) {
		if _, err := parseRecord(rec[:len(rec)-1]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad tag", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[0] = 0x7F
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad flags", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[33] = 0x80
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped payload byte fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[len(bad)-1] ^= 0x01
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("flipped length fails CRC", func(t *testing.T) {
		bad := bytes.Clone(rec)
		bad[39] ^= 0x01 // inside slen, keeps record long enough to parse
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("non-canonical key", func(t *testing.T) {
		// Encode with an invalid type nibble; encodeRecord does not validate
		// keys (callers supply canonical keys), parseRecord must.
		var k key.Key
		copy(k[:], obj.Key[:])
		k[0] = 0xF0 // type 15: reserved
		bad, err := encodeRecord(k, obj.Data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("raw ulen != slen", func(t *testing.T) {
		bad := bytes.Clone(rec)
		binary.BigEndian.PutUint32(bad[34:38], r0ulen(bad)+1)
		fixCRC(bad)
		if _, err := parseRecord(bad); err == nil {
			t.Fatal("want error")
		}
	})
}

// r0ulen reads the ulen field of a record.
func r0ulen(rec []byte) uint32 { return binary.BigEndian.Uint32(rec[34:38]) }

// fixCRC recomputes a record's CRC after test tampering.
func fixCRC(rec []byte) {
	binary.BigEndian.PutUint32(rec[42:46], 0)
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
}
