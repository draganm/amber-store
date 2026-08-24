package gc

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

func crc32Of(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

func testRoot(t *testing.T) key.Key {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte("root-blob"))
	if err != nil {
		t.Fatal(err)
	}
	return o.Key
}

func TestClosureRoundTrip(t *testing.T) {
	root := testRoot(t)
	for _, tails := range [][]uint64{nil, {42}, {1, 2, 3, 1 << 63}} {
		b := encodeClosure(root, tails)
		got, err := decodeClosure(root, b)
		if err != nil {
			t.Fatalf("decode(%v): %v", tails, err)
		}
		if len(got) != len(tails) {
			t.Fatalf("got %v, want %v", got, tails)
		}
		for i := range got {
			if got[i] != tails[i] {
				t.Fatalf("got %v, want %v", got, tails)
			}
		}
	}
}

func TestClosureRejects(t *testing.T) {
	root := testRoot(t)
	good := encodeClosure(root, []uint64{5, 9})
	cases := map[string]func() ([]byte, key.Key){
		"crc flip": func() ([]byte, key.Key) {
			b := append([]byte(nil), good...)
			b[len(b)-1] ^= 1
			return b, root
		},
		"magic": func() ([]byte, key.Key) {
			b := append([]byte(nil), good...)
			b[0] = 'X'
			return b, root
		},
		"wrong root": func() ([]byte, key.Key) {
			other, err := fstree.EncodeBlob([]byte("other"))
			if err != nil {
				t.Fatal(err)
			}
			return good, other.Key
		},
		"truncated": func() ([]byte, key.Key) { return good[:len(good)-5], root },
		"count lies": func() ([]byte, key.Key) {
			b := append([]byte(nil), good...)
			binary.BigEndian.PutUint64(b[8+32:], 99)
			return b, root
		},
		"unsorted": func() ([]byte, key.Key) {
			b := encodeClosure(root, nil)
			// Hand-build a 2-tail body out of order, then re-CRC.
			b = b[:len(b)-4]
			b = b[:8+32]
			b = binary.BigEndian.AppendUint64(b, 2)
			b = binary.BigEndian.AppendUint64(b, 9)
			b = binary.BigEndian.AppendUint64(b, 5)
			return binary.BigEndian.AppendUint32(b, crc32Of(b)), root
		},
		"duplicate": func() ([]byte, key.Key) {
			b := encodeClosure(root, nil)
			b = b[:8+32]
			b = binary.BigEndian.AppendUint64(b, 2)
			b = binary.BigEndian.AppendUint64(b, 7)
			b = binary.BigEndian.AppendUint64(b, 7)
			return binary.BigEndian.AppendUint32(b, crc32Of(b)), root
		},
		"count overflow": func() ([]byte, key.Key) {
			// count = 1<<61 so 8*count overflows to 0 mod 2^64: closureHead
			// (48) + 8*count + 4 wraps to exactly 52, matching this
			// 52-byte buffer's real length, with a correctly recomputed
			// CRC over the 48-byte prefix. Must not panic on decode.
			b := make([]byte, 0, closureHead+4)
			b = append(b, closureMagic...)
			b = append(b, root[:]...)
			b = binary.BigEndian.AppendUint64(b, 1<<61)
			b = binary.BigEndian.AppendUint32(b, crc32Of(b))
			return b, root
		},
		"below minimum": func() ([]byte, key.Key) {
			// Shorter than closureHead+4: exercises the first guard
			// directly, distinct from "truncated" (which trims a
			// full-sized closure by a few bytes but stays above the
			// minimum).
			return good[:20], root
		},
	}
	for name, mk := range cases {
		b, r := mk()
		if _, err := decodeClosure(r, b); err == nil {
			t.Errorf("%s: decode accepted", name)
		}
	}
}

func TestTailsOf(t *testing.T) {
	a, _ := fstree.EncodeBlob([]byte("a"))
	b, _ := fstree.EncodeBlob([]byte("b"))
	got := tailsOf([]key.Key{b.Key, a.Key, b.Key})
	if len(got) != 2 {
		t.Fatalf("got %d tails, want 2 (sorted, deduplicated)", len(got))
	}
	if got[0] >= got[1] {
		t.Error("tails not ascending")
	}
	if got[0] != min(Tail(a.Key), Tail(b.Key)) {
		t.Error("tail values wrong")
	}
}

func TestTailMatchesSpec(t *testing.T) {
	k := testRoot(t)
	if Tail(k) != binary.BigEndian.Uint64(k[24:32]) {
		t.Error("Tail is not key[24:32] BE")
	}
}
