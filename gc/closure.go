// The closure file: the sorted tails of every key reachable from one root.
//
//	magic   "AMBERCL\x01"   8 bytes
//	root    [32]byte        checked against the file name
//	count   u64 BE
//	tails   count × u64 BE  ascending, deduplicated
//	crc     u32 BE          CRC-32C over everything before it
//
// A root's tree is immutable, so its closure is too. A file failing any
// check counts as absent: closures are derived data, rebuilt by a walk.

package gc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"

	"github.com/draganm/amber-store/key"
)

const closureMagic = "AMBERCL\x01"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var errBadClosure = errors.New("gc: bad closure file")

const closureHead = len(closureMagic) + key.Size + 8

// encodeClosure serializes root's tails, which must be sorted ascending and
// deduplicated (tailsOf's output).
func encodeClosure(root key.Key, tails []uint64) []byte {
	b := make([]byte, 0, closureHead+8*len(tails)+4)
	b = append(b, closureMagic...)
	b = append(b, root[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(len(tails)))
	for _, t := range tails {
		b = binary.BigEndian.AppendUint64(b, t)
	}
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeClosure parses a closure file for root; any deviation is
// errBadClosure.
func decodeClosure(root key.Key, b []byte) ([]uint64, error) {
	if len(b) < closureHead+4 || string(b[:len(closureMagic)]) != closureMagic {
		return nil, errBadClosure
	}
	if !bytes.Equal(b[len(closureMagic):len(closureMagic)+key.Size], root[:]) {
		return nil, errBadClosure
	}
	count := binary.BigEndian.Uint64(b[len(closureMagic)+key.Size : closureHead])
	// Derive the expected count from the buffer's actual size rather than
	// multiplying the attacker-controlled count by 8: count is a u64 read
	// straight from the file, and 8*count can overflow and wrap back to a
	// value that spuriously matches len(b) (see architecture/simple-gc.md
	// and the fix report in task-7-report.md for the crafted 52-byte
	// input this guards against).
	remaining := uint64(len(b)) - uint64(closureHead) - 4 // safe: len(b) >= closureHead+4, checked above
	if remaining%8 != 0 || count != remaining/8 {
		return nil, errBadClosure
	}
	if crc32.Checksum(b[:len(b)-4], castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, errBadClosure
	}
	tails := make([]uint64, count)
	for i := range tails {
		tails[i] = binary.BigEndian.Uint64(b[closureHead+8*i:])
		if i > 0 && tails[i] <= tails[i-1] {
			return nil, errBadClosure
		}
	}
	return tails, nil
}

// tailsOf is the closure of a visited-key list: sorted, deduplicated tails.
func tailsOf(keys []key.Key) []uint64 {
	tails := make([]uint64, len(keys))
	for i, k := range keys {
		tails[i] = Tail(k)
	}
	slices.Sort(tails)
	return slices.Compact(tails)
}
