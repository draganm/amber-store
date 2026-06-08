// Package chunkers provides the two content-defined chunkers the fstree uses:
// a byte chunker (ultracdc) for file content, and an item chunker for the
// index/entry streams (architecture/fstree.md, "Content-defined chunking").
package chunkers

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// ItemChunker decides chunk boundaries between whole items (a child key, a
// directory entry, or a [sepName, childKey] pair). A boundary ends the current
// run when the low k bits of BLAKE3(item encoding) are zero (average run 2^k),
// bounded by MinRun and MaxRun so an item is never split and variance is capped.
type ItemChunker struct {
	MinRun int
	MaxRun int
	mask   uint64
}

// NewItemChunker returns an item chunker with average run 2^bits and derived
// bounds MinRun = 2^bits / 4 (at least 2) and MaxRun = 2^bits * 4.
func NewItemChunker(bits int) ItemChunker {
	avg := 1 << bits
	minRun := max(avg/4, 2)
	var mask uint64
	if bits > 0 {
		mask = uint64(1)<<bits - 1
	}
	return ItemChunker{MinRun: minRun, MaxRun: avg * 4, mask: mask}
}

// IsBoundary reports whether the run should end after the item whose canonical
// encoding is enc, given the current run length (including this item).
func (c ItemChunker) IsBoundary(enc []byte, runLen int) bool {
	if runLen >= c.MaxRun {
		return true
	}
	if runLen < c.MinRun {
		return false
	}
	sum := blake3.Sum256(enc)
	return binary.LittleEndian.Uint64(sum[:8])&c.mask == 0
}
