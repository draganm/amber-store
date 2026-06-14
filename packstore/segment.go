// Package packstore persists Amber-Store CAS objects in log-structured,
// append-only segment (pack) files. The store directory contains only segment
// files: sealed segments are immutable, mmap'd whole, and self-indexed by a
// footer (fanout index on the last key byte + binary fuse filter + fixed
// trailer); the single active segment is recovered by a tail-scan. There is no
// global index. All format integers are big-endian. Record framing lives in the
// amberpack package. See docs/superpowers/specs/2026-06-13-packstore-design.md.
package packstore

import (
	"hash/crc32"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
)

const (
	tagSeal   byte = 0xF0 // first byte of the footer
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
)

var (
	magicHeader  = []byte("AMBERSG\x01")
	magicTrailer = []byte("AMBERSGF")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli) // footer CRC; record CRC lives in amberpack
)

// ErrCorrupt wraps every structural-corruption error (bad record framing, bad
// footer, scrub findings). It aliases amberpack's record-corruption sentinel so
// a single errors.Is target covers both record- and footer-level corruption.
var ErrCorrupt = amberpack.ErrCorrupt

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}
