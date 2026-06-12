// Package packstore persists Amber-Store CAS objects in log-structured,
// append-only segment (pack) files. The store directory contains only segment
// files: sealed segments are immutable, mmap'd whole, and self-indexed by a
// footer (fanout index on the last key byte + binary fuse filter + fixed
// trailer); the single active segment is recovered by a tail-scan. There is no
// global index. All format integers are big-endian. See
// docs/superpowers/specs/2026-06-13-packstore-design.md.
package packstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/draganm/amber-store/key"
	"github.com/klauspost/compress/zstd"
)

const (
	recHeaderSize = 46

	tagChunk  byte = 0x01
	tagDelete byte = 0x02 // reserved for v2 GC; never written in v1
	tagSeal   byte = 0xF0 // first byte of the footer

	flagZstd byte = 0x01

	maxPayload = math.MaxUint32 // u32 length fields cap one object at 4 GiB
)

var (
	magicHeader  = []byte("AMBERSG\x01")
	magicTrailer = []byte("AMBERSGF")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)

// ErrCorrupt wraps every structural-corruption error (bad record framing, bad
// footer, scrub findings). Callers distinguish it with errors.Is.
var ErrCorrupt = errors.New("packstore: corrupt segment")

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}

// Shared zstd coders; EncodeAll/DecodeAll are safe for concurrent use.
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	if zstdEnc, err = zstd.NewWriter(nil); err != nil {
		panic(err)
	}
	if zstdDec, err = zstd.NewReader(nil); err != nil {
		panic(err)
	}
}

// record describes a parsed record header. The payload lives at
// [recHeaderSize : recHeaderSize+slen] within the record's bytes.
type record struct {
	key   key.Key
	flags byte
	ulen  uint32
	slen  uint32
}

// encodeRecord serializes (k, data) into a complete record, compressing the
// payload with zstd when that makes it strictly smaller. k is written as
// given; canonical-form validation happens on the read side.
func encodeRecord(k key.Key, data []byte) ([]byte, error) {
	if uint64(len(data)) > maxPayload {
		return nil, fmt.Errorf("packstore: object %s too large: %d bytes", k, len(data))
	}
	payload := data
	flags := byte(0)
	if comp := zstdEnc.EncodeAll(data, make([]byte, 0, len(data))); len(comp) < len(data) {
		payload = comp
		flags = flagZstd
	}
	rec := make([]byte, recHeaderSize+len(payload))
	rec[0] = tagChunk
	copy(rec[1:33], k[:])
	rec[33] = flags
	binary.BigEndian.PutUint32(rec[34:38], uint32(len(data)))
	binary.BigEndian.PutUint32(rec[38:42], uint32(len(payload)))
	copy(rec[recHeaderSize:], payload)
	// CRC over the whole record; the crc field itself is still zero here.
	binary.BigEndian.PutUint32(rec[42:46], crc32.Checksum(rec, castagnoli))
	return rec, nil
}

var zero4 [4]byte

// parseRecord validates the record at the start of b (which may extend past
// it) and returns its header. It checks framing, flags, key canonicality, and
// the CRC, without mutating b (b may be a read-only mmap).
func parseRecord(b []byte) (record, error) {
	if len(b) < recHeaderSize {
		return record{}, fmt.Errorf("%w: truncated record header", ErrCorrupt)
	}
	if b[0] != tagChunk {
		return record{}, fmt.Errorf("%w: unexpected record tag %#x", ErrCorrupt, b[0])
	}
	flags := b[33]
	if flags&^flagZstd != 0 {
		return record{}, fmt.Errorf("%w: unknown record flags %#x", ErrCorrupt, flags)
	}
	ulen := binary.BigEndian.Uint32(b[34:38])
	slen := binary.BigEndian.Uint32(b[38:42])
	if int64(len(b)) < recHeaderSize+int64(slen) {
		return record{}, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	if flags&flagZstd == 0 && ulen != slen {
		return record{}, fmt.Errorf("%w: raw record with ulen %d != slen %d", ErrCorrupt, ulen, slen)
	}
	if flags&flagZstd != 0 && slen >= ulen {
		return record{}, fmt.Errorf("%w: compressed record with slen %d >= ulen %d", ErrCorrupt, slen, ulen)
	}
	c := crc32.Update(0, castagnoli, b[:42])
	c = crc32.Update(c, castagnoli, zero4[:])
	c = crc32.Update(c, castagnoli, b[recHeaderSize:recHeaderSize+int(slen)])
	if c != binary.BigEndian.Uint32(b[42:46]) {
		return record{}, fmt.Errorf("%w: record CRC mismatch", ErrCorrupt)
	}
	k, err := key.Parse(b[1:33])
	if err != nil {
		return record{}, fmt.Errorf("%w: record key: %v", ErrCorrupt, err)
	}
	return record{key: k, flags: flags, ulen: ulen, slen: slen}, nil
}

// decodePayload returns caller-owned payload bytes from a record's stored
// payload. stored may be a read-only mmap slice and is never retained.
func decodePayload(flags byte, ulen uint32, stored []byte) ([]byte, error) {
	if flags&flagZstd == 0 {
		out := make([]byte, len(stored))
		copy(out, stored)
		return out, nil
	}
	out, err := zstdDec.DecodeAll(stored, make([]byte, 0, ulen))
	if err != nil {
		return nil, fmt.Errorf("%w: zstd: %v", ErrCorrupt, err)
	}
	if uint32(len(out)) != ulen {
		return nil, fmt.Errorf("%w: decompressed to %d bytes, header says %d", ErrCorrupt, len(out), ulen)
	}
	return out, nil
}
