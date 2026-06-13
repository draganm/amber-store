package packstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// ErrVerify is returned (wrapped) when an object's key does not match its
// payload. Callers distinguish it with errors.Is to map to a client error.
var ErrVerify = errors.New("packstore: object verification failed")

// Verify scrubs every sealed segment: walks the body record by record
// (validating framing, CRCs, and that each payload re-hashes to its key),
// recomputes the index section and compares it bytewise with the footer's,
// and checks the filter contains every body key. The active segment is
// covered by tail-scan on reopen, not by Verify.
func (s *Store) Verify(ctx context.Context) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	segs := make([]*sealedSegment, len(s.sealed))
	copy(segs, s.sealed)
	s.mu.RUnlock()

	for _, seg := range segs {
		if err := seg.verify(ctx); err != nil {
			return err
		}
	}
	return nil
}

// verify scrubs one sealed segment. The segment is immutable and the caller
// holds no locks: scrubbing runs concurrently with reads and writes.
func (g *sealedSegment) verify(ctx context.Context) error {
	var entries []indexEntry
	off := int64(len(magicHeader))
	for off < g.fv.bodyLen {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := parseRecord(g.mm[off:g.fv.bodyLen])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		payload, err := decodePayload(rec.flags, rec.ulen, g.mm[off+recHeaderSize:off+recHeaderSize+int64(rec.slen)])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		if err := verifyObject(Object{Key: rec.key, Data: payload}); err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		if !g.fv.filter.Contains(filterKey(rec.key)) {
			return fmt.Errorf("%w: %s: filter missing key %s (offset %d)", ErrCorrupt, g.path, rec.key, off)
		}
		entries = append(entries, indexEntry{k: rec.key, off: uint64(off), slen: rec.slen})
		off += recHeaderSize + int64(rec.slen)
	}
	if off != g.fv.bodyLen {
		return fmt.Errorf("%w: %s: records end at %d, trailer says %d", ErrCorrupt, g.path, off, g.fv.bodyLen)
	}
	if uint64(len(entries)) != g.fv.keyCount {
		return fmt.Errorf("%w: %s: body has %d records, trailer says %d", ErrCorrupt, g.path, len(entries), g.fv.keyCount)
	}
	rebuilt := buildIndexSection(entries)
	stored := g.mm[g.fv.indexOff : g.fv.indexOff+g.fv.indexLen]
	if !bytes.Equal(rebuilt, stored) {
		return fmt.Errorf("%w: %s: index section does not match body", ErrCorrupt, g.path)
	}
	return nil
}

// verifyObject recomputes o.Key from o.Data and reports ErrVerify on mismatch.
// For Blob and XattrSet — whose key length is the serialized byte length — it
// also checks the length field. Aggregate types (FileNode/DirLeaf/DirNode)
// carry a logical length the store cannot recompute without parsing, so only
// their hash is checked.
func verifyObject(o Object) error {
	sum := blake3.Sum256(o.Data)
	want, err := key.NewFromHash(o.Key.Type(), o.Key.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
	}
	if want != o.Key {
		return fmt.Errorf("%w: payload hashes to %s, not %s", ErrVerify, want, o.Key)
	}
	switch o.Key.Type() {
	case key.Blob, key.XattrSet:
		if o.Key.Length() != uint64(len(o.Data)) {
			return fmt.Errorf("%w: %s length field %d != payload %d", ErrVerify, o.Key, o.Key.Length(), len(o.Data))
		}
	}
	return nil
}
