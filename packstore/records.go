package packstore

import (
	"fmt"
	"iter"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
)

// WriteRecords stores already-encoded records verbatim, skipping
// recompression. Every record is verified (CRC, key, payload rehash)
// before append. The contract otherwise matches WriteParallel.
func (s *Store) WriteRecords(seq iter.Seq2[amberpack.RawRecord, error], opts WriteOpts, recycle func(amberpack.RawRecord)) (WriteStats, error) {
	return writePipeline(s, seq, opts, func(r amberpack.RawRecord) key.Key { return r.Key }, recycle,
		func(r amberpack.RawRecord, scratch *[]byte) (recLen, dataLen int, err error) {
			if err := verifyRecordBytes(r.Key, r.Bytes, scratch); err != nil {
				return 0, 0, err
			}
			return len(r.Bytes), len(r.Bytes), s.append(r.Key, r.Bytes, false)
		})
}

// verifyRecordBytes: CRC, key match, payload rehashed against the key.
func verifyRecordBytes(k key.Key, rec []byte, scratch *[]byte) error {
	if err := checkRecord(k, rec, scratch); err != nil {
		return fmt.Errorf("%w: record for %s: %w", ErrVerify, k, err)
	}
	return nil
}

// checkRecord is the full record scrub: framing CRC, key match, and the
// payload rehashed against the key. Raw payloads hash in place.
func checkRecord(k key.Key, rec []byte, scratch *[]byte) error {
	parsed, err := amberpack.ParseRecord(rec)
	if err != nil {
		return err
	}
	if parsed.Key != k {
		return fmt.Errorf("record keyed %s, expected %s", parsed.Key, k)
	}
	stored := rec[amberpack.RecHeaderSize:]
	payload := stored
	if parsed.Flags&amberpack.FlagZstd != 0 {
		payload, err = amberpack.AppendPayload((*scratch)[:0], parsed.Flags, parsed.Ulen, stored)
		if payload != nil {
			*scratch = payload
		}
		if err != nil {
			return err
		}
	}
	return verifyObject(Object{Key: k, Data: payload})
}
