// Package inbox stores authenticated packs that have been received but not yet
// processed into the packstore. An entry is a single self-describing file: a
// CBOR meta header framed by its big-endian length, followed by the raw
// amberpack body. Entries are content-addressed by blake3 of the body, so a
// re-received identical pack is idempotent. A pool of workers drains the
// directory into the store; setting a reference waits on the entries tagged
// with that root. The directory is the only durable state — on restart a scan
// rebuilds the in-memory view and resumes processing.
package inbox

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// Meta tags a staged pack with the (ref, root) it belongs to and when it
// arrived. The barrier keys on Root alone; Ref and ReceivedAt are carried for
// operability (which ref a pending pack was for, how old it is).
type Meta struct {
	Ref        string `cbor:"0,keyasint"`
	Root       []byte `cbor:"1,keyasint"` // 32-byte root key
	ReceivedAt int64  `cbor:"2,keyasint"` // ns since the Unix epoch
}

// encMode mirrors the deterministic CBOR conventions used across the project
// (reference, httpsig, fstree).
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("inbox: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// writeMetaHeader writes [u32 BE meta-len][meta CBOR] to w.
func writeMetaHeader(w io.Writer, m Meta) error {
	b, err := encMode.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding inbox meta: %w", err)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// readMetaHeader reads a header written by writeMetaHeader and leaves r
// positioned at the first body byte.
func readMetaHeader(r io.Reader) (Meta, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Meta{}, fmt.Errorf("reading inbox meta length: %w", err)
	}
	buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return Meta{}, fmt.Errorf("reading inbox meta: %w", err)
	}
	var m Meta
	if err := cbor.Unmarshal(buf, &m); err != nil {
		return Meta{}, fmt.Errorf("decoding inbox meta: %w", err)
	}
	return m, nil
}
