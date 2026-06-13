// Package amberpack defines the pack-write format: a flat, sequential,
// stream-friendly serialization of CAS objects, used both as the body uploaded
// to the daemon and as a standalone on-disk artifact. A stream is a
// possibly-partial, unordered set of objects (like a git pack) and carries no
// root key. Layout:
//
//	Header   "AMBERPK\x01"                  8 bytes  magic + version
//	Records  repeat: 0x01 key[32] uvarint(len) payload
//	End      0x00
//
// The 32-byte key already encodes the object's type and logical length, so no
// separate fields are needed. The Reader validates framing and that each key is
// canonical; it does NOT verify the payload hash — that happens in the storage
// path (packstore verification).
package amberpack

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// magic identifies the format and its version (the trailing byte).
const magic = "AMBERPK\x01"

const (
	tagObject byte = 0x01 // an object record follows
	tagEnd    byte = 0x00 // end of stream
)

// ErrMalformed wraps every error from a structurally invalid stream (bad magic,
// bad record tag, non-canonical key, or truncation). Callers distinguish it with
// errors.Is to map to a client error.
var ErrMalformed = errors.New("amberpack: malformed stream")

// maxPayloadBytes bounds a single object's payload so a corrupt or hostile
// stream cannot trigger an unbounded allocation. It is far above any real CAS
// object (chunker MaxSize is on the order of a few hundred KiB).
const maxPayloadBytes = 256 << 20 // 256 MiB

// Writer serializes fstree.Objects into the pack-write format. It is not safe
// for concurrent use; a client wanting parallel uploads creates one Writer per
// stream.
type Writer struct {
	bw          *bufio.Writer
	wroteHeader bool
}

// NewWriter returns a Writer emitting to w. The caller owns w and must close it;
// Writer.Close only flushes and writes the end marker.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) ensureHeader() error {
	if w.wroteHeader {
		return nil
	}
	if _, err := w.bw.WriteString(magic); err != nil {
		return err
	}
	w.wroteHeader = true
	return nil
}

// Add appends one object record.
func (w *Writer) Add(o fstree.Object) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagObject); err != nil {
		return err
	}
	if _, err := w.bw.Write(o.Key[:]); err != nil {
		return err
	}
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(o.Bytes)))
	if _, err := w.bw.Write(lb[:n]); err != nil {
		return err
	}
	_, err := w.bw.Write(o.Bytes)
	return err
}

// Close writes the header (if no object was added) and the end marker, then
// flushes. It does not close the underlying writer.
func (w *Writer) Close() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	if err := w.bw.WriteByte(tagEnd); err != nil {
		return err
	}
	return w.bw.Flush()
}

// Reader decodes a pack-write stream.
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// All iterates over the objects in the stream. It yields exactly one error (and
// stops) on any structural problem; on a clean stream it yields every object and
// returns after the end marker. All must be called at most once per Reader
// because the underlying stream position is not reset between calls.
func (r *Reader) All() iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		var hdr [len(magic)]byte
		if _, err := io.ReadFull(r.br, hdr[:]); err != nil {
			yield(fstree.Object{}, fmt.Errorf("%w: reading header: %v", ErrMalformed, err))
			return
		}
		if string(hdr[:]) != magic {
			yield(fstree.Object{}, fmt.Errorf("%w: bad magic", ErrMalformed))
			return
		}
		for {
			tag, err := r.br.ReadByte()
			if err != nil {
				yield(fstree.Object{}, fmt.Errorf("%w: truncated before end marker: %v", ErrMalformed, err))
				return
			}
			switch tag {
			case tagEnd:
				return
			case tagObject:
				var kb [key.Size]byte
				if _, err := io.ReadFull(r.br, kb[:]); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading key: %v", ErrMalformed, err))
					return
				}
				k, err := key.Parse(kb[:])
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: non-canonical key: %v", ErrMalformed, err))
					return
				}
				n, err := binary.ReadUvarint(r.br)
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading length: %v", ErrMalformed, err))
					return
				}
				if n > maxPayloadBytes {
					yield(fstree.Object{}, fmt.Errorf("%w: payload length %d exceeds limit %d", ErrMalformed, n, maxPayloadBytes))
					return
				}
				payload := make([]byte, n)
				if _, err := io.ReadFull(r.br, payload); err != nil {
					yield(fstree.Object{}, fmt.Errorf("%w: reading payload: %v", ErrMalformed, err))
					return
				}
				if !yield(fstree.Object{Key: k, Bytes: payload}, nil) {
					return
				}
			default:
				yield(fstree.Object{}, fmt.Errorf("%w: bad record tag %#x", ErrMalformed, tag))
				return
			}
		}
	}
}
