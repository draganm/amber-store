// Package narzstd streams a zstd-compressed NAR by splicing stored zstd
// record frames verbatim (decoders accept concatenated frames), framing
// only NAR structure on the fly. Output decompresses to the canonical
// NAR, so upstream NarHash/signatures verify.
package narzstd

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/klauspost/compress/zstd"
)

const flushThreshold = 1 << 20

const (
	sIFMT  = 0o170000
	sIFREG = 0o100000
	sIFDIR = 0o040000
	sIFLNK = 0o120000
)

var enc *zstd.Encoder

func init() {
	var err error
	// Only ever compresses NAR framing bytes.
	enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		panic(err)
	}
}

// Write streams the zstd NAR of the tree at root. get returns
// decompressed interior nodes. ViewRecord lends a raw record
// (packstore.Store.ViewRecord's shape). A DirLeaf holding one empty-named
// entry is narimport's file/symlink root wrapper and is unwrapped.
func Write(w io.Writer, root key.Key, get func(key.Key) ([]byte, error), viewRecord func(key.Key, func([]byte) error) error) error {
	s := &stitcher{w: w, get: get, viewRecord: viewRecord}
	s.str("nix-archive-1")
	re, err := rootEntry(root, get)
	if err != nil {
		return err
	}
	if err := s.node(re); err != nil {
		return err
	}
	return s.flush()
}

func rootEntry(root key.Key, get func(key.Key) ([]byte, error)) (fstree.Entry, error) {
	switch root.Type() {
	case key.DirLeaf:
		b, err := get(root)
		if err != nil {
			return fstree.Entry{}, fmt.Errorf("narzstd: reading root %s: %w", root, err)
		}
		entries, err := fstree.DecodeDirLeaf(b)
		if err != nil {
			return fstree.Entry{}, err
		}
		if len(entries) == 1 && len(entries[0].Name) == 0 {
			return entries[0], nil
		}
		return fstree.Entry{Mode: sIFDIR | 0o555, ContentKey: root[:]}, nil
	case key.DirNode:
		return fstree.Entry{Mode: sIFDIR | 0o555, ContentKey: root[:]}, nil
	default:
		return fstree.Entry{Mode: sIFREG | 0o444, ContentKey: root[:]}, nil
	}
}

type stitcher struct {
	w          io.Writer
	get        func(key.Key) ([]byte, error)
	viewRecord func(key.Key, func([]byte) error) error
	buf        []byte // pending structural bytes, not yet framed
	scratch    []byte // reused EncodeAll output buffer
}

// One NAR string token: u64-LE length, bytes, zero padding to 8.
func (s *stitcher) str(tok string) { s.strBytes([]byte(tok)) }

func (s *stitcher) strBytes(b []byte) {
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(len(b)))
	s.buf = append(s.buf, l[:]...)
	s.buf = append(s.buf, b...)
	s.pad(uint64(len(b)))
}

func (s *stitcher) pad(n uint64) {
	if r := n % 8; r != 0 {
		s.buf = append(s.buf, make([]byte, 8-r)...)
	}
}

func (s *stitcher) flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	s.scratch = enc.EncodeAll(s.buf, s.scratch[:0])
	s.buf = s.buf[:0]
	_, err := s.w.Write(s.scratch)
	return err
}

const rawBlockMax = 128 << 10

// One stored-raw zstd frame: magic, descriptor 0xE0 (single-segment, 8-byte
// content size), u64-LE size, raw blocks.
func (s *stitcher) writeRawFrame(data []byte) error {
	hdr := make([]byte, 0, 16)
	hdr = append(hdr, 0x28, 0xB5, 0x2F, 0xFD, 0xE0)
	hdr = binary.LittleEndian.AppendUint64(hdr, uint64(len(data)))
	if _, err := s.w.Write(hdr); err != nil {
		return err
	}
	for {
		n := min(len(data), rawBlockMax)
		last := n == len(data)
		bh := uint32(n) << 3
		if last {
			bh |= 1
		}
		if _, err := s.w.Write([]byte{byte(bh), byte(bh >> 8), byte(bh >> 16)}); err != nil {
			return err
		}
		if _, err := s.w.Write(data[:n]); err != nil {
			return err
		}
		data = data[n:]
		if last {
			return nil
		}
	}
}

func (s *stitcher) node(e fstree.Entry) error {
	s.str("(")
	s.str("type")
	switch e.Mode & sIFMT {
	case sIFREG:
		s.str("regular")
		if e.Mode&0o111 != 0 {
			s.str("executable")
			s.str("")
		}
		if err := s.contents(e); err != nil {
			return err
		}
	case sIFLNK:
		s.str("symlink")
		s.str("target")
		s.strBytes(e.LinkTarget)
	case sIFDIR:
		s.str("directory")
		ck, err := key.Parse(e.ContentKey)
		if err != nil {
			return fmt.Errorf("narzstd: directory content key: %w", err)
		}
		if err := s.dirEntries(ck); err != nil {
			return err
		}
	default:
		return fmt.Errorf("narzstd: mode %#o not representable in NAR", e.Mode)
	}
	s.str(")")
	if len(s.buf) >= flushThreshold {
		return s.flush()
	}
	return nil
}

func (s *stitcher) contents(e fstree.Entry) error {
	ck, err := key.Parse(e.ContentKey)
	if err != nil {
		return fmt.Errorf("narzstd: file content key: %w", err)
	}
	size := ck.Length()
	s.str("contents")
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], size)
	s.buf = append(s.buf, l[:]...)
	if err := s.fileData(ck); err != nil {
		return err
	}
	s.pad(size)
	return nil
}

func (s *stitcher) fileData(k key.Key) error {
	switch k.Type() {
	case key.Blob:
		return s.blob(k)
	case key.FileNode:
		b, err := s.get(k)
		if err != nil {
			return fmt.Errorf("narzstd: reading FileNode %s: %w", k, err)
		}
		children, err := fileNodeChildren(b)
		if err != nil {
			return err
		}
		for _, c := range children {
			if err := s.fileData(c); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("narzstd: unexpected object type %s in file content", k.Type())
	}
}

// Fast decode of a FileNode: deterministic CBOR array of 32-byte strings.
func fileNodeChildren(b []byte) ([]key.Key, error) {
	n, off := 0, 0
	switch {
	case len(b) > 0 && b[0] >= 0x80 && b[0] <= 0x97:
		n, off = int(b[0]-0x80), 1
	case len(b) >= 2 && b[0] == 0x98:
		n, off = int(b[1]), 2
	case len(b) >= 3 && b[0] == 0x99:
		n, off = int(binary.BigEndian.Uint16(b[1:3])), 3
	case len(b) >= 5 && b[0] == 0x9a:
		n, off = int(binary.BigEndian.Uint32(b[1:5])), 5
	default:
		return fstree.DecodeFileNode(b)
	}
	if len(b)-off != n*(2+key.Size) {
		return fstree.DecodeFileNode(b)
	}
	out := make([]key.Key, n)
	for i := range out {
		if b[off] != 0x58 || b[off+1] != key.Size {
			return fstree.DecodeFileNode(b)
		}
		k, err := key.Parse(b[off+2 : off+2+key.Size])
		if err != nil {
			return nil, err
		}
		out[i] = k
		off += 2 + key.Size
	}
	return out, nil
}

func (s *stitcher) blob(k key.Key) error {
	if err := s.flush(); err != nil {
		return err
	}
	err := s.viewRecord(k, func(rec []byte) error {
		if len(rec) < amberpack.RecHeaderSize {
			return fmt.Errorf("narzstd: truncated Blob record %s", k)
		}
		flags := rec[33]
		slen := binary.BigEndian.Uint32(rec[38:42])
		if len(rec) < amberpack.RecHeaderSize+int(slen) {
			return fmt.Errorf("narzstd: truncated Blob record %s", k)
		}
		payload := rec[amberpack.RecHeaderSize : amberpack.RecHeaderSize+int(slen)]
		if flags&amberpack.FlagZstd == 0 {
			return s.writeRawFrame(payload)
		}
		_, err := s.w.Write(payload)
		return err
	})
	if err != nil {
		return fmt.Errorf("narzstd: reading Blob record %s: %w", k, err)
	}
	return nil
}

func (s *stitcher) dirEntries(k key.Key) error {
	b, err := s.get(k)
	if err != nil {
		return fmt.Errorf("narzstd: reading %s %s: %w", k.Type(), k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := fstree.DecodeDirLeaf(b)
		if err != nil {
			return err
		}
		for _, e := range entries {
			s.str("entry")
			s.str("(")
			s.str("name")
			s.strBytes(e.Name)
			s.str("node")
			if err := s.node(e); err != nil {
				return err
			}
			s.str(")")
		}
		return nil
	case key.DirNode:
		pairs, err := fstree.DecodeDirNode(b)
		if err != nil {
			return err
		}
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return fmt.Errorf("narzstd: DirNode child key: %w", err)
			}
			if err := s.dirEntries(ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("narzstd: unexpected object type %s in directory", k.Type())
	}
}
