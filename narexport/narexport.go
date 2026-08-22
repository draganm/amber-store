// Package narexport streams the canonical uncompressed Nix NAR of a stored
// tree. The deterministic inverse of narimport.
package narexport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

const (
	sIFMT  = 0o170000
	sIFREG = 0o100000
	sIFDIR = 0o040000
	sIFLNK = 0o120000
)

// Export streams the NAR of the tree at root to w. Blob/FileNode roots are
// non-executable regular files. A single-empty-named-entry DirLeaf is
// narimport's exec-file/symlink root wrapper and is unwrapped.
func Export(w io.Writer, root key.Key, get func(key.Key) ([]byte, error)) error {
	e := &exporter{w: w, get: get}
	e.str("nix-archive-1")
	re, err := rootEntry(root, get)
	if err != nil {
		return err
	}
	if err := e.node(re); err != nil {
		return err
	}
	return e.flush()
}

func rootEntry(root key.Key, get func(key.Key) ([]byte, error)) (fstree.Entry, error) {
	switch root.Type() {
	case key.Blob, key.FileNode:
		return fstree.Entry{Mode: sIFREG | 0o444, ContentKey: root[:]}, nil
	case key.DirLeaf:
		b, err := get(root)
		if err != nil {
			return fstree.Entry{}, fmt.Errorf("narexport: reading root %s: %w", root, err)
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
		return fstree.Entry{}, fmt.Errorf("narexport: unsupported root type %s", root.Type())
	}
}

type exporter struct {
	w   io.Writer
	get func(key.Key) ([]byte, error)
	buf []byte
}

func (e *exporter) str(tok string) { e.strBytes([]byte(tok)) }

func (e *exporter) strBytes(b []byte) {
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(len(b)))
	e.buf = append(e.buf, l[:]...)
	e.buf = append(e.buf, b...)
	e.pad(uint64(len(b)))
}

func (e *exporter) pad(n uint64) {
	if r := n % 8; r != 0 {
		e.buf = append(e.buf, make([]byte, 8-r)...)
	}
}

func (e *exporter) flush() error {
	if len(e.buf) == 0 {
		return nil
	}
	_, err := e.w.Write(e.buf)
	e.buf = e.buf[:0]
	return err
}

func (e *exporter) node(en fstree.Entry) error {
	e.str("(")
	e.str("type")
	switch en.Mode & sIFMT {
	case sIFREG:
		e.str("regular")
		if en.Mode&0o111 != 0 {
			e.str("executable")
			e.str("")
		}
		ck, err := key.Parse(en.ContentKey)
		if err != nil {
			return fmt.Errorf("narexport: file content key: %w", err)
		}
		size := ck.Length()
		e.str("contents")
		var l [8]byte
		binary.LittleEndian.PutUint64(l[:], size)
		e.buf = append(e.buf, l[:]...)
		if err := e.flush(); err != nil {
			return err
		}
		if err := fstree.WriteContent(e.w, ck, e.get); err != nil {
			return err
		}
		e.pad(size)
	case sIFLNK:
		e.str("symlink")
		e.str("target")
		e.strBytes(en.LinkTarget)
	case sIFDIR:
		e.str("directory")
		ck, err := key.Parse(en.ContentKey)
		if err != nil {
			return fmt.Errorf("narexport: directory content key: %w", err)
		}
		if err := e.dirEntries(ck); err != nil {
			return err
		}
	default:
		return fmt.Errorf("narexport: mode %#o not representable in NAR", en.Mode)
	}
	e.str(")")
	if len(e.buf) >= 1<<20 {
		return e.flush()
	}
	return nil
}

func (e *exporter) dirEntries(k key.Key) error {
	b, err := e.get(k)
	if err != nil {
		return fmt.Errorf("narexport: reading %s %s: %w", k.Type(), k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := fstree.DecodeDirLeaf(b)
		if err != nil {
			return err
		}
		for _, en := range entries {
			e.str("entry")
			e.str("(")
			e.str("name")
			e.strBytes(en.Name)
			e.str("node")
			if err := e.node(en); err != nil {
				return err
			}
			e.str(")")
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
				return fmt.Errorf("narexport: DirNode child key: %w", err)
			}
			if err := e.dirEntries(ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("narexport: unexpected object type %s in directory", k.Type())
	}
}
