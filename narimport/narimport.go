// Package narimport parses an untrusted Nix NAR stream into fstree objects
// with canonical metadata (uid/gid/mtime 0, modes 0444/0555/0777).
package narimport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

const (
	sIFREG = 0o100000
	sIFDIR = 0o040000
	sIFLNK = 0o120000

	nameMax   = 255
	targetMax = 4095
	depthMax  = 512
)

// ErrMalformed wraps all input-validation failures.
var ErrMalformed = errors.New("narimport: malformed NAR")

// Import reads one NAR from r, emits objects children-before-parents, and
// returns the root key. Exec-file and symlink roots are wrapped in a DirLeaf
// with one empty-named entry so the root key alone reproduces the NAR.
func Import(r io.Reader, emit fstree.Emit) (key.Key, error) {
	p := &parser{r: r, emit: emit, ic: chunkers.NewItemChunker(7)}
	if err := p.expect("nix-archive-1"); err != nil {
		return key.Key{}, err
	}
	e, err := p.node(nil, 0)
	if err != nil {
		return key.Key{}, err
	}
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != io.EOF {
		return key.Key{}, fmt.Errorf("%w: trailing data after root node", ErrMalformed)
	}
	switch e.Mode {
	case sIFDIR | 0o555, sIFREG | 0o444:
		return key.Parse(e.ContentKey)
	}
	db := fstree.NewDirBuilder(p.ic)
	if err := db.AddEntry(emit, e); err != nil {
		return key.Key{}, err
	}
	return db.Finish(emit)
}

type parser struct {
	r    io.Reader
	emit fstree.Emit
	ic   chunkers.ItemChunker
}

func (p *parser) readNum() (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(p.r, b[:]); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

func (p *parser) readPad(n uint64) error {
	r := n % 8
	if r == 0 {
		return nil
	}
	var b [8]byte
	if _, err := io.ReadFull(p.r, b[:8-r]); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if !bytes.Equal(b[:8-r], make([]byte, 8-r)) {
		return fmt.Errorf("%w: nonzero padding", ErrMalformed)
	}
	return nil
}

func (p *parser) readStr(max uint64) ([]byte, error) {
	n, err := p.readNum()
	if err != nil {
		return nil, err
	}
	if n > max {
		return nil, fmt.Errorf("%w: string of %d bytes exceeds limit %d", ErrMalformed, n, max)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(p.r, b); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return b, p.readPad(n)
}

func (p *parser) expect(tok string) error {
	b, err := p.readStr(64)
	if err != nil {
		return err
	}
	if string(b) != tok {
		return fmt.Errorf("%w: expected %q, got %q", ErrMalformed, tok, b)
	}
	return nil
}

func (p *parser) node(name []byte, depth int) (fstree.Entry, error) {
	if depth > depthMax {
		return fstree.Entry{}, fmt.Errorf("%w: nesting deeper than %d", ErrMalformed, depthMax)
	}
	if err := p.expect("("); err != nil {
		return fstree.Entry{}, err
	}
	if err := p.expect("type"); err != nil {
		return fstree.Entry{}, err
	}
	typ, err := p.readStr(64)
	if err != nil {
		return fstree.Entry{}, err
	}
	e := fstree.Entry{Name: name}
	switch string(typ) {
	case "regular":
		mode := uint64(sIFREG | 0o444)
		tok, err := p.readStr(64)
		if err != nil {
			return fstree.Entry{}, err
		}
		if string(tok) == "executable" {
			mode = sIFREG | 0o555
			if err := p.expect(""); err != nil {
				return fstree.Entry{}, err
			}
			tok, err = p.readStr(64)
			if err != nil {
				return fstree.Entry{}, err
			}
		}
		if string(tok) != "contents" {
			return fstree.Entry{}, fmt.Errorf("%w: expected \"contents\", got %q", ErrMalformed, tok)
		}
		ck, err := p.contents()
		if err != nil {
			return fstree.Entry{}, err
		}
		e.Mode, e.ContentKey = mode, ck[:]
	case "symlink":
		if err := p.expect("target"); err != nil {
			return fstree.Entry{}, err
		}
		target, err := p.readStr(targetMax)
		if err != nil {
			return fstree.Entry{}, err
		}
		if len(target) == 0 {
			return fstree.Entry{}, fmt.Errorf("%w: empty symlink target", ErrMalformed)
		}
		e.Mode, e.LinkTarget = sIFLNK|0o777, target
	case "directory":
		ck, err := p.directory(depth)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.Mode, e.ContentKey = sIFDIR|0o555, ck[:]
		return e, nil // directory consumes its own ")"
	default:
		return fstree.Entry{}, fmt.Errorf("%w: unknown node type %q", ErrMalformed, typ)
	}
	if err := p.expect(")"); err != nil {
		return fstree.Entry{}, err
	}
	return e, nil
}

func (p *parser) contents() (key.Key, error) {
	size, err := p.readNum()
	if err != nil {
		return key.Key{}, err
	}
	ib := fstree.NewFileIndexBuilder(p.ic)
	saw := false
	add := func(chunk []byte) error {
		saw = true
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := p.emit(obj); err != nil {
			return err
		}
		return ib.AddChild(p.emit, obj.Key, nil)
	}
	lr := &io.LimitedReader{R: p.r, N: int64(size)}
	if err := chunkers.SplitBytes(lr, nil, add); err != nil {
		return key.Key{}, err
	}
	if lr.N != 0 {
		return key.Key{}, fmt.Errorf("%w: truncated file contents", ErrMalformed)
	}
	if !saw {
		if err := add(nil); err != nil {
			return key.Key{}, err
		}
	}
	if err := p.readPad(size); err != nil {
		return key.Key{}, err
	}
	return ib.Finish(p.emit)
}

// Parses entries (strictly increasing names) through the trailing ")".
func (p *parser) directory(depth int) (key.Key, error) {
	db := fstree.NewDirBuilder(p.ic)
	var prev []byte
	for {
		tok, err := p.readStr(64)
		if err != nil {
			return key.Key{}, err
		}
		switch string(tok) {
		case ")":
			return db.Finish(p.emit)
		case "entry":
			if err := p.expect("("); err != nil {
				return key.Key{}, err
			}
			if err := p.expect("name"); err != nil {
				return key.Key{}, err
			}
			name, err := p.readStr(nameMax)
			if err != nil {
				return key.Key{}, err
			}
			if err := validName(name); err != nil {
				return key.Key{}, err
			}
			if prev != nil && bytes.Compare(name, prev) <= 0 {
				return key.Key{}, fmt.Errorf("%w: entry %q not after %q", ErrMalformed, name, prev)
			}
			prev = name
			if err := p.expect("node"); err != nil {
				return key.Key{}, err
			}
			e, err := p.node(name, depth+1)
			if err != nil {
				return key.Key{}, err
			}
			if err := p.expect(")"); err != nil {
				return key.Key{}, err
			}
			if err := db.AddEntry(p.emit, e); err != nil {
				return key.Key{}, err
			}
		default:
			return key.Key{}, fmt.Errorf("%w: expected \"entry\" or \")\", got %q", ErrMalformed, tok)
		}
	}
}

func validName(name []byte) error {
	if len(name) == 0 || string(name) == "." || string(name) == ".." ||
		bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: invalid entry name %q", ErrMalformed, name)
	}
	return nil
}
