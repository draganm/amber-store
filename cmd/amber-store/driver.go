package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/draganm/amber-store/castar"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// pack builds the content-addressed tree for the directory at root, writing
// every unique chunk to w as a tar and the root object as the final member. It
// returns the root key. Any I/O or encoding error aborts (fail fast).
func pack(root string, w io.Writer, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, error) {
	sink := castar.NewSink(w)
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}

	var pending *fstree.Object
	emit := func(o fstree.Object) error {
		if pending != nil {
			if err := sink.Put(pending.Key, pending.Bytes); err != nil {
				return err
			}
		}
		cp := o
		pending = &cp
		return nil
	}

	rootKey, err := d.buildDir(root, emit)
	if err != nil {
		return key.Key{}, err
	}
	if pending == nil {
		return key.Key{}, fmt.Errorf("pack: no objects emitted")
	}
	if pending.Key != rootKey {
		return key.Key{}, fmt.Errorf("pack: internal error, last emitted %s != root %s", pending.Key, rootKey)
	}
	if err := sink.PutRoot(pending.Key, pending.Bytes); err != nil {
		return key.Key{}, err
	}
	if err := sink.Close(); err != nil {
		return key.Key{}, err
	}
	return rootKey, nil
}

type driver struct {
	ic             chunkers.ItemChunker
	byteOpts       *chunkers.ByteOpts
	xattrInlineMax int
}

// buildDir builds the directory at path and returns its root key, emitting every
// object in its subtree (children before parents).
func (d *driver) buildDir(path string, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	db := fstree.NewDirBuilder(d.ic)
	for _, de := range ents {
		full := filepath.Join(path, de.Name())
		e, err := d.buildEntry(full, de.Name(), emit, d.buildDir)
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

// buildEntry produces the directory entry for one path, recursing into files and
// subdirectories (and emitting their objects) and reading inline metadata for
// links and special files. buildDir is the function used to build a
// subdirectory: d.buildDir for the sequential pack walk, or the parallel
// builder's buildDir for concurrent ingestion.
func (d *driver) buildEntry(full, name string, emit fstree.Emit, buildDir func(string, fstree.Emit) (key.Key, error)) (fstree.Entry, error) {
	info, err := os.Lstat(full)
	if err != nil {
		return fstree.Entry{}, err
	}
	m := entryMeta(info)
	e := fstree.Entry{
		Name:  []byte(name),
		Mode:  m.Mode,
		UID:   m.UID,
		GID:   m.GID,
		Mtime: m.Mtime,
	}

	switch m.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		ck, err := d.buildFile(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFDIR:
		ck, err := buildDir(full, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFLNK:
		target, err := os.Readlink(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.LinkTarget = []byte(target)
	case unix.S_IFCHR, unix.S_IFBLK:
		major, minor := deviceNumbers(info)
		e.Rdev = []uint64{major, minor}
	case unix.S_IFIFO, unix.S_IFSOCK:
		// no payload key
	default:
		return fstree.Entry{}, fmt.Errorf("%s: unsupported file type %#o", full, m.Mode&unix.S_IFMT)
	}

	// Extended attributes (skip symlinks).
	if m.Mode&unix.S_IFMT != unix.S_IFLNK {
		xattrs, err := readXattrs(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		if len(xattrs) > 0 {
			enc := cborx.EncodeXattrs(xattrs)
			if len(enc) <= d.xattrInlineMax {
				e.XattrsIn = enc
			} else {
				obj, err := fstree.EncodeXattrSet(xattrs)
				if err != nil {
					return fstree.Entry{}, err
				}
				if err := emit(obj); err != nil {
					return fstree.Entry{}, err
				}
				e.XattrsKey = obj.Key[:]
			}
		}
	}
	return e, nil
}

// buildFile chunks a regular file into Blobs (emitting each), builds its FileNode
// index, and returns the file's root key.
func (d *driver) buildFile(full string, emit fstree.Emit) (key.Key, error) {
	f, err := os.Open(full)
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()

	ib := fstree.NewFileIndexBuilder(d.ic)
	saw := false
	err = chunkers.SplitBytes(f, d.byteOpts, func(chunk []byte) error {
		saw = true
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}
		return ib.AddChild(emit, obj.Key, nil)
	})
	if err != nil {
		return key.Key{}, err
	}
	if !saw {
		obj, err := fstree.EncodeBlob([]byte{})
		if err != nil {
			return key.Key{}, err
		}
		if err := emit(obj); err != nil {
			return key.Key{}, err
		}
		if err := ib.AddChild(emit, obj.Key, nil); err != nil {
			return key.Key{}, err
		}
	}
	return ib.Finish(emit)
}
