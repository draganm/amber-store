// Package tarexport traverses an Amber-Store CAS from a directory key and writes
// a PAX-format tar of the filesystem tree. PAX is required to faithfully carry
// nanosecond mtimes, extended attributes (SCHILY.xattr.*), long names, and device
// nodes. Sockets cannot be archived and are skipped. The tree's root directory
// itself is not emitted (its metadata is not stored); only its descendants are.
package tarexport

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// Getter fetches the bytes stored under a key.
type Getter func(key.Key) ([]byte, error)

// Write streams a PAX tar of the directory tree rooted at root to w. root must
// be a directory object (DirLeaf or DirNode).
func Write(w io.Writer, root key.Key, get Getter) error {
	if root.Type() != key.DirLeaf && root.Type() != key.DirNode {
		return fmt.Errorf("tarexport: root %s is not a directory object (type %v)", root, root.Type())
	}
	tw := tar.NewWriter(w)
	e := &exporter{tw: tw, get: get}
	if err := e.dir(root, ""); err != nil {
		return err
	}
	return tw.Close()
}

type exporter struct {
	tw  *tar.Writer
	get Getter
}

// dir writes every entry of the directory object dirKey, prefixing names with
// prefix (the path of the directory relative to the export root, "" for root).
func (e *exporter) dir(dirKey key.Key, prefix string) error {
	entries, err := e.collectEntries(dirKey)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if err := e.entry(prefix, ent); err != nil {
			return err
		}
	}
	return nil
}

// collectEntries returns the entries reachable from k, descending DirNode index
// levels into the DirLeaves that hold them.
func (e *exporter) collectEntries(k key.Key) ([]fstree.Entry, error) {
	data, err := e.get(k)
	if err != nil {
		return nil, fmt.Errorf("tarexport: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := fstree.DecodeDirLeaf(data)
		if err != nil {
			return nil, fmt.Errorf("tarexport: decoding DirLeaf %s: %w", k, err)
		}
		return entries, nil
	case key.DirNode:
		pairs, err := fstree.DecodeDirNode(data)
		if err != nil {
			return nil, fmt.Errorf("tarexport: decoding DirNode %s: %w", k, err)
		}
		var out []fstree.Entry
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, fmt.Errorf("tarexport: child key in DirNode %s: %w", k, err)
			}
			sub, err := e.collectEntries(ck)
			if err != nil {
				return nil, err
			}
			out = append(out, sub...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("tarexport: %s is not a directory object (type %v)", k, k.Type())
	}
}

func (e *exporter) entry(prefix string, ent fstree.Entry) error {
	comp := string(ent.Name)
	if comp == "" || comp == "." || comp == ".." || strings.Contains(comp, "/") {
		return fmt.Errorf("tarexport: refusing unsafe entry name %q", comp)
	}
	name := path.Join(prefix, comp)
	hdr := &tar.Header{
		Name:    name,
		Mode:    int64(ent.Mode & 0o7777),
		Uid:     int(ent.UID),
		Gid:     int(ent.GID),
		ModTime: time.Unix(0, ent.Mtime),
		Format:  tar.FormatPAX,
	}
	if err := e.setXattrs(hdr, ent); err != nil {
		return err
	}

	switch ent.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		hdr.Typeflag = tar.TypeDir
		hdr.Name = name + "/"
		if err := e.tw.WriteHeader(hdr); err != nil {
			return err
		}
		ck, err := key.Parse(ent.ContentKey)
		if err != nil {
			return err
		}
		return e.dir(ck, name)

	case unix.S_IFREG:
		ck, err := key.Parse(ent.ContentKey)
		if err != nil {
			return err
		}
		hdr.Typeflag = tar.TypeReg
		hdr.Size = int64(ck.Length()) // FileNode/Blob length == content byte count
		if err := e.tw.WriteHeader(hdr); err != nil {
			return err
		}
		return e.writeContent(ck)

	case unix.S_IFLNK:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Linkname = string(ent.LinkTarget)
		return e.tw.WriteHeader(hdr)

	case unix.S_IFIFO:
		hdr.Typeflag = tar.TypeFifo
		return e.tw.WriteHeader(hdr)

	case unix.S_IFCHR, unix.S_IFBLK:
		if len(ent.Rdev) != 2 {
			return fmt.Errorf("tarexport: %s: device entry missing [major, minor]", name)
		}
		if ent.Mode&unix.S_IFMT == unix.S_IFCHR {
			hdr.Typeflag = tar.TypeChar
		} else {
			hdr.Typeflag = tar.TypeBlock
		}
		hdr.Devmajor = int64(ent.Rdev[0])
		hdr.Devminor = int64(ent.Rdev[1])
		return e.tw.WriteHeader(hdr)

	case unix.S_IFSOCK:
		// Sockets cannot be archived; skip (consistent with restore).
		return nil

	default:
		return fmt.Errorf("tarexport: %s: unsupported file type %#o", name, ent.Mode&unix.S_IFMT)
	}
}

// writeContent writes the bytes addressed by k to the current tar member,
// descending FileNode index levels and concatenating Blob leaves in order.
func (e *exporter) writeContent(k key.Key) error {
	data, err := e.get(k)
	if err != nil {
		return fmt.Errorf("tarexport: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.Blob:
		_, err := e.tw.Write(data)
		return err
	case key.FileNode:
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := e.writeContent(ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("tarexport: %s is not a file-content object (type %v)", k, k.Type())
	}
}

// setXattrs decodes an entry's extended attributes (inline or spilled) into the
// header's PAX records under the SCHILY.xattr.* namespace.
func (e *exporter) setXattrs(hdr *tar.Header, ent fstree.Entry) error {
	var (
		m   map[string][]byte
		err error
	)
	switch {
	case len(ent.XattrsIn) > 0:
		m, err = cborx.DecodeXattrs(ent.XattrsIn)
	case len(ent.XattrsKey) == key.Size:
		xk, perr := key.Parse(ent.XattrsKey)
		if perr != nil {
			return fmt.Errorf("tarexport: xattrs key for %s: %w", hdr.Name, perr)
		}
		data, gerr := e.get(xk)
		if gerr != nil {
			return fmt.Errorf("tarexport: reading xattrs %s for %s: %w", xk, hdr.Name, gerr)
		}
		m, err = cborx.DecodeXattrs(data)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}
	if hdr.PAXRecords == nil {
		hdr.PAXRecords = make(map[string]string, len(m))
	}
	for k, v := range m {
		hdr.PAXRecords["SCHILY.xattr."+k] = string(v)
	}
	return nil
}
