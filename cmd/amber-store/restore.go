package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/cborx"
	"github.com/draganm/amber-store/key"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

// restoreConfig is the fully-typed configuration for the restore command.
type restoreConfig struct {
	store string
}

// restoreCommand builds the restore command, wiring its flags into a restoreConfig.
func restoreCommand() *cli.Command {
	cfg := &restoreConfig{}
	return &cli.Command{
		Name:      "restore",
		Usage:     "restore the filesystem tree rooted at KEY from a diskstore into DIR",
		ArgsUsage: "KEY DIR",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "store",
				Aliases:     []string{"s"},
				Usage:       "diskstore directory to read objects from",
				Required:    true,
				Destination: &cfg.store,
			},
		},
		Action: func(c *cli.Context) error { return runRestore(c, cfg) },
	}
}

// Handles the 'restore' command.
func runRestore(c *cli.Context, cfg *restoreConfig) error {
	if c.NArg() != 2 {
		return fmt.Errorf("restore requires exactly two arguments KEY and DIR, got %d", c.NArg())
	}
	rootKey, err := parseHexKey(c.Args().Get(0))
	if err != nil {
		return err
	}
	outDir := c.Args().Get(1)

	store, err := diskstore.Open(cfg.store)
	if err != nil {
		return err
	}
	defer store.Close()

	return (&restorer{get: store.Get}).restoreTree(rootKey, outDir)
}

// parseHexKey decodes a lowercase-hex key argument into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	k, err := key.Parse(raw)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return k, nil
}

// restorer reconstructs a filesystem tree from CAS objects fetched via get.
type restorer struct {
	get func(key.Key) ([]byte, error)
}

// object fetches the bytes stored under k, wrapping not-found errors with context.
func (r *restorer) object(k key.Key) ([]byte, error) {
	data, err := r.get(k)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", k, err)
	}
	return data, nil
}

// restoreTree restores the directory tree rooted at rootKey into outDir, which
// is created if necessary. outDir's own metadata is not part of the tree (the
// root object holds only its children), so it keeps default attributes.
func (r *restorer) restoreTree(rootKey key.Key, outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	return r.restoreDir(rootKey, outDir)
}

// restoreDir restores every entry of the directory object dirKey into dir.
func (r *restorer) restoreDir(dirKey key.Key, dir string) error {
	entries, err := r.collectEntries(dirKey)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := r.restoreEntry(dir, e); err != nil {
			return err
		}
	}
	return nil
}

// collectEntries returns the directory entries reachable from k, descending
// through DirNode index levels into the DirLeaves that hold the entries.
func (r *restorer) collectEntries(k key.Key) ([]fstree.Entry, error) {
	data, err := r.object(k)
	if err != nil {
		return nil, err
	}
	switch k.Type() {
	case key.DirLeaf:
		return fstree.DecodeDirLeaf(data)
	case key.DirNode:
		pairs, err := fstree.DecodeDirNode(data)
		if err != nil {
			return nil, err
		}
		var out []fstree.Entry
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, err
			}
			sub, err := r.collectEntries(ck)
			if err != nil {
				return nil, err
			}
			out = append(out, sub...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s: not a directory object (type %v)", k, k.Type())
	}
}

// restoreEntry materializes a single directory entry under dir, recursing into
// subdirectories. Directory metadata is applied after its children so that
// writing them does not disturb its restored permissions and mtime.
func (r *restorer) restoreEntry(dir string, e fstree.Entry) error {
	name := string(e.Name)
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
		return fmt.Errorf("refusing unsafe entry name %q", name)
	}
	target := filepath.Join(dir, name)

	switch e.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		ck, err := key.Parse(e.ContentKey)
		if err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
		if err := os.Mkdir(target, 0o700); err != nil {
			return err
		}
		if err := r.restoreDir(ck, target); err != nil {
			return err
		}
		return r.applyMeta(target, e, false)

	case unix.S_IFREG:
		ck, err := key.Parse(e.ContentKey)
		if err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
		if err := r.writeRegular(target, ck); err != nil {
			return err
		}
		return r.applyMeta(target, e, false)

	case unix.S_IFLNK:
		if err := os.Symlink(string(e.LinkTarget), target); err != nil {
			return err
		}
		return r.applyMeta(target, e, true)

	case unix.S_IFIFO:
		if err := unix.Mkfifo(target, uint32(e.Mode&0o7777)); err != nil {
			return fmt.Errorf("%s: mkfifo: %w", target, err)
		}
		return r.applyMeta(target, e, false)

	case unix.S_IFCHR, unix.S_IFBLK:
		if len(e.Rdev) != 2 {
			return fmt.Errorf("%s: device entry missing [major, minor]", target)
		}
		dev := int(unix.Mkdev(uint32(e.Rdev[0]), uint32(e.Rdev[1])))
		mode := uint32(e.Mode&unix.S_IFMT) | uint32(e.Mode&0o7777)
		if err := unix.Mknod(target, mode, dev); err != nil {
			if isPrivilegeError(err) {
				fmt.Fprintf(os.Stderr, "amber-store: skipping device node %s: %v\n", target, err)
				return nil
			}
			return fmt.Errorf("%s: mknod: %w", target, err)
		}
		return r.applyMeta(target, e, false)

	case unix.S_IFSOCK:
		fmt.Fprintf(os.Stderr, "amber-store: skipping socket %s (cannot be recreated)\n", target)
		return nil

	default:
		return fmt.Errorf("%s: unsupported file type %#o", target, e.Mode&unix.S_IFMT)
	}
}

// writeRegular creates target and streams the file content addressed by
// contentKey into it. Permissions are set later by applyMeta.
func (r *restorer) writeRegular(target string, contentKey key.Key) error {
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := r.writeContent(f, contentKey); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// writeContent writes the bytes addressed by k to w, descending FileNode index
// levels and concatenating the Blob leaves in order.
func (r *restorer) writeContent(w io.Writer, k key.Key) error {
	data, err := r.object(k)
	if err != nil {
		return err
	}
	switch k.Type() {
	case key.Blob:
		_, err := w.Write(data)
		return err
	case key.FileNode:
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := r.writeContent(w, ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s: not a file-content object (type %v)", k, k.Type())
	}
}

// applyMeta restores permissions, ownership, extended attributes, and mtime for
// target. Permissions and mtime are restored faithfully; ownership is restored
// only when running as root; xattrs are best-effort. mtime is set last so no
// earlier step disturbs it.
func (r *restorer) applyMeta(target string, e fstree.Entry, isSymlink bool) error {
	if !isSymlink {
		if err := unix.Chmod(target, uint32(e.Mode&0o7777)); err != nil {
			return fmt.Errorf("%s: chmod: %w", target, err)
		}
	}
	if os.Geteuid() == 0 {
		if err := os.Lchown(target, int(e.UID), int(e.GID)); err != nil {
			return fmt.Errorf("%s: chown: %w", target, err)
		}
	}
	if !isSymlink {
		if err := r.restoreXattrs(target, e); err != nil {
			return err
		}
	}
	return setMtime(target, e.Mtime, isSymlink)
}

// restoreXattrs applies an entry's extended attributes, whether inlined in the
// leaf (key 8) or spilled to an XattrSet object (key 9).
func (r *restorer) restoreXattrs(target string, e fstree.Entry) error {
	var (
		m   map[string][]byte
		err error
	)
	switch {
	case len(e.XattrsIn) > 0:
		m, err = cborx.DecodeXattrs(e.XattrsIn)
	case len(e.XattrsKey) == key.Size:
		xk, perr := key.Parse(e.XattrsKey)
		if perr != nil {
			return fmt.Errorf("%s: xattrs key: %w", target, perr)
		}
		data, gerr := r.object(xk)
		if gerr != nil {
			return gerr
		}
		m, err = cborx.DecodeXattrs(data)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: %w", target, err)
	}
	return writeXattrs(target, m)
}

// setMtime sets target's modification time (and access time, which the tree
// does not store) to ns nanoseconds since the Unix epoch, without following a
// final symlink.
func setMtime(target string, ns int64, isSymlink bool) error {
	ts := unix.NsecToTimespec(ns)
	flags := 0
	if isSymlink {
		flags = unix.AT_SYMLINK_NOFOLLOW
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, target, []unix.Timespec{ts, ts}, flags); err != nil {
		return fmt.Errorf("%s: set mtime: %w", target, err)
	}
	return nil
}

// writeXattrs sets every extended attribute in m on path without following a
// final symlink. Permission and not-supported failures are reported and skipped
// rather than aborting the restore (e.g. restoring privileged namespaces as a
// non-root user, or onto a filesystem without xattr support).
func writeXattrs(path string, m map[string][]byte) error {
	for name, val := range m {
		if err := unix.Lsetxattr(path, name, val, 0); err != nil {
			if isPrivilegeError(err) || err == unix.ENOTSUP {
				fmt.Fprintf(os.Stderr, "amber-store: skipping xattr %q on %s: %v\n", name, path, err)
				continue
			}
			return fmt.Errorf("setting xattr %q on %s: %w", name, path, err)
		}
	}
	return nil
}

// isPrivilegeError reports whether err indicates the operation was refused for
// lack of privilege rather than a genuine fault.
func isPrivilegeError(err error) bool {
	return err == unix.EPERM || err == unix.EACCES || err == unix.EOPNOTSUPP
}
