package fstree

import (
	"errors"
	"fmt"
	"strings"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// ErrNotFound reports a path component that does not exist in its directory.
var ErrNotFound = errors.New("entry not found")

// ErrNotDir reports a path component that exists but is not a directory.
var ErrNotDir = errors.New("not a directory")

// ResolvePath descends from the directory object root along the slash-separated
// path and returns the key of the directory it names. Empty components and "."
// are ignored, so "", ".", and paths with leading/trailing slashes are
// accepted; ".." is rejected (a CAS tree has no parent links). A missing
// component wraps ErrNotFound, a non-directory component wraps ErrNotDir.
func ResolvePath(root key.Key, path string, get func(key.Key) ([]byte, error)) (key.Key, error) {
	k := root
	for comp := range strings.SplitSeq(path, "/") {
		if comp == "" || comp == "." {
			continue
		}
		if comp == ".." {
			return key.Key{}, fmt.Errorf("fstree: %q: \"..\" is not supported", path)
		}
		found, err := LookupEntry(k, []byte(comp), get)
		if err != nil {
			return key.Key{}, err
		}
		if found.Mode&unix.S_IFMT != unix.S_IFDIR {
			return key.Key{}, fmt.Errorf("fstree: %q: %w", comp, ErrNotDir)
		}
		ck, err := key.Parse(found.ContentKey)
		if err != nil {
			return key.Key{}, fmt.Errorf("fstree: %q: content key: %w", comp, err)
		}
		k = ck
	}
	return k, nil
}

// ResolveEntry descends from the directory object root along the
// slash-separated path and returns the entry the final component names — of
// any kind (file, directory, symlink, device, …), carrying its metadata. The
// empty path (or chains of "" and ".") returns nil: the root directory is not
// an entry and has no metadata of its own. Intermediate components must name
// directories (ErrNotDir otherwise); a missing component wraps ErrNotFound;
// ".." is rejected.
func ResolveEntry(root key.Key, path string, get func(key.Key) ([]byte, error)) (*Entry, error) {
	dir := root
	var cur *Entry // entry of dir; nil while dir is the root
	for comp := range strings.SplitSeq(path, "/") {
		if comp == "" || comp == "." {
			continue
		}
		if comp == ".." {
			return nil, fmt.Errorf("fstree: %q: \"..\" is not supported", path)
		}
		if cur != nil {
			if cur.Mode&unix.S_IFMT != unix.S_IFDIR {
				return nil, fmt.Errorf("fstree: %q: %w", cur.Name, ErrNotDir)
			}
			ck, err := key.Parse(cur.ContentKey)
			if err != nil {
				return nil, fmt.Errorf("fstree: %q: content key: %w", cur.Name, err)
			}
			dir = ck
		}
		ent, err := LookupEntry(dir, []byte(comp), get)
		if err != nil {
			return nil, err
		}
		cur = &ent
	}
	return cur, nil
}

// CollectEntries returns the directory entries reachable from k, descending
// DirNode index levels into the DirLeaves that hold them. Entries are returned
// in name order (the order the leaves store them). get fetches the bytes stored
// under a key.
func CollectEntries(k key.Key, get func(key.Key) ([]byte, error)) ([]Entry, error) {
	data, err := get(k)
	if err != nil {
		return nil, fmt.Errorf("fstree: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := DecodeDirLeaf(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
		return entries, nil
	case key.DirNode:
		pairs, err := DecodeDirNode(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
		var out []Entry
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			sub, err := CollectEntries(ck, get)
			if err != nil {
				return nil, err
			}
			out = append(out, sub...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
	}
}
