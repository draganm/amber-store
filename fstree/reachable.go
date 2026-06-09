package fstree

import (
	"fmt"

	"github.com/draganm/amber-store/key"
)

// ReachableKeys returns the keys of every object reachable from root — the set
// that must be fetched to hold the whole content — in depth-first pre-order,
// root first, each key listed once even when referenced repeatedly. root may be
// any object type. get fetches the bytes stored under a key; Blob and XattrSet
// objects are leaves and are not fetched.
func ReachableKeys(root key.Key, get func(key.Key) ([]byte, error)) ([]key.Key, error) {
	seen := map[key.Key]bool{}
	var out []key.Key
	var walk func(k key.Key) error
	walk = func(k key.Key) error {
		if seen[k] {
			return nil
		}
		seen[k] = true
		out = append(out, k)
		if k.Type() == key.Blob || k.Type() == key.XattrSet {
			return nil
		}
		data, err := get(k)
		if err != nil {
			return fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		switch k.Type() {
		case key.FileNode:
			children, err := DecodeFileNode(data)
			if err != nil {
				return fmt.Errorf("fstree: decoding FileNode %s: %w", k, err)
			}
			for _, ck := range children {
				if err := walk(ck); err != nil {
					return err
				}
			}
		case key.DirNode:
			pairs, err := DecodeDirNode(data)
			if err != nil {
				return fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
			}
			for _, p := range pairs {
				ck, err := key.Parse(p.ChildKey)
				if err != nil {
					return fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
				}
				if err := walk(ck); err != nil {
					return err
				}
			}
		case key.DirLeaf:
			entries, err := DecodeDirLeaf(data)
			if err != nil {
				return fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
			}
			for _, ent := range entries {
				if len(ent.ContentKey) > 0 {
					ck, err := key.Parse(ent.ContentKey)
					if err != nil {
						return fmt.Errorf("fstree: %q: content key: %w", ent.Name, err)
					}
					if err := walk(ck); err != nil {
						return err
					}
				}
				if len(ent.XattrsKey) > 0 {
					xk, err := key.Parse(ent.XattrsKey)
					if err != nil {
						return fmt.Errorf("fstree: %q: xattrs key: %w", ent.Name, err)
					}
					if err := walk(xk); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	return out, nil
}
