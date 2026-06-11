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
		children, err := ChildKeys(k, data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := walk(ck); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	return out, nil
}
