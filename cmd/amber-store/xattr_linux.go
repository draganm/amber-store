//go:build linux

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes without following
// symlinks. Called only for non-symlink entries.
func readXattrs(path string) (map[string][]byte, error) {
	sz, err := unix.Llistxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = unix.Llistxattr(path, buf)
	if err != nil {
		return nil, err
	}
	return readAllXattrs(path, buf[:sz], unix.Lgetxattr)
}
