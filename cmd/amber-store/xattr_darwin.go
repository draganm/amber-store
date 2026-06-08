//go:build darwin

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes. Called only for
// non-symlink entries, so the follow-symlink behavior of the plain calls does
// not matter.
func readXattrs(path string) (map[string][]byte, error) {
	sz, err := unix.Listxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	return readAllXattrs(path, buf[:sz], unix.Getxattr)
}
