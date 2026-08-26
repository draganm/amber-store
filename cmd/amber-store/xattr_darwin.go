//go:build darwin

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes. Called only for
// non-symlink entries, so the follow-symlink behavior of the plain calls does
// not matter.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Listxattr, unix.Getxattr)
}
