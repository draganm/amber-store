//go:build linux

package main

import "golang.org/x/sys/unix"

// readXattrs lists and reads an entry's extended attributes without following
// symlinks. Called only for non-symlink entries.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Llistxattr, unix.Lgetxattr)
}
