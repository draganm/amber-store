package main

import (
	"bytes"
	"errors"

	"golang.org/x/sys/unix"
)

// splitXattrNames splits a NUL-separated xattr name list into names, dropping
// empty entries.
func splitXattrNames(buf []byte) []string {
	var names []string
	for _, n := range bytes.Split(buf, []byte{0}) {
		if len(n) > 0 {
			names = append(names, string(n))
		}
	}
	return names
}

// readXattrsWith lists xattrs with list and reads each with get (the
// unix.Llistxattr / unix.Lgetxattr shapes). ENOTSUP from a filesystem
// without xattr support means "no xattrs", as in tar and rsync.
func readXattrsWith(path string, list func(string, []byte) (int, error), get func(string, string, []byte) (int, error)) (map[string][]byte, error) {
	sz, err := list(path, nil)
	if err != nil {
		return nil, ignoreUnsupported(err)
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = list(path, buf)
	if err != nil {
		return nil, ignoreUnsupported(err)
	}
	return readAllXattrs(path, buf[:sz], get)
}

func ignoreUnsupported(err error) error {
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		return nil
	}
	return err
}

// readAllXattrs fetches each named attribute's value using get, returning a map.
// get has the signature of unix.Lgetxattr / unix.Getxattr: (path, attr, dest).
func readAllXattrs(path string, nameBuf []byte, get func(string, string, []byte) (int, error)) (map[string][]byte, error) {
	names := splitXattrNames(nameBuf)
	if len(names) == 0 {
		return nil, nil
	}
	m := make(map[string][]byte, len(names))
	for _, name := range names {
		sz, err := get(path, name, nil)
		if err != nil {
			return nil, err
		}
		val := make([]byte, sz)
		sz, err = get(path, name, val)
		if err != nil {
			return nil, err
		}
		m[name] = val[:sz]
	}
	return m, nil
}
