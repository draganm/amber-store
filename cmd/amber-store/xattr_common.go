package main

import "bytes"

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
