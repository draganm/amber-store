package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestEntryMeta_RegularFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("hi"), 0o640); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	m := entryMeta(info)
	if m.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Errorf("type bits = %o, want S_IFREG", m.Mode&unix.S_IFMT)
	}
	if m.Mode&0o777 != 0o640 {
		t.Errorf("perm bits = %o, want 640", m.Mode&0o777)
	}
}

func TestReadXattrs_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(p, "user.greeting", []byte("hello"), 0); err != nil {
		t.Skipf("xattrs unsupported on this filesystem: %v", err)
	}
	m, err := readXattrs(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(m["user.greeting"]) != "hello" {
		t.Errorf("xattr value = %q, want hello", m["user.greeting"])
	}
}
