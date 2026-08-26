package main

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReadXattrsWith_UnsupportedFilesystemMeansNone(t *testing.T) {
	for _, errno := range []error{unix.ENOTSUP, unix.EOPNOTSUPP} {
		list := func(string, []byte) (int, error) { return 0, errno }
		get := func(string, string, []byte) (int, error) { t.Fatal("get called"); return 0, nil }
		m, err := readXattrsWith("/x", list, get)
		if err != nil || m != nil {
			t.Fatalf("%v: got (%v, %v), want (nil, nil)", errno, m, err)
		}
	}
}

func TestReadXattrsWith_OtherErrorsPropagate(t *testing.T) {
	list := func(string, []byte) (int, error) { return 0, unix.EACCES }
	_, err := readXattrsWith("/x", list, nil)
	if !errors.Is(err, unix.EACCES) {
		t.Fatalf("err = %v, want EACCES", err)
	}
}

func TestReadXattrsWith_ReadsValues(t *testing.T) {
	names := []byte("user.a\x00user.b\x00")
	list := func(_ string, buf []byte) (int, error) {
		if buf == nil {
			return len(names), nil
		}
		return copy(buf, names), nil
	}
	get := func(_ string, attr string, dst []byte) (int, error) {
		v := "v-" + attr
		if dst == nil {
			return len(v), nil
		}
		return copy(dst, v), nil
	}
	m, err := readXattrsWith("/x", list, get)
	if err != nil {
		t.Fatal(err)
	}
	if string(m["user.a"]) != "v-user.a" || string(m["user.b"]) != "v-user.b" {
		t.Fatalf("got %q", m)
	}
}
