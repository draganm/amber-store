package castar

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	"github.com/draganm/amber-store/key"
)

func mkKey(t *testing.T, tp key.Type, n int) key.Key {
	t.Helper()
	k, err := key.New(tp, uint64(n), []byte{byte(n)})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func readNames(t *testing.T, b []byte) []string {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(b))
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	return names
}

func TestSink_DedupsAndKeepsRootLast(t *testing.T) {
	var buf bytes.Buffer
	s := NewSink(&buf)
	a := mkKey(t, key.Blob, 1)
	b := mkKey(t, key.Blob, 2)
	root := mkKey(t, key.DirLeaf, 3)
	if err := s.Put(a, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(b, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(a, []byte("a")); err != nil { // duplicate, must be skipped
		t.Fatal(err)
	}
	if err := s.PutRoot(root, []byte("r")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	names := readNames(t, buf.Bytes())
	want := []string{a.String(), b.String(), root.String()}
	if len(names) != len(want) {
		t.Fatalf("members = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("member %d = %s, want %s", i, names[i], want[i])
		}
	}
}
