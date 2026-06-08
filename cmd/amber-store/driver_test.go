package main

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/key"
)

// readTar returns a map from member name (hex key) to its bytes, plus the
// ordered list of names.
func readTar(t *testing.T, b []byte) (map[string][]byte, []string) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(b))
	members := map[string][]byte{}
	var order []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		members[h.Name] = data
		order = append(order, h.Name)
	}
	return members, order
}

func TestPack_SmallTreeRootIsLast(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	ic := chunkers.NewItemChunker(7)
	root, err := pack(dir, &buf, ic, nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	members, order := readTar(t, buf.Bytes())
	if order[len(order)-1] != root.String() {
		t.Errorf("last member = %s, want root %s", order[len(order)-1], root)
	}
	if _, ok := members[root.String()]; !ok {
		t.Errorf("root member missing")
	}
	if root.Type() != key.DirLeaf && root.Type() != key.DirNode {
		t.Errorf("root type = %v, want a directory object", root.Type())
	}
}

func TestPack_DeduplicatesIdenticalFiles(t *testing.T) {
	dir := t.TempDir()
	content := bytes.Repeat([]byte("x"), 100)
	if err := os.WriteFile(filepath.Join(dir, "one"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "two"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256); err != nil {
		t.Fatal(err)
	}
	members, _ := readTar(t, buf.Bytes())
	// Both files share one Blob; count Blob-type members.
	blobs := 0
	for name := range members {
		raw, err := hex.DecodeString(name)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if k.Type() == key.Blob {
			blobs++
		}
	}
	if blobs != 1 {
		t.Errorf("blob members = %d, want 1 (deduplicated)", blobs)
	}
}

func TestPack_Deterministic(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	var b1, b2 bytes.Buffer
	r1, err := pack(dir, &b1, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := pack(dir, &b2, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Errorf("root differs across runs: %s vs %s", r1, r2)
	}
	if !bytes.Equal(b1.Bytes(), b2.Bytes()) {
		t.Errorf("tar bytes differ across runs of identical input")
	}
}

func TestPack_FailFastOnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "secret")
	if err := os.WriteFile(p, []byte("data"), 0o000); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	_, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err == nil {
		t.Errorf("expected pack to fail on an unreadable file")
	}
}
