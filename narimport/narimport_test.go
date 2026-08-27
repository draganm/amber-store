package narimport_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/narexport"
	"github.com/draganm/amber-store/narimport"
)

type memStore map[key.Key][]byte

func (m memStore) emit(o fstree.Object) error { m[o.Key] = o.Bytes; return nil }
func (m memStore) get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

func nixNar(t *testing.T, path string) []byte {
	t.Helper()
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix not found")
	}
	out, err := exec.Command(nix, "nar", "dump-path", path).Output()
	if err != nil {
		t.Fatalf("nix nar dump-path: %v", err)
	}
	return out
}

func roundTrip(t *testing.T, nar []byte) key.Key {
	t.Helper()
	st := memStore{}
	root, err := narimport.Import(bytes.NewReader(nar), st.emit)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	var out bytes.Buffer
	if err := narexport.Export(&out, root, st.get); err != nil {
		t.Fatalf("export: %v", err)
	}
	if !bytes.Equal(out.Bytes(), nar) {
		t.Fatalf("round trip mismatch: %d bytes in, %d out", len(nar), out.Len())
	}
	return root
}

func TestRoundTripTree(t *testing.T) {
	dir := t.TempDir()
	big := make([]byte, 300<<10)
	rand.Read(big)
	for p, b := range map[string][]byte{
		"a.txt":          []byte("hello\n"),
		"big.bin":        big,
		"empty":          {},
		"sub/nested/c":   []byte("deep"),
		"sub/z-last.txt": bytes.Repeat([]byte("compressible "), 1000),
	} {
		full := filepath.Join(dir, p)
		os.MkdirAll(filepath.Dir(full), 0o755)
		if err := os.WriteFile(full, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(dir, "run.sh"), []byte("#!/bin/sh\n"), 0o755)
	os.Symlink("a.txt", filepath.Join(dir, "link"))
	os.Mkdir(filepath.Join(dir, "emptydir"), 0o755)

	root := roundTrip(t, nixNar(t, dir))
	if typ := root.Type(); typ != key.DirLeaf && typ != key.DirNode {
		t.Fatalf("directory NAR root has type %s", typ)
	}
}

func TestRoundTripFileRoots(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	os.WriteFile(plain, []byte("data\n"), 0o644)
	execf := filepath.Join(dir, "exec")
	os.WriteFile(execf, []byte("#!/bin/sh\n"), 0o755)
	link := filepath.Join(dir, "link")
	os.Symlink("/nix/store/somewhere", link)

	root := roundTrip(t, nixNar(t, plain))
	if typ := root.Type(); typ != key.Blob && typ != key.FileNode {
		t.Fatalf("plain file root has type %s", typ)
	}
	for _, p := range []string{execf, link} {
		if typ := roundTrip(t, nixNar(t, p)).Type(); typ != key.DirLeaf {
			t.Fatalf("%s root not wrapped in DirLeaf, got %s", p, typ)
		}
	}
}

func TestImportDeterministic(t *testing.T) {
	nar := nixNar(t, mkTree(t))
	st := memStore{}
	k1, err := narimport.Import(bytes.NewReader(nar), st.emit)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := narimport.Import(bytes.NewReader(nar), st.emit)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Fatalf("keys differ: %s vs %s", k1, k2)
	}
}

func mkTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644)
	return dir
}

type nb struct{ bytes.Buffer }

func (b *nb) str(s string) *nb {
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(len(s)))
	b.Write(l[:])
	b.WriteString(s)
	if r := len(s) % 8; r != 0 {
		b.Write(make([]byte, 8-r))
	}
	return b
}

func file(entries ...string) *nb {
	b := &nb{}
	b.str("nix-archive-1").str("(").str("type").str("regular")
	for _, e := range entries {
		b.str(e)
	}
	return b
}

func dirNar(build func(b *nb)) []byte {
	b := &nb{}
	b.str("nix-archive-1").str("(").str("type").str("directory")
	build(b)
	b.str(")")
	return b.Bytes()
}

func entry(b *nb, name string, node func(b *nb)) {
	b.str("entry").str("(").str("name").str(name).str("node")
	node(b)
	b.str(")")
}

func fileNode(b *nb) {
	b.str("(").str("type").str("regular").str("contents").str("").str(")")
}

func TestImportRejectsMalformed(t *testing.T) {
	longName := string(bytes.Repeat([]byte("n"), 256))
	cases := map[string][]byte{
		"bad magic":     (&nb{}).str("nix-archive-2").str("(").Bytes(),
		"empty":         {},
		"truncated":     (&nb{}).str("nix-archive-1").str("(").Bytes(),
		"unknown type":  (&nb{}).str("nix-archive-1").str("(").str("type").str("socket").Bytes(),
		"empty target":  (&nb{}).str("nix-archive-1").str("(").str("type").str("symlink").str("target").str("").str(")").Bytes(),
		"trailing data": append(file("contents", "", ")").Bytes(), 0),
		"unsorted": dirNar(func(b *nb) {
			entry(b, "b", fileNode)
			entry(b, "a", fileNode)
		}),
		"duplicate name": dirNar(func(b *nb) {
			entry(b, "a", fileNode)
			entry(b, "a", fileNode)
		}),
		"empty name":     dirNar(func(b *nb) { entry(b, "", fileNode) }),
		"dot name":       dirNar(func(b *nb) { entry(b, ".", fileNode) }),
		"dotdot name":    dirNar(func(b *nb) { entry(b, "..", fileNode) }),
		"slash in name":  dirNar(func(b *nb) { entry(b, "a/b", fileNode) }),
		"NUL in name":    dirNar(func(b *nb) { entry(b, "a\x00b", fileNode) }),
		"oversized name": dirNar(func(b *nb) { entry(b, longName, fileNode) }),
	}
	pad := file("contents").Bytes()
	pad = append(pad, 1, 0, 0, 0, 0, 0, 0, 0, 'x', 0xff, 0, 0, 0, 0, 0, 0)
	cases["nonzero padding"] = pad

	for name, nar := range cases {
		t.Run(name, func(t *testing.T) {
			st := memStore{}
			if _, err := narimport.Import(bytes.NewReader(nar), st.emit); err == nil {
				t.Fatal("import accepted malformed NAR")
			}
		})
	}
}

func FuzzImport(f *testing.F) {
	f.Add(file("contents", "hi", ")").Bytes())
	f.Add(dirNar(func(b *nb) { entry(b, "a", fileNode) }))
	f.Fuzz(func(t *testing.T, nar []byte) {
		st := memStore{}
		root, err := narimport.Import(bytes.NewReader(nar), st.emit)
		if err != nil {
			return
		}
		var out bytes.Buffer
		if err := narexport.Export(&out, root, st.get); err != nil {
			t.Fatalf("export of imported NAR failed: %v", err)
		}
		if !bytes.Equal(out.Bytes(), nar) {
			t.Fatalf("round trip mismatch")
		}
	})
}
