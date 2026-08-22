package narzstd_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/narimport"
	"github.com/draganm/amber-store/narzstd"
	"github.com/klauspost/compress/zstd"
)

// memStore holds objects both decompressed and as amberpack records, mimicking
// packstore's Get/GetRecord pair.
type memStore struct {
	data map[key.Key][]byte
	recs map[key.Key][]byte
}

func newMemStore() *memStore {
	return &memStore{data: map[key.Key][]byte{}, recs: map[key.Key][]byte{}}
}

func (m *memStore) emit(t *testing.T) fstree.Emit {
	return func(o fstree.Object) error {
		m.data[o.Key] = o.Bytes
		rec, err := amberpack.EncodeRecord(o.Key, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		m.recs[o.Key] = rec
		return nil
	}
}

func (m *memStore) get(k key.Key) ([]byte, error)                     { return m.data[k], nil }
func (m *memStore) viewRecord(k key.Key, fn func([]byte) error) error { return fn(m.recs[k]) }

// buildFile chunks and stores one file, returning its content key.
func buildFile(t *testing.T, st *memStore, content []byte) key.Key {
	t.Helper()
	emit := st.emit(t)
	ib := fstree.NewFileIndexBuilder(chunkers.NewItemChunker(0))
	add := func(chunk []byte) error {
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}
		return ib.AddChild(emit, obj.Key, nil)
	}
	// Small min/max forces multi-chunk files at test sizes.
	opts := &chunkers.ByteOpts{MinSize: 2 << 10, NormalSize: 8 << 10, MaxSize: 16 << 10}
	if err := chunkers.SplitBytes(bytes.NewReader(content), opts, add); err != nil {
		t.Fatal(err)
	}
	if len(content) == 0 {
		if err := add(nil); err != nil {
			t.Fatal(err)
		}
	}
	k, err := ib.Finish(emit)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// buildDir stores a directory tree read from disk, returning its root key.
func buildDir(t *testing.T, st *memStore, dir string) key.Key {
	t.Helper()
	emit := st.emit(t)
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(0))
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, de := range des {
		full := filepath.Join(dir, de.Name())
		fi, err := os.Lstat(full)
		if err != nil {
			t.Fatal(err)
		}
		e := fstree.Entry{Name: []byte(de.Name())}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(full)
			if err != nil {
				t.Fatal(err)
			}
			e.Mode = 0o120777
			e.LinkTarget = []byte(target)
		case fi.IsDir():
			ck := buildDir(t, st, full)
			e.Mode = 0o040555
			e.ContentKey = ck[:]
		default:
			content, err := os.ReadFile(full)
			if err != nil {
				t.Fatal(err)
			}
			ck := buildFile(t, st, content)
			e.Mode = 0o100444
			if fi.Mode()&0o111 != 0 {
				e.Mode = 0o100555
			}
			e.ContentKey = ck[:]
		}
		if err := db.AddEntry(emit, e); err != nil {
			t.Fatal(err)
		}
	}
	k, err := db.Finish(emit)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// fixtureDir builds an on-disk tree exercising every NAR node kind: regular,
// executable, empty file, symlink, nested and empty directories, a
// multi-chunk incompressible file, and a compressible one.
func fixtureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world\n"), 0o644))
	must(os.WriteFile(filepath.Join(dir, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o755))
	must(os.WriteFile(filepath.Join(dir, "empty"), nil, 0o644))
	must(os.Symlink("hello.txt", filepath.Join(dir, "link")))
	must(os.MkdirAll(filepath.Join(dir, "sub", "deeper"), 0o755))
	must(os.MkdirAll(filepath.Join(dir, "emptydir"), 0o755))
	must(os.WriteFile(filepath.Join(dir, "sub", "deeper", "nested.txt"), []byte("nested\n"), 0o644))
	rnd := make([]byte, 100<<10) // multi-chunk, stays raw in the store
	if _, err := rand.Read(rnd); err != nil {
		t.Fatal(err)
	}
	must(os.WriteFile(filepath.Join(dir, "random.bin"), rnd, 0o644))
	must(os.WriteFile(filepath.Join(dir, "zeros.bin"), make([]byte, 64<<10), 0o644)) // compresses in the store
	return dir
}

func decompress(t *testing.T, b []byte) []byte {
	t.Helper()
	dec, err := zstd.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(dec); err != nil {
		t.Fatalf("decompressing stitched output: %v", err)
	}
	return out.Bytes()
}

// TestStitchedNarMatchesNixNarDumpPath is the golden test: the decompressed
// stitched output must be byte-identical to `nix nar dump-path`.
func TestStitchedNarMatchesNixNarDumpPath(t *testing.T) {
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix not on PATH")
	}
	dir := fixtureDir(t)
	want, err := exec.Command(nix, "nar", "dump-path", dir).Output()
	if err != nil {
		t.Fatalf("nix nar dump-path: %v", err)
	}

	st := newMemStore()
	root := buildDir(t, st, dir)
	var out bytes.Buffer
	if err := narzstd.Write(&out, root, st.get, st.viewRecord); err != nil {
		t.Fatal(err)
	}
	got := decompress(t, out.Bytes())
	if !bytes.Equal(got, want) {
		t.Fatalf("stitched NAR differs from nix nar dump-path: got %d bytes, want %d", len(got), len(want))
	}

	// Cross-check the hand-rolled stored-raw frames against libzstd's
	// decoder, not just klauspost's.
	if zstdBin, err := exec.LookPath("zstd"); err == nil {
		cmd := exec.Command(zstdBin, "-d", "-c")
		cmd.Stdin = bytes.NewReader(out.Bytes())
		cliOut, err := cmd.Output()
		if err != nil {
			t.Fatalf("zstd CLI rejected stitched stream: %v", err)
		}
		if !bytes.Equal(cliOut, want) {
			t.Fatalf("zstd CLI decompression differs: got %d bytes, want %d", len(cliOut), len(want))
		}
	}
}

// TestFileRoot covers a bare-file root (store paths can be single files).
func TestFileRoot(t *testing.T) {
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix not on PATH")
	}
	f := filepath.Join(t.TempDir(), "file.bin")
	content := bytes.Repeat([]byte("data!"), 10_000)
	if err := os.WriteFile(f, content, 0o644); err != nil {
		t.Fatal(err)
	}
	want, err := exec.Command(nix, "nar", "dump-path", f).Output()
	if err != nil {
		t.Fatalf("nix nar dump-path: %v", err)
	}

	st := newMemStore()
	root := buildFile(t, st, content)
	var out bytes.Buffer
	if err := narzstd.Write(&out, root, st.get, st.viewRecord); err != nil {
		t.Fatal(err)
	}
	if got := decompress(t, out.Bytes()); !bytes.Equal(got, want) {
		t.Fatalf("stitched NAR differs: got %d bytes, want %d", len(got), len(want))
	}
}

// TestVerbatimFrameReuse proves the point of the package: compressible blob
// payloads appear in the output as their stored zstd frames, verbatim.
func TestVerbatimFrameReuse(t *testing.T) {
	st := newMemStore()
	content := bytes.Repeat([]byte("abcdefgh"), 8<<10) // compresses well
	root := buildFile(t, st, content)

	var out bytes.Buffer
	if err := narzstd.Write(&out, root, st.get, st.viewRecord); err != nil {
		t.Fatal(err)
	}
	found := 0
	for k, rec := range st.recs {
		if k.Type() != key.Blob {
			continue
		}
		hdr, err := amberpack.ParseRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Flags&amberpack.FlagZstd == 0 {
			continue
		}
		frame := rec[amberpack.RecHeaderSize : amberpack.RecHeaderSize+int(hdr.Slen)]
		if bytes.Contains(out.Bytes(), frame) {
			found++
		}
	}
	if found == 0 {
		t.Fatal("no stored zstd frame found verbatim in the stitched output")
	}
}

func TestWrappedRoots(t *testing.T) {
	dir := t.TempDir()
	execf := filepath.Join(dir, "exec")
	if err := os.WriteFile(execf, []byte("#!/bin/sh\necho hi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink("/nix/store/somewhere", link); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{execf, link} {
		want := nixNarDump(t, p)
		st := newMemStore()
		root, err := narimport.Import(bytes.NewReader(want), st.emit(t))
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := narzstd.Write(&buf, root, st.get, st.viewRecord); err != nil {
			t.Fatal(err)
		}
		dec, err := zstd.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(dec)
		dec.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: stitched NAR differs from nix nar dump-path", p)
		}
	}
}

func nixNarDump(t *testing.T, path string) []byte {
	t.Helper()
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix not on PATH")
	}
	out, err := exec.Command(nix, "nar", "dump-path", path).Output()
	if err != nil {
		t.Fatalf("nix nar dump-path: %v", err)
	}
	return out
}
