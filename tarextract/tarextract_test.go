package tarextract_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/tarextract"
)

func TestExtract_FilesDirsAndDeferredDirMeta(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 222_000_000)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(h *tar.Header, body string) {
		h.Format = tar.FormatPAX
		h.ModTime = mtime
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A read-only directory listed BEFORE its child file. If dir mode were applied
	// immediately, writing the child would fail; deferral makes it work.
	write(&tar.Header{Name: "ro/", Typeflag: tar.TypeDir, Mode: 0o500}, "")
	write(&tar.Header{Name: "ro/child.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5}, "hello")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "ro", "child.txt"))
	if err != nil {
		t.Fatalf("reading child: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("child content = %q, want hello", got)
	}
	di, err := os.Lstat(filepath.Join(dest, "ro"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o500 {
		t.Errorf("dir perm = %v, want 0500 (applied after children)", di.Mode().Perm())
	}
	if di.ModTime().UnixNano() != mtime.UnixNano() {
		t.Errorf("dir mtime = %d, want %d", di.ModTime().UnixNano(), mtime.UnixNano())
	}
	// Restore write permission so t.TempDir() cleanup can remove the directory.
	os.Chmod(filepath.Join(dest, "ro"), 0o700)
}

func TestExtract_RejectsUnsafeName(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	h := &tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1, Format: tar.FormatPAX}
	if err := tw.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	tw.Write([]byte("x"))
	tw.Close()

	dest := filepath.Join(t.TempDir(), "out")
	if err := tarextract.Extract(&buf, dest); err == nil {
		t.Fatalf("expected error extracting an escaping path")
	}
}
