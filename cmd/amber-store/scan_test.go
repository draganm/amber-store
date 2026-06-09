package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanTree_CountsRegularFileBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "c.txt"), []byte("cee"), 0o644); err != nil { // 3
		t.Fatal(err)
	}

	files, bytes, err := scanTree(dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	if files != 3 {
		t.Errorf("files = %d, want 3", files)
	}
	if bytes != 14 {
		t.Errorf("bytes = %d, want 14", bytes)
	}
}

func TestScanTree_ExcludesSymlinks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "real"), []byte("1234"), 0o644); err != nil { // 4
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	files, bytes, err := scanTree(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if files != 1 || bytes != 4 {
		t.Errorf("scanTree = (%d files, %d bytes), want (1, 4)", files, bytes)
	}
}

func TestScanTree_EmptyDir(t *testing.T) {
	files, bytes, err := scanTree(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if files != 0 || bytes != 0 {
		t.Errorf("scanTree(empty) = (%d, %d), want (0, 0)", files, bytes)
	}
}
