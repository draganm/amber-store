package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunPack_WritesOutputFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.tar")
	app := newApp()
	err := app.Run([]string{"amber-store", "pack", "-o", out, dir})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Errorf("output tar is empty")
	}
}

func TestRunPack_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	if err := app.Run([]string{"amber-store", "pack", f}); err == nil {
		t.Errorf("expected error packing a non-directory")
	}
}

func TestRunPack_RequiresExactlyOneArg(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "pack"}); err == nil {
		t.Errorf("expected error with no DIR argument")
	}
}
