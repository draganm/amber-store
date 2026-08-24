package gc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClosureDirRoundTrip(t *testing.T) {
	d, err := openClosureDir(filepath.Join(t.TempDir(), "closures"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	root := testRoot(t)
	if _, ok := d.read(root); ok {
		t.Error("read of absent closure succeeded")
	}
	tails := []uint64{3, 8, 1 << 40}
	if err := d.write(root, tails); err != nil {
		t.Fatal(err)
	}
	got, ok := d.read(root)
	if !ok || len(got) != 3 || got[2] != 1<<40 {
		t.Fatalf("read = %v, %v", got, ok)
	}
	roots, err := d.list()
	if err != nil || len(roots) != 1 || roots[0] != root {
		t.Fatalf("list = %v, %v", roots, err)
	}
	if err := d.remove(root); err != nil {
		t.Fatal(err)
	}
	if err := d.remove(root); err != nil {
		t.Errorf("removing an absent closure errored: %v", err)
	}
	if _, ok := d.read(root); ok {
		t.Error("read after remove succeeded")
	}
}

func TestClosureDirCorruptCountsAsAbsent(t *testing.T) {
	d, err := openClosureDir(filepath.Join(t.TempDir(), "closures"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	root := testRoot(t)
	if err := d.write(root, []uint64{1}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(d.file(root))
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if err := os.WriteFile(d.file(root), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.read(root); ok {
		t.Error("corrupt closure read as valid")
	}
}

func TestClosureDirSweepsTmpAndIgnoresJunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closures")
	if err := os.MkdirAll(filepath.Join(path, "tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(path, "tmp", "stage-123")
	if err := os.WriteFile(stale, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, ".DS_Store"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "nothex.tails"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := openClosureDir(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("tmp/ not swept at open")
	}
	roots, err := d.list()
	if err != nil || len(roots) != 0 {
		t.Errorf("list = %v, %v; junk should be ignored", roots, err)
	}
}

func TestClosureDirWipe(t *testing.T) {
	d, err := openClosureDir(filepath.Join(t.TempDir(), "closures"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	if err := d.write(testRoot(t), []uint64{1}); err != nil {
		t.Fatal(err)
	}
	if err := d.wipe(); err != nil {
		t.Fatal(err)
	}
	roots, err := d.list()
	if err != nil || len(roots) != 0 {
		t.Errorf("closures survive wipe: %v, %v", roots, err)
	}
}
