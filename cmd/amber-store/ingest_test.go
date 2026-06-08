package main

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
)

// TestIngestObjects_ParityWithPack asserts that ingesting a directory stores
// exactly the objects pack would emit: every tar member is retrievable from the
// store by key with identical bytes, and the resolved root matches pack's root.
func TestIngestObjects_ParityWithPack(t *testing.T) {
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

	// Reference: pack the same tree to a tar.
	var buf bytes.Buffer
	packRoot, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	members, _ := readTar(t, buf.Bytes())

	// Ingest the same tree into a diskstore.
	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var root key.Key
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, &root)
	if err := store.WriteBatch(seq); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if root != packRoot {
		t.Fatalf("ingest root = %s, want pack root %s", root, packRoot)
	}

	for name, want := range members {
		raw, err := hex.DecodeString(name)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("object %s: stored bytes differ from packed bytes", k)
		}
	}
}

// TestRunIngest_StoresRoot drives the CLI ingest command end to end and checks
// that the resulting store contains the root object pack would produce.
func TestRunIngest_StoresRoot(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()

	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", "--store", storeDir, src}); err != nil {
		t.Fatal(err)
	}

	// The root pack would produce must be present in the store.
	var buf bytes.Buffer
	root, err := pack(src, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	store, err := diskstore.Open(storeDir, diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	has, err := store.Has(root)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("store is missing root object %s", root)
	}
}

func TestRunIngest_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", "--store", t.TempDir(), f}); err == nil {
		t.Errorf("expected error ingesting a non-directory")
	}
}

func TestRunIngest_RequiresStoreFlag(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", t.TempDir()}); err == nil {
		t.Errorf("expected error without --store flag")
	}
}
