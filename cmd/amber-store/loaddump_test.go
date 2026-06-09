package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadDump_RoundTrip ingests a tree to a pack file, loads it into a running
// daemon, then dumps the tar by root key and checks a known file is present.
func TestLoadDump_RoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hi there"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Build a pack file offline and capture the root key.
	out := filepath.Join(t.TempDir(), "tree.amberpack")
	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatal(err)
	}
	root := strings.TrimSpace(rootBuf.String())

	sock := startDaemon(t)

	// load the pack file into the daemon.
	app = newApp()
	if err := app.Run([]string{"amber-store", "load", "--socket", sock, out}); err != nil {
		t.Fatalf("load: %v", err)
	}

	// dump the tar to a file and verify it contains hello.txt with the content.
	tarPath := filepath.Join(t.TempDir(), "tree.tar")
	app = newApp()
	if err := app.Run([]string{"amber-store", "dump", "--socket", sock, "-o", tarPath, root}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	data, err := os.ReadFile(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("hi there")) {
		t.Errorf("dumped tar does not contain the file content")
	}
}
