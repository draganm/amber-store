package main

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDump_SubdirPath dumps a subdirectory of the tree by appending its path
// to the key (KEY/PATH) and checks the tar is rooted at that subdirectory.
func TestDump_SubdirPath(t *testing.T) {
	src := t.TempDir()
	if err := os.Mkdir(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "top.txt"), []byte("top level"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "nested.txt"), []byte("nested content"), 0o644); err != nil {
		t.Fatal(err)
	}

	sock, root := ingestViaDaemon(t, src)

	tarPath := filepath.Join(t.TempDir(), "sub.tar")
	app := newApp()
	if err := app.Run([]string{"amber-store", "dump", "--socket", sock, "-o", tarPath, root.String() + "/sub"}); err != nil {
		t.Fatalf("dump sub: %v", err)
	}
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := map[string]string{}
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if len(got) != 1 || got["nested.txt"] != "nested content" {
		t.Fatalf("tar = %v, want only nested.txt", got)
	}
}

func TestRunDump_RejectsTooManyArgs(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "dump", "deadbeef", "sub"}); err == nil {
		t.Errorf("expected error with two positional arguments")
	}
}

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
