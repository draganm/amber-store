package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
)

// TestEndToEnd_IngestStreamThenRestore runs the full path: a daemon owns a store,
// `ingest` streams a tree to it over the socket, and `restore` reconstructs the
// tree faithfully from a `dump`-equivalent tar.
func TestEndToEnd_IngestStreamThenRestore(t *testing.T) {
	configureTestUser(t, "e2e")
	src := buildSourceTree(t)
	sock := startDaemon(t)

	// Stream-ingest (no --output): the client builds and uploads to the daemon,
	// printing the root to its writer.
	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "e2e/tree", src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(rootBuf.String())

	out := filepath.Join(t.TempDir(), "restored")
	app = newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root, out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	compareTrees(t, src, out)
}

// TestEndToEnd_IngestFile ingests a single file over the socket, then fetches it
// back through the client: the ref resolves to a file key and the bytes round-trip.
func TestEndToEnd_IngestFile(t *testing.T) {
	configureTestUser(t, "e2efile")
	sock := startDaemon(t)

	content := []byte("the quick brown fox jumps over the lazy dog\n")
	src := filepath.Join(t.TempDir(), "note.txt")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}

	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "e2efile/note", src}); err != nil {
		t.Fatalf("ingest file: %v", err)
	}

	raw, err := hex.DecodeString(strings.TrimSpace(rootBuf.String()))
	if err != nil {
		t.Fatalf("root hex: %v", err)
	}
	k, err := key.Parse(raw)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	if !isFileKey(k) {
		t.Fatalf("file ingest root has type %v, want a file key", k.Type())
	}

	rc, err := client.New(sock).File(context.Background(), k)
	if err != nil {
		t.Fatalf("fetch file: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch:\n got %q\nwant %q", got, content)
	}
}
