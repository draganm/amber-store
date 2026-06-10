package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
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
