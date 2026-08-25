package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
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

// startGCDaemon brings up the daemon handler with a small-segment store and
// a millisecond-grace collector, so the GC lifecycle is testable without
// waiting out the production grace period. Returns the socket path.
func startGCDaemon(t *testing.T) string {
	t.Helper()
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false), packstore.WithSegmentSize(4096))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	coll, err := gc.Open(filepath.Join(t.TempDir(), "closures"), store, refs, gc.Options{Grace: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	sockDir, err := os.MkdirTemp("", "amber-gc-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.New(store, refs, coll, nil)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// TestEndToEnd_GC drives the whole lifecycle through the CLI: ingest, a
// re-ingest that orphans the first tree, gc status / run / why, and a
// restore proving the referenced tree survived the sweep intact.
func TestEndToEnd_GC(t *testing.T) {
	configureTestUser(t, "gce2e")
	sock := startGCDaemon(t)

	run := func(args ...string) string {
		t.Helper()
		var buf bytes.Buffer
		app := newApp()
		app.Writer = &buf
		if err := app.Run(append([]string{"amber-store"}, args...)); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return buf.String()
	}

	// One 32 KiB incompressible file: rewriting it orphans a pack's worth
	// of unique blobs.
	src := t.TempDir()
	data := make([]byte, 32<<10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	root1 := strings.TrimSpace(run("ingest", "--no-progress", "--socket", sock, "gce2e/tree", src))

	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	root2 := strings.TrimSpace(run("ingest", "--no-progress", "--socket", sock, "gce2e/tree", src))
	if root1 == root2 {
		t.Fatal("rewriting the file did not change the root")
	}

	if out := run("gc", "status", "--socket", sock); !strings.Contains(out, "live") {
		t.Errorf("gc status output %q missing totals", out)
	}

	time.Sleep(50 * time.Millisecond) // sealed packs age past the 1ms grace
	out := run("gc", "run", "--socket", sock, "--garbage", "0")
	if !strings.Contains(out, "reaped") {
		t.Errorf("gc run output %q missing summary", out)
	}

	if out := run("gc", "why", "--socket", sock, root2); !strings.Contains(out, "gce2e/tree") {
		t.Errorf("gc why on the live root = %q, want gce2e/tree", out)
	}
	if out := run("gc", "why", "--socket", sock, root1); !strings.Contains(out, "unreferenced") {
		t.Errorf("gc why on the orphaned root = %q, want unreferenced", out)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	run("restore", "--socket", sock, "ref:gce2e/tree", restored)
	got, err := os.ReadFile(filepath.Join(restored, "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("restored content differs after the sweep")
	}
}
