package main

import (
	"bytes"
	"context"
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
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
)

// startGCDaemon is startDaemon with a collector wired in, so the gc CLI
// commands have live routes to talk to.
func startGCDaemon(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	store, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	coll, err := gc.Open(filepath.Join(dir, "closures"), store, refs, gc.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	sockDir, err := os.MkdirTemp("", "amber-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.New(store, refs, nil, daemon.WithCollector(coll))}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// runGCApp runs one gc CLI invocation and returns its output.
func runGCApp(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run(append([]string{"amber-store"}, args...)); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return out.String()
}

// TestDaemonCommandOpensCollector exercises the real cli `daemon` command:
// gc is on by default, so GET /v1/gc answers with a report, not a 503.
func TestDaemonCommandOpensCollector(t *testing.T) {
	storeDir := t.TempDir()
	sockDir, err := os.MkdirTemp("", "amber-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = newApp().RunContext(ctx, []string{"amber-store", "daemon", "--store", storeDir, "--socket", sock})
	}()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon socket did not appear")
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, err := client.New(sock).GCStatus(context.Background())
	if err != nil {
		t.Fatalf("GCStatus against daemon command: %v", err)
	}
	if st.Refs != 0 || st.Union != 0 {
		t.Fatalf("fresh store status = %+v, want empty", st)
	}
}

func TestGCCommandsEndToEnd(t *testing.T) {
	configureTestUser(t, "tester")
	sock := startGCDaemon(t)
	root := ingestTestTree(t, sock, "keep")

	out := runGCApp(t, "gc", "why", "--socket", sock, root)
	if !strings.Contains(out, "keep") {
		t.Fatalf("gc why = %q, want it to name the reference keep", out)
	}
	out = runGCApp(t, "gc", "status", "--socket", sock)
	if !strings.Contains(out, "1 refs, 1 closures") {
		t.Fatalf("gc status = %q, want 1 ref and 1 closure", out)
	}
	out = runGCApp(t, "gc", "run", "--socket", sock)
	if !strings.Contains(out, "packs scored") && !strings.Contains(out, "skipped") {
		t.Fatalf("gc run = %q, want cycle stats or a skip", out)
	}
	if err := newApp().Run([]string{"amber-store", "ref", "rm", "--socket", sock, "keep"}); err != nil {
		t.Fatal(err)
	}
	out = runGCApp(t, "gc", "why", "--socket", sock, root)
	if !strings.Contains(out, "unreferenced") {
		t.Fatalf("gc why after rm = %q, want unreferenced", out)
	}
}
