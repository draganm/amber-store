package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/refstore"
)

// startDaemon brings up the daemon HTTP handler directly on a fresh unix socket
// (no CLI), returning the socket path. It deliberately avoids newApp().Run so it
// can run concurrently with the cli client commands under test without racing on
// urfave/cli's package-global flags. The socket is bound before this returns, so
// clients may connect immediately.
func startDaemon(t *testing.T) string {
	t.Helper()
	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	refs, err := refstore.Open(filepath.Join(t.TempDir(), "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })

	// Keep the socket path short: a unix sun_path is capped at ~104 bytes on
	// macOS/BSD, and t.TempDir() embeds the (long) test name and can overflow it.
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
	srv := &http.Server{Handler: daemon.New(store, refs, nil)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// TestDaemon_ServesIngest exercises the real cli `daemon` command: it launches it
// in a goroutine (the only cli.App.Run in this test) and drives it with the
// direct client (not cli), so there is no concurrent cli flag parsing.
func TestDaemon_ServesIngest(t *testing.T) {
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

	// Wait for the daemon to bind the socket.
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

	o, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w := amberpack.NewWriter(&buf)
	if err := w.Add(o); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats, err := client.New(sock).Ingest(context.Background(), &buf)
	if err != nil {
		t.Fatalf("Ingest against daemon command: %v", err)
	}
	if stats.ObjectsStored != 1 {
		t.Fatalf("stored = %d, want 1", stats.ObjectsStored)
	}
}
