package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
)

// startDaemon runs the daemon command in a goroutine on a temp store + socket and
// waits until the socket accepts connections. It returns the socket path.
func startDaemon(t *testing.T) string {
	t.Helper()
	storeDir := t.TempDir()
	// Keep the socket path short: a unix sun_path is capped at ~104 bytes on
	// macOS/BSD, and t.TempDir() embeds the (long) test name and can overflow it.
	// MkdirTemp("", ...) yields a short per-user path.
	sockDir, err := os.MkdirTemp("", "amber-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")

	app := newApp()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = app.RunContext(ctx, []string{"amber-store", "daemon", "--store", storeDir, "--socket", sock})
	}()
	t.Cleanup(func() { cancel(); <-done })

	// Wait for the socket file to appear and accept a connection.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			return sock
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("daemon socket did not appear")
	return ""
}

func TestDaemon_ServesIngest(t *testing.T) {
	sock := startDaemon(t)
	c := client.New(sock)

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

	stats, err := c.Ingest(context.Background(), &buf)
	if err != nil {
		t.Fatalf("Ingest against daemon command: %v", err)
	}
	if stats.ObjectsStored != 1 {
		t.Fatalf("stored = %d, want 1", stats.ObjectsStored)
	}
}
