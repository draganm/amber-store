package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

// writeIdentityFixture writes an unencrypted SSH identity key file.
func writeIdentityFixture(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(t.TempDir(), "identity")
	if err := os.WriteFile(identityPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return identityPath
}

func TestServeRequiresStoreFlag(t *testing.T) {
	identity := writeIdentityFixture(t)
	if err := newApp().Run([]string{"amber-store", "serve", "--identity", identity}); err == nil {
		t.Fatal("serve without --store succeeded, want missing-flag error")
	}
}

// TestServeAutoIdentity starts serve without --identity and checks that the
// served identity matches the auto-generated <store>/identity.pub.
func TestServeAutoIdentity(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "store")

	// Reserve a port so the test can find the server.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- newApp().RunContext(ctx, []string{
			"amber-store", "serve", "--debug-listen", "", "--store", storeDir,
			"--listen", addr, "--sync=false",
		})
	}()

	var pubWire []byte
	deadline := time.Now().Add(10 * time.Second)
	for {
		pubWire, err = remoteclient.FetchIdentity(ctx, "http://"+addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not come up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	pubBytes, err := os.ReadFile(filepath.Join(storeDir, "identity.pub"))
	if err != nil {
		t.Fatalf("identity.pub not written: %v", err)
	}
	filePub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(filePub.Marshal(), pubWire) {
		t.Fatal("served identity does not match the generated identity.pub")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned: %v", err)
	}
}

// Daemons negotiate HTTP/2 when serve terminates TLS itself; the default
// 1 MiB upload flow-control windows cap a push at ~1 MiB per round trip on
// high-latency links, so the server must raise both receive windows. Current
// daemons stay on HTTP/1.1, but older ones still benefit.
func TestServeRaisesHTTP2UploadWindows(t *testing.T) {
	srv := newHTTPServer(http.NewServeMux(), slog.New(slog.DiscardHandler))
	if srv.HTTP2 == nil {
		t.Fatal("server has no HTTP2 config")
	}
	if srv.HTTP2.MaxReceiveBufferPerConnection <= 1<<20 {
		t.Fatalf("MaxReceiveBufferPerConnection = %d, want above the 1 MiB default", srv.HTTP2.MaxReceiveBufferPerConnection)
	}
	if srv.HTTP2.MaxReceiveBufferPerStream <= 1<<20 {
		t.Fatalf("MaxReceiveBufferPerStream = %d, want above the 1 MiB default", srv.HTTP2.MaxReceiveBufferPerStream)
	}
}

func TestServeRejectsTLSHalfConfig(t *testing.T) {
	identity := writeIdentityFixture(t)
	err := newApp().Run([]string{
		"amber-store", "serve", "--debug-listen", "", "--store", t.TempDir(),
		"--identity", identity,
		"--tls-cert", "/nonexistent/cert.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("err = %v, want tls flag pairing error", err)
	}
}

func TestServeRejectsEncryptedIdentityFile(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	encPath := filepath.Join(t.TempDir(), "enc-identity")
	if err := os.WriteFile(encPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	err = newApp().Run([]string{
		"amber-store", "serve", "--debug-listen", "", "--store", t.TempDir(),
		"--identity", encPath,
	})
	if err == nil || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("err = %v, want agent-hint error for passphrase-protected key", err)
	}
}
