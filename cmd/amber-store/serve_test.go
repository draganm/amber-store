package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

// writeServeFixtures writes an identity key and an allowed-keys file.
func writeServeFixtures(t *testing.T) (identityPath, allowedPath string) {
	t.Helper()
	dir := t.TempDir()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	identityPath = filepath.Join(dir, "identity")
	if err := os.WriteFile(identityPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	allowedPath = filepath.Join(dir, "allowed")
	if err := os.WriteFile(allowedPath, ssh.MarshalAuthorizedKey(sshPub), 0o644); err != nil {
		t.Fatal(err)
	}
	return identityPath, allowedPath
}

func TestServeRequiresFlags(t *testing.T) {
	identity, allowed := writeServeFixtures(t)
	cases := [][]string{
		{"amber-store", "serve", "--store", t.TempDir(), "--identity", identity},    // no allowed-keys
		{"amber-store", "serve", "--identity", identity, "--allowed-keys", allowed}, // no store
	}
	for _, args := range cases {
		if err := newApp().Run(args); err == nil {
			t.Fatalf("serve %v succeeded, want missing-flag error", args[2:])
		}
	}
}

// TestServeAutoIdentity starts serve without --identity and checks that the
// served identity matches the auto-generated <store>/identity.pub.
func TestServeAutoIdentity(t *testing.T) {
	_, allowed := writeServeFixtures(t)
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
			"amber-store", "serve", "--store", storeDir,
			"--allowed-keys", allowed, "--listen", addr, "--sync=false",
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

func TestServeRejectsTLSHalfConfig(t *testing.T) {
	identity, allowed := writeServeFixtures(t)
	err := newApp().Run([]string{
		"amber-store", "serve", "--store", t.TempDir(),
		"--identity", identity, "--allowed-keys", allowed,
		"--tls-cert", "/nonexistent/cert.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("err = %v, want tls flag pairing error", err)
	}
}

func TestServeRejectsEncryptedIdentityFile(t *testing.T) {
	_, allowed := writeServeFixtures(t)
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
		"amber-store", "serve", "--store", t.TempDir(),
		"--identity", encPath, "--allowed-keys", allowed,
	})
	if err == nil || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("err = %v, want agent-hint error for passphrase-protected key", err)
	}
}
