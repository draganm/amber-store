package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		{"amber-store", "serve", "--store", t.TempDir(), "--allowed-keys", allowed}, // no identity
		{"amber-store", "serve", "--store", t.TempDir(), "--identity", identity},    // no allowed-keys
		{"amber-store", "serve", "--identity", identity, "--allowed-keys", allowed}, // no store
	}
	for _, args := range cases {
		if err := newApp().Run(args); err == nil {
			t.Fatalf("serve %v succeeded, want missing-flag error", args[2:])
		}
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
