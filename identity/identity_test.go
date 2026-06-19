package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestLoadOrCreateRoundTrip(t *testing.T) {
	// A nested, not-yet-existing dir exercises the MkdirAll path.
	dir := filepath.Join(t.TempDir(), "store")
	s1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Fatal("second LoadOrCreate returned a different key")
	}

	info, err := os.Stat(filepath.Join(dir, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o, want 0600", info.Mode().Perm())
	}
	pubInfo, err := os.Stat(filepath.Join(dir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	if pubInfo.Mode().Perm() != 0o644 {
		t.Fatalf("identity.pub mode = %o, want 0644", pubInfo.Mode().Perm())
	}

	pubBytes, err := os.ReadFile(filepath.Join(dir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	pub, comment, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatalf("identity.pub is not a valid authorized_keys line: %v", err)
	}
	if comment != Comment {
		t.Fatalf("identity.pub comment = %q, want %q", comment, Comment)
	}
	if !bytes.Equal(pub.Marshal(), s1.PublicKey().Marshal()) {
		t.Fatal("identity.pub does not match the private key")
	}
	if pub.Type() != ssh.KeyAlgoED25519 {
		t.Fatalf("key type = %s, want %s", pub.Type(), ssh.KeyAlgoED25519)
	}
}

func TestLoadOrCreateRejectsCorruptKey(t *testing.T) {
	dir := t.TempDir()
	garbage := []byte("not a key")
	if err := os.WriteFile(filepath.Join(dir, "identity"), garbage, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("corrupt identity accepted")
	}
	// The bad file must survive untouched — never silently regenerated.
	got, err := os.ReadFile(filepath.Join(dir, "identity"))
	if err != nil || !bytes.Equal(got, garbage) {
		t.Fatalf("identity file was modified: %q, %v", got, err)
	}
}

func TestLoadOrCreateRejectsEncryptedKey(t *testing.T) {
	dir := t.TempDir()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "identity"), pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadOrCreate(dir)
	if err == nil || !strings.Contains(err.Error(), "passphrase") {
		t.Fatalf("err = %v, want passphrase-protected error", err)
	}
}

func TestLoadOrCreateHealsMissingPub(t *testing.T) {
	dir := t.TempDir()
	s1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	pubPath := filepath.Join(dir, "identity.pub")
	if err := os.Remove(pubPath); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatal(err)
	}
	pubBytes, err := os.ReadFile(pubPath)
	if err != nil {
		t.Fatalf("identity.pub not regenerated: %v", err)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pub.Marshal(), s1.PublicKey().Marshal()) {
		t.Fatal("regenerated identity.pub does not match the private key")
	}
}
