package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestParseRemoteKeyFlags(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	def, overrides, err := parseRemoteKeys([]string{keyPath, "origin=" + keyPath})
	if err != nil {
		t.Fatal(err)
	}
	if def == nil {
		t.Fatal("default signer not loaded")
	}
	if _, ok := overrides["origin"]; !ok {
		t.Fatal("origin override not loaded")
	}
	var _ ssh.Signer = def
}

func TestParseRemoteKeysRejectsTwoDefaults(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	_, _, err := parseRemoteKeys([]string{keyPath, keyPath})
	if err == nil || !strings.Contains(err.Error(), "default") {
		t.Fatalf("err = %v, want duplicate-default error", err)
	}
}

func TestParseRemoteKeysRejectsBadPath(t *testing.T) {
	if _, _, err := parseRemoteKeys([]string{"/nonexistent/key"}); err == nil {
		t.Fatal("want error for unreadable key")
	}
}

func TestParseRemoteKeysRejectsDuplicateOverride(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	_, _, err := parseRemoteKeys([]string{"origin=" + keyPath, "origin=" + keyPath})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("err = %v, want duplicate-override error", err)
	}
}

func TestDefaultRemoteSignerAutoGenerates(t *testing.T) {
	dir := t.TempDir()
	s1, err := defaultRemoteSigner(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "identity.pub")); err != nil {
		t.Fatalf("identity.pub not written: %v", err)
	}
	s2, err := defaultRemoteSigner(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Fatal("auto identity not stable across restarts")
	}
}

func TestDefaultRemoteSignerKeepsConfigured(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	configured := signerFromKeyPath(t, keyPath)
	dir := t.TempDir()
	got, err := defaultRemoteSigner(configured, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.PublicKey().Marshal(), configured.PublicKey().Marshal()) {
		t.Fatal("configured signer was replaced")
	}
	if _, err := os.Stat(filepath.Join(dir, "identity")); !os.IsNotExist(err) {
		t.Fatal("identity file generated despite a configured key")
	}
}
