package main

import (
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
