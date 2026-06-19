package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/userconfig"
)

func TestConfigUserCommand(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user", "alice"}); err != nil {
		t.Fatalf("config-user: %v", err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "alice" {
		t.Fatalf("User = %q, want alice", cfg.User)
	}
}

func TestConfigUserRequiresOneArg(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user"}); err == nil {
		t.Fatal("expected error without NAME")
	}
}

func TestConfigUserRejectsEmptyName(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user", ""}); err == nil {
		t.Fatal("expected error for empty NAME")
	}
}

func TestConfigUserRejectsControlChar(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user", "a\nb"}); err == nil {
		t.Fatal("expected error for NAME containing newline")
	}
}

// writeTestPrivateKey writes an unencrypted ed25519 OpenSSH key, returns path.
func writeTestPrivateKey(t *testing.T) string {
	t.Helper()
	p, _ := writeSigningKey(t)
	return p
}

func TestConfigUserSetsSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "--signing-key", keyPath, "alice"}); err != nil {
		t.Fatalf("config-user: %v", err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SigningKey != keyPath {
		t.Fatalf("SigningKey = %q, want %q", cfg.SigningKey, keyPath)
	}
}

func TestConfigUserPreservesSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "--signing-key", keyPath, "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "bob"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "bob" || cfg.SigningKey != keyPath {
		t.Fatalf("cfg = %+v, want user bob with signing key preserved", cfg)
	}
}

func TestConfigUserClearsSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "--signing-key", keyPath, "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "--signing-key", "", "alice"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SigningKey != "" {
		t.Fatalf("SigningKey = %q, want cleared", cfg.SigningKey)
	}
}

func TestConfigUserRejectsBadSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	bad := filepath.Join(t.TempDir(), "garbage")
	if err := os.WriteFile(bad, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "--signing-key", bad, "alice"}); err == nil {
		t.Fatal("expected error for non-key signing-key file")
	}
}
