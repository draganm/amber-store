package main

import (
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/userconfig"
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
