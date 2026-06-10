package userconfig_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/userconfig"
)

func TestLoadMissingIsErrNotConfigured(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if _, err := userconfig.Load(); !errors.Is(err, userconfig.ErrNotConfigured) {
		t.Fatalf("Load = %v, want ErrNotConfigured", err)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "deep", "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := userconfig.Save(userconfig.Config{User: "dragan"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "dragan" {
		t.Fatalf("User = %q, want dragan", cfg.User)
	}
}

func TestLoadEmptyUserIsErrNotConfigured(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := os.WriteFile(p, []byte(`{"user":""}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := userconfig.Load(); !errors.Is(err, userconfig.ErrNotConfigured) {
		t.Fatalf("Load = %v, want ErrNotConfigured", err)
	}
}

func TestLoadMalformedJSONError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := os.WriteFile(p, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := userconfig.Load()
	if err == nil {
		t.Fatal("Load = nil, want error for malformed JSON")
	}
	if errors.Is(err, userconfig.ErrNotConfigured) {
		t.Fatalf("Load = ErrNotConfigured, want a parse error")
	}
	if !strings.Contains(err.Error(), p) {
		t.Fatalf("error %q does not mention the config path %q", err.Error(), p)
	}
}
