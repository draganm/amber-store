// Package userconfig reads and writes the per-user amber-store configuration:
// a JSON file holding the user identity recorded in references it creates.
// The JSON format leaves room for a future signing key.
package userconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotConfigured means no usable config exists; commands that create
// references refuse to run until `amber-store config-user NAME` is run.
var ErrNotConfigured = errors.New("no user configured — run 'amber-store config-user <name>' first")

// Config is the persisted user configuration.
type Config struct {
	User string `json:"user"`
}

// Path returns the config file location: $AMBER_STORE_CONFIG when set,
// otherwise <os.UserConfigDir>/amber-store/config.json.
func Path() (string, error) {
	if p := os.Getenv("AMBER_STORE_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolving user config dir: %w", err)
	}
	return filepath.Join(dir, "amber-store", "config.json"), nil
}

// Load reads the config; a missing file or empty user is ErrNotConfigured.
func Load() (Config, error) {
	p, err := Path()
	if err != nil {
		return Config{}, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, ErrNotConfigured
	}
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing %s: %w", p, err)
	}
	if cfg.User == "" {
		return Config{}, ErrNotConfigured
	}
	return cfg, nil
}

// Save writes the config, creating parent directories as needed.
func Save(cfg Config) error {
	p, err := Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, append(b, '\n'), 0o644)
}
