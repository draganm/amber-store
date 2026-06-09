// Package socketpath resolves the unix-socket path that the daemon and its CLI
// clients agree on. Resolution order: explicit flag, AMBER_STORE_SOCKET env, then
// a platform default — $XDG_RUNTIME_DIR/amber-store.sock when XDG_RUNTIME_DIR is
// set and absolute (typical on Linux), otherwise /tmp/amber-store-<uid>.sock.
//
// The fallback deliberately ignores TMPDIR: environments like nix-shell and
// direnv set a fresh per-shell TMPDIR, so a TMPDIR-derived default would make a
// daemon and a client started in different shells resolve different "default"
// paths and never rendezvous. /tmp is stable for every process of a user; the
// per-uid suffix keeps it from colliding across users, and the socket's file
// mode (no write permission for group/other) keeps other users from connecting.
package socketpath

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnvVar is the environment variable that overrides the default socket path.
const EnvVar = "AMBER_STORE_SOCKET"

// Resolve returns the socket path given a possibly-empty flag value.
func Resolve(flag string) string {
	if flag != "" {
		return flag
	}
	if env := os.Getenv(EnvVar); env != "" {
		return env
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "amber-store.sock")
	}
	return filepath.Join("/tmp", perUserName())
}

func perUserName() string {
	return fmt.Sprintf("amber-store-%d.sock", os.Getuid())
}
