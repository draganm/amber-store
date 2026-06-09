// Package socketpath resolves the unix-socket path that the daemon and its CLI
// clients agree on. Resolution order: explicit flag, AMBER_STORE_SOCKET env, then
// a platform default — $XDG_RUNTIME_DIR/amber-store.sock when XDG_RUNTIME_DIR is
// set and absolute (typical on Linux), otherwise os.TempDir()/amber-store-<uid>.sock.
// On macOS XDG_RUNTIME_DIR is unset, so os.TempDir() (the secure per-user $TMPDIR)
// is used; the per-uid suffix keeps the Linux /tmp fallback from colliding across
// users.
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
	return filepath.Join(os.TempDir(), perUserName())
}

func perUserName() string {
	return fmt.Sprintf("amber-store-%d.sock", os.Getuid())
}
