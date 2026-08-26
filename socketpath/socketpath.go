// Package socketpath resolves the unix-socket path that the daemon and its CLI
// clients agree on. Resolution order: explicit flag, AMBER_STORE_SOCKET env, then
// a platform default — $XDG_RUNTIME_DIR/amber-store.sock when XDG_RUNTIME_DIR is
// set and absolute (typical on Linux), otherwise /tmp/amber-store-<uid>/sock.
//
// The fallback deliberately ignores TMPDIR: environments like nix-shell and
// direnv set a fresh per-shell TMPDIR, so a TMPDIR-derived default would make a
// daemon and a client started in different shells resolve different "default"
// paths and never rendezvous. /tmp is stable for every process of a user but
// world-writable, so the socket lives in a per-user directory that daemon and
// clients create 0700 and verify (real directory, owned by us, no group/other
// access) before use. Nobody else can plant a socket in it.
package socketpath

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// EnvVar is the environment variable that overrides the default socket path.
const EnvVar = "AMBER_STORE_SOCKET"

// Resolve returns the socket path given a possibly-empty flag value. It fails
// only when the /tmp fallback directory cannot be made private.
func Resolve(flag string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	if env := os.Getenv(EnvVar); env != "" {
		return env, nil
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "amber-store.sock"), nil
	}
	dir := fallbackDir()
	if err := preparePrivateDir(dir); err != nil {
		return "", fmt.Errorf("default socket directory %s: %w (set --socket or %s)", dir, err, EnvVar)
	}
	return filepath.Join(dir, "sock"), nil
}

func fallbackDir() string {
	return filepath.Join("/tmp", fmt.Sprintf("amber-store-%d", os.Getuid()))
}

// preparePrivateDir creates dir 0700 if absent and verifies it is a real
// directory (Lstat, so a planted symlink fails) owned by us and closed to
// group/other.
func preparePrivateDir(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("is not a directory")
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot determine owner")
	}
	if int(st.Uid) != os.Getuid() {
		return fmt.Errorf("is owned by uid %d, not by us (uid %d)", st.Uid, os.Getuid())
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("has mode %04o, must not be accessible by group or others", perm)
	}
	return nil
}
