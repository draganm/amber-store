package socketpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolve_FlagWins(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got := Resolve("/from/flag.sock"); got != "/from/flag.sock" {
		t.Fatalf("Resolve(flag) = %q, want /from/flag.sock", got)
	}
}

func TestResolve_EnvBeatsDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got := Resolve(""); got != "/from/env.sock" {
		t.Fatalf("Resolve(\"\") = %q, want /from/env.sock", got)
	}
}

func TestResolve_XDGDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	want := "/run/user/1000/amber-store.sock"
	if got := Resolve(""); got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}

func TestResolve_TempDirFallback(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "") // unset -> fall back to os.TempDir (macOS path)
	got := Resolve("")
	want := filepath.Join(os.TempDir(), perUserName())
	if got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}

func TestResolve_IgnoresRelativeXDG(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "relative/dir") // not absolute -> ignored
	got := Resolve("")
	want := filepath.Join(os.TempDir(), perUserName())
	if got != want {
		t.Fatalf("Resolve(\"\") = %q, want %q", got, want)
	}
}
