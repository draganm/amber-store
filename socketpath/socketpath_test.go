package socketpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolve_FlagWins(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got, err := Resolve("/from/flag.sock"); err != nil || got != "/from/flag.sock" {
		t.Fatalf("Resolve(flag) = %q, %v; want /from/flag.sock", got, err)
	}
}

func TestResolve_EnvBeatsDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "/from/env.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got, err := Resolve(""); err != nil || got != "/from/env.sock" {
		t.Fatalf("Resolve(\"\") = %q, %v; want /from/env.sock", got, err)
	}
}

func TestResolve_XDGDefault(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	want := "/run/user/1000/amber-store.sock"
	if got, err := Resolve(""); err != nil || got != want {
		t.Fatalf("Resolve(\"\") = %q, %v; want %q", got, err, want)
	}
}

// Without XDG_RUNTIME_DIR the default is inside a private per-user
// directory and does not depend on TMPDIR.
func TestResolve_TmpFallbackIsInsidePrivateDir(t *testing.T) {
	t.Setenv("AMBER_STORE_SOCKET", "")
	for _, xdg := range []string{"", "relative/dir"} {
		t.Setenv("XDG_RUNTIME_DIR", xdg)
		t.Setenv("TMPDIR", t.TempDir())
		got, err := Resolve("")
		if err != nil {
			t.Fatalf("XDG_RUNTIME_DIR=%q: %v", xdg, err)
		}
		if want := filepath.Join(fallbackDir(), "sock"); got != want {
			t.Fatalf("XDG_RUNTIME_DIR=%q: Resolve = %q, want %q", xdg, got, want)
		}
		fi, err := os.Lstat(filepath.Dir(got))
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() || fi.Mode().Perm()&0o077 != 0 {
			t.Fatalf("fallback dir mode = %v, want a 0700 directory", fi.Mode())
		}
	}
}

func TestPreparePrivateDir(t *testing.T) {
	t.Run("creates", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "d")
		if err := preparePrivateDir(dir); err != nil {
			t.Fatal(err)
		}
		if err := preparePrivateDir(dir); err != nil {
			t.Fatalf("second call on our own dir: %v", err)
		}
	})
	t.Run("rejects symlink planted at the path", func(t *testing.T) {
		base := t.TempDir()
		real := filepath.Join(base, "real")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(base, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if err := preparePrivateDir(link); err == nil {
			t.Fatal("expected error for a symlink")
		}
	})
	t.Run("rejects regular file", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "f")
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := preparePrivateDir(p); err == nil {
			t.Fatal("expected error for a file")
		}
	})
	t.Run("rejects group or world accessible dir", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "open")
		if err := os.Mkdir(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o755); err != nil { // defeat umask
			t.Fatal(err)
		}
		if err := preparePrivateDir(p); err == nil {
			t.Fatal("expected error for a 0755 dir")
		}
	})
}
