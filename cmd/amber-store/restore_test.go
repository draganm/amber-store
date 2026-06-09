package main

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// fixedTime is the mtime stamped on every source entry so restoration can be
// compared to the nanosecond.
var fixedTime = time.Unix(1_700_000_000, 123_456_789)

func setTimes(t *testing.T, path string, symlink bool) {
	t.Helper()
	ts := unix.NsecToTimespec(fixedTime.UnixNano())
	flags := 0
	if symlink {
		flags = unix.AT_SYMLINK_NOFOLLOW
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{ts, ts}, flags); err != nil {
		t.Fatal(err)
	}
}

// buildSourceTree creates a representative tree: regular files (including a
// multi-chunk one), a subdirectory, a symlink, and a FIFO, each with explicit
// permissions and a fixed mtime. It returns the root directory.
func buildSourceTree(t *testing.T) string {
	t.Helper()
	src := t.TempDir()

	mustWrite := func(rel string, data []byte, perm os.FileMode) {
		p := filepath.Join(src, rel)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, perm); err != nil {
			t.Fatal(err)
		}
	}

	mustWrite("small.txt", []byte("tiny"), 0o600)
	mustWrite("big.bin", bytes.Repeat([]byte("0123456789"), 200000), 0o644) // ~2 MiB

	sub := filepath.Join(src, "sub")
	if err := os.Mkdir(sub, 0o750); err != nil {
		t.Fatal(err)
	}
	mustWrite(filepath.Join("sub", "nested.txt"), []byte("nested"), 0o640)

	if err := os.Symlink("big.bin", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(src, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Stamp a fixed mtime on every entry (children before their directories is
	// irrelevant: setting times never re-bumps a parent's mtime).
	setTimes(t, filepath.Join(src, "small.txt"), false)
	setTimes(t, filepath.Join(src, "big.bin"), false)
	setTimes(t, filepath.Join(src, "sub", "nested.txt"), false)
	setTimes(t, filepath.Join(src, "sub"), false)
	setTimes(t, filepath.Join(src, "link"), true)
	setTimes(t, filepath.Join(src, "pipe"), false)
	return src
}

// compareTrees asserts that dst reproduces src: same entries, types,
// permissions, mtimes, file contents, and symlink targets. Ownership and xattrs
// are not compared (ownership needs root; xattrs are covered separately).
func compareTrees(t *testing.T, src, dst string) {
	t.Helper()
	sents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	dents, err := os.ReadDir(dst)
	if err != nil {
		t.Fatalf("reading restored dir %s: %v", dst, err)
	}
	if len(sents) != len(dents) {
		t.Fatalf("%s: entry count src=%d dst=%d", src, len(sents), len(dents))
	}
	for _, de := range sents {
		name := de.Name()
		sp := filepath.Join(src, name)
		dp := filepath.Join(dst, name)
		si, err := os.Lstat(sp)
		if err != nil {
			t.Fatal(err)
		}
		di, err := os.Lstat(dp)
		if err != nil {
			t.Fatalf("restored entry missing: %s: %v", dp, err)
		}
		if si.Mode()&os.ModeType != di.Mode()&os.ModeType {
			t.Errorf("%s: type src=%v dst=%v", name, si.Mode(), di.Mode())
			continue
		}
		if si.ModTime().UnixNano() != di.ModTime().UnixNano() {
			t.Errorf("%s: mtime src=%d dst=%d", name, si.ModTime().UnixNano(), di.ModTime().UnixNano())
		}
		switch {
		case si.IsDir():
			if si.Mode().Perm() != di.Mode().Perm() {
				t.Errorf("%s: perm src=%v dst=%v", name, si.Mode().Perm(), di.Mode().Perm())
			}
			compareTrees(t, sp, dp)
		case si.Mode()&os.ModeSymlink != 0:
			st, _ := os.Readlink(sp)
			dt, _ := os.Readlink(dp)
			if st != dt {
				t.Errorf("%s: symlink target src=%q dst=%q", name, st, dt)
			}
		case si.Mode().IsRegular():
			if si.Mode().Perm() != di.Mode().Perm() {
				t.Errorf("%s: perm src=%v dst=%v", name, si.Mode().Perm(), di.Mode().Perm())
			}
			sb, err := os.ReadFile(sp)
			if err != nil {
				t.Fatal(err)
			}
			db, err := os.ReadFile(dp)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(sb, db) {
				t.Errorf("%s: content differs (src=%d dst=%d bytes)", name, len(sb), len(db))
			}
		case si.Mode()&os.ModeNamedPipe != 0:
			if si.Mode().Perm() != di.Mode().Perm() {
				t.Errorf("%s: fifo perm src=%v dst=%v", name, si.Mode().Perm(), di.Mode().Perm())
			}
		}
	}
}

// ingestViaDaemon builds src into a pack file, starts a daemon, loads the pack,
// and returns the daemon socket and the tree root key.
func ingestViaDaemon(t *testing.T, src string) (sock string, root key.Key) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "tree.amberpack")
	var rootBuf bytes.Buffer
	app := newApp()
	app.Writer = &rootBuf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(rootBuf.String()))
	if err != nil {
		t.Fatal(err)
	}
	root, err = key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	sock = startDaemon(t)
	app = newApp()
	if err := app.Run([]string{"amber-store", "load", "--socket", sock, out}); err != nil {
		t.Fatalf("load: %v", err)
	}
	return sock, root
}

func TestRestore_RoundTrip(t *testing.T) {
	src := buildSourceTree(t)
	sock, root := ingestViaDaemon(t, src)

	out := filepath.Join(t.TempDir(), "restored")
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root.String(), out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	compareTrees(t, src, out)
}

func TestRestore_Xattrs(t *testing.T) {
	src := t.TempDir()
	f := filepath.Join(src, "file")
	if err := os.WriteFile(f, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := []byte("hello world")
	if err := unix.Lsetxattr(f, "user.comment", want, 0); err != nil {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}

	sock, root := ingestViaDaemon(t, src)
	out := filepath.Join(t.TempDir(), "restored")
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "--socket", sock, root.String(), out}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := getXattr(t, filepath.Join(out, "file"), "user.comment")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("restored xattr = %q, want %q", got, want)
	}
}

// getXattr reads a single xattr value via unix.Lgetxattr.
func getXattr(t *testing.T, path, name string) ([]byte, error) {
	t.Helper()
	buf := make([]byte, 1024)
	n, err := unix.Lgetxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func TestRunRestore_RejectsBadKey(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "not-a-key", t.TempDir()}); err == nil {
		t.Errorf("expected error for malformed key")
	}
}

func TestRunRestore_RequiresTwoArgs(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "restore", "deadbeef"}); err == nil {
		t.Errorf("expected error with only one positional argument")
	}
}
