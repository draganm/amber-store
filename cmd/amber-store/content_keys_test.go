package main

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/key"
)

// TestContentKeys_ListsReachableKeys ingests a small tree and checks that
// content-keys prints one hex key per line, starting with the root, and that
// every printed key parses. With KEY/PATH the listing is rooted at the
// subdirectory and excludes the root.
func TestContentKeys_ListsReachableKeys(t *testing.T) {
	src := t.TempDir()
	if err := os.Mkdir(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "top.txt"), []byte("top level"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "nested.txt"), []byte("nested content"), 0o644); err != nil {
		t.Fatal(err)
	}

	sock, root := ingestViaDaemon(t, src)

	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "content-keys", "--socket", sock, root.String()}); err != nil {
		t.Fatalf("content-keys: %v", err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	// root leaf, top.txt blob, sub leaf, nested.txt blob
	if len(lines) != 4 {
		t.Fatalf("got %d keys, want 4:\n%s", len(lines), out.String())
	}
	if lines[0] != root.String() {
		t.Errorf("first key = %q, want the root %s", lines[0], root)
	}
	all := map[string]bool{}
	for _, l := range lines {
		raw, err := hex.DecodeString(l)
		if err != nil {
			t.Fatalf("line %q is not hex: %v", l, err)
		}
		if _, err := key.Parse(raw); err != nil {
			t.Fatalf("line %q is not a valid key: %v", l, err)
		}
		all[l] = true
	}

	// The subdirectory's listing is a strict subset that excludes the root.
	out.Reset()
	app = newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "content-keys", "--socket", sock, root.String() + "/sub"}); err != nil {
		t.Fatalf("content-keys sub: %v", err)
	}
	subLines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(subLines) != 2 {
		t.Fatalf("got %d sub keys, want 2:\n%s", len(subLines), out.String())
	}
	for _, l := range subLines {
		if !all[l] {
			t.Errorf("sub key %s not in the full listing", l)
		}
		if l == root.String() {
			t.Errorf("sub listing must not contain the root key")
		}
	}
}

func TestRunContentKeys_RejectsBadKey(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "content-keys", "not-a-key"}); err == nil {
		t.Errorf("expected error for a malformed key")
	}
}

func TestRunContentKeys_RequiresOneArg(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "content-keys", "deadbeef", "extra"}); err == nil {
		t.Errorf("expected error with two positional arguments")
	}
}
