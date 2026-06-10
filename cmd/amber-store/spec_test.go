package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestFile writes content to dir/rel, creating parent directories.
func writeTestFile(dir, rel, content string) error {
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(content), 0o644)
}

// TestLsViaReference checks that `ls ref:NAME@PATH` equals `ls KEY/PATH`.
func TestLsViaReference(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)

	// Build a tree with a subdirectory to exercise @PATH.
	src := t.TempDir()
	if err := writeTestFile(src, "sub/inner.txt", "content"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "tree", src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out.String())

	run := func(spec string) string {
		var b bytes.Buffer
		a := newApp()
		a.Writer = &b
		if err := a.Run([]string{"amber-store", "ls", "--socket", sock, spec}); err != nil {
			t.Fatalf("ls %s: %v", spec, err)
		}
		return b.String()
	}

	if got, want := run("ref:tree"), run(root); got != want {
		t.Fatalf("ls ref:tree = %q, ls KEY = %q", got, want)
	}
	if got, want := run("ref:tree@sub"), run(root+"/sub"); got != want {
		t.Fatalf("ls ref:tree@sub = %q, ls KEY/sub = %q", got, want)
	}
}

func TestLsRefErrors(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)

	err := newApp().Run([]string{"amber-store", "ls", "--socket", sock, "ref:absent"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ls ref:absent = %v, want not-found error", err)
	}
	err = newApp().Run([]string{"amber-store", "ls", "--socket", sock, "ref:@path"})
	if err == nil {
		t.Fatal("ls ref:@path should fail: empty name")
	}
}
