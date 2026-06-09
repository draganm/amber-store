package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
)

func TestModeString(t *testing.T) {
	cases := []struct {
		mode uint64
		want string
	}{
		{0o100644, "-rw-r--r--"},
		{0o100755, "-rwxr-xr-x"},
		{0o040750, "drwxr-x---"},
		{0o120777, "lrwxrwxrwx"},
		{0o010644, "prw-r--r--"},
		{0o140644, "srw-r--r--"},
		{0o020660, "crw-rw----"},
		{0o060660, "brw-rw----"},
		{0o104755, "-rwsr-xr-x"}, // setuid with user x
		{0o104655, "-rwSr-xr-x"}, // setuid without user x
		{0o102675, "-rw-rwsr-x"}, // setgid with group x
		{0o102645, "-rw-r-Sr-x"}, // setgid without group x
		{0o101777, "-rwxrwxrwt"}, // sticky with other x
		{0o101776, "-rwxrwxrwT"}, // sticky without other x
	}
	for _, c := range cases {
		if got := modeString(c.mode); got != c.want {
			t.Errorf("modeString(%#o) = %q, want %q", c.mode, got, c.want)
		}
	}
}

func TestFormatMtime(t *testing.T) {
	now := time.Date(2024, 6, 15, 12, 0, 0, 0, time.Local)
	recent := time.Date(2024, 6, 1, 10, 30, 0, 0, time.Local)
	old := time.Date(2020, 1, 2, 10, 30, 0, 0, time.Local)
	future := time.Date(2025, 1, 2, 10, 30, 0, 0, time.Local)

	if got := formatMtime(recent, now); got != "Jun  1 10:30" {
		t.Errorf("recent = %q, want %q", got, "Jun  1 10:30")
	}
	if got := formatMtime(old, now); got != "Jan  2  2020" {
		t.Errorf("old = %q, want %q", got, "Jan  2  2020")
	}
	if got := formatMtime(future, now); got != "Jan  2  2025" {
		t.Errorf("future = %q, want %q", got, "Jan  2  2025")
	}
}

func TestRenderLs(t *testing.T) {
	now := time.Date(2024, 6, 15, 12, 0, 0, 0, time.Local)
	mtime := time.Date(2024, 6, 1, 10, 30, 0, 0, time.Local).UnixNano()
	entries := []client.Entry{
		{Name: "a.txt", Mode: 0o100644, UID: 501, GID: 20, MtimeNs: mtime, Size: 5, Key: "aa"},
		{Name: "link", Mode: 0o120777, UID: 0, GID: 0, MtimeNs: mtime, Size: 5, LinkTarget: "a.txt"},
		{Name: "sub", Mode: 0o040755, UID: 1000, GID: 1000, MtimeNs: mtime, Size: 12345, Key: "bb"},
	}

	var buf bytes.Buffer
	if err := renderLs(&buf, entries, now, false); err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"-rw-r--r--  501   20     5 Jun  1 10:30 a.txt",
		"lrwxrwxrwx    0    0     5 Jun  1 10:30 link -> a.txt",
		"drwxr-xr-x 1000 1000 12345 Jun  1 10:30 sub",
		"",
	}, "\n")
	if buf.String() != want {
		t.Errorf("renderLs output:\n%s\nwant:\n%s", buf.String(), want)
	}

	// With keys enabled the content key is appended after the name.
	buf.Reset()
	if err := renderLs(&buf, entries, now, true); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if !strings.HasSuffix(lines[0], "a.txt aa") {
		t.Errorf("line with key = %q, want suffix %q", lines[0], "a.txt aa")
	}
	if !strings.HasSuffix(lines[1], "link -> a.txt") {
		t.Errorf("keyless entry line = %q, want no key suffix", lines[1])
	}
}

func TestLs_ListsDaemonDirectory(t *testing.T) {
	sock := startDaemon(t)

	content, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	subLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("nested.txt"), Mode: 0o100600, Mtime: time.Now().UnixNano(), ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, UID: 501, GID: 20, Mtime: time.Now().UnixNano(), ContentKey: content.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, Mtime: time.Now().UnixNano(), LinkTarget: []byte("a.txt")},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: time.Now().UnixNano(), ContentKey: subLeaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	var pack bytes.Buffer
	w := amberpack.NewWriter(&pack)
	for _, o := range []fstree.Object{content, subLeaf, leaf} {
		if err := w.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.New(sock).Ingest(context.Background(), &pack); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ls", "--socket", sock, leaf.Key.String()}); err != nil {
		t.Fatalf("ls: %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3:\n%s", len(lines), out.String())
	}
	if !strings.HasPrefix(lines[0], "-rw-r--r--") || !strings.HasSuffix(lines[0], " a.txt") {
		t.Errorf("file line = %q", lines[0])
	}
	if !strings.Contains(lines[0], " 5 ") {
		t.Errorf("file line %q missing size 5", lines[0])
	}
	if !strings.HasPrefix(lines[1], "lrwxrwxrwx") || !strings.HasSuffix(lines[1], " link -> a.txt") {
		t.Errorf("link line = %q", lines[1])
	}

	// With a PATH argument the subdirectory is listed instead of the root.
	out.Reset()
	app = newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ls", "--socket", sock, leaf.Key.String(), "sub"}); err != nil {
		t.Fatalf("ls sub: %v", err)
	}
	lines = strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 1 || !strings.HasSuffix(lines[0], " nested.txt") {
		t.Fatalf("ls sub output:\n%s", out.String())
	}

	// A missing path is an error.
	app = newApp()
	app.Writer = io.Discard
	if err := app.Run([]string{"amber-store", "ls", "--socket", sock, leaf.Key.String(), "nope"}); err == nil {
		t.Fatal("expected error for a missing path")
	}
}
