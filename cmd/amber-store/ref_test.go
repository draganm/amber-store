package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/client"
)

// configureTestUser points AMBER_STORE_CONFIG at a fresh file and writes a
// config for the given user, so commands that create references can run.
func configureTestUser(t *testing.T, user string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := os.WriteFile(p, []byte(`{"user":"`+user+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
}

// ingestTestTree ingests a small tree through the daemon and returns the
// printed root key (hex). It requires configureTestUser to have run.
func ingestTestTree(t *testing.T, sock, name string) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, name, src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return strings.TrimSpace(out.String())
}

func TestRefCreateShowLsRm(t *testing.T) {
	t.Skip("enabled by ingest NAME DIR task")
	configureTestUser(t, "tester")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "first")

	// create (re-point a second name at the same key)
	if err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "second/name", root}); err != nil {
		t.Fatalf("ref create: %v", err)
	}

	// show
	var showOut bytes.Buffer
	app := newApp()
	app.Writer = &showOut
	if err := app.Run([]string{"amber-store", "ref", "show", "--socket", sock, "second/name"}); err != nil {
		t.Fatalf("ref show: %v", err)
	}
	var shown struct {
		Name      string `json:"name"`
		Key       string `json:"key"`
		User      string `json:"user"`
		CreatedAt string `json:"created_at"`
		Signature string `json:"signature,omitempty"`
	}
	if err := json.Unmarshal(showOut.Bytes(), &shown); err != nil {
		t.Fatalf("ref show output not JSON: %v\n%s", err, showOut.String())
	}
	if shown.Name != "second/name" || shown.Key != root || shown.User != "tester" {
		t.Fatalf("ref show = %+v", shown)
	}

	// ls lists both, name order
	var lsOut bytes.Buffer
	app = newApp()
	app.Writer = &lsOut
	if err := app.Run([]string{"amber-store", "ref", "ls", "--socket", sock}); err != nil {
		t.Fatalf("ref ls: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(lsOut.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("ref ls printed %d lines, want 2:\n%s", len(lines), lsOut.String())
	}
	if !strings.HasPrefix(lines[0], "first") || !strings.HasPrefix(lines[1], "second/name") {
		t.Fatalf("ref ls order/content wrong:\n%s", lsOut.String())
	}

	// rm
	if err := newApp().Run([]string{"amber-store", "ref", "rm", "--socket", sock, "second/name"}); err != nil {
		t.Fatalf("ref rm: %v", err)
	}
	_, err := client.New(sock).GetRef(context.Background(), "second/name")
	if !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("after rm, GetRef = %v, want ErrRefNotFound", err)
	}
}

func TestRefCreateNeedsUserConfig(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	sock := startDaemon(t)
	err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "n", strings.Repeat("0", 64)})
	if err == nil || !strings.Contains(err.Error(), "config-user") {
		t.Fatalf("ref create without config = %v, want config-user hint", err)
	}
}
