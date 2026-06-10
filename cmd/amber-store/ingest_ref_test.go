package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
)

func TestIngestCreatesReference(t *testing.T) {
	configureTestUser(t, "ingester")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "my/backup")

	rec, err := client.New(sock).GetRef(context.Background(), "my/backup")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if k.String() != root {
		t.Fatalf("reference key = %s, want printed root %s", k.String(), root)
	}
	if rec.User != "ingester" {
		t.Fatalf("reference user = %q, want ingester", rec.User)
	}
	if rec.CreatedAt == 0 {
		t.Fatal("reference CreatedAt is zero")
	}
}

func TestIngestRefusesWithoutUserConfig(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	sock := startDaemon(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "name", src})
	if err == nil || !strings.Contains(err.Error(), "config-user") {
		t.Fatalf("ingest without config = %v, want config-user hint", err)
	}
}

func TestIngestRejectsBadName(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)
	src := t.TempDir()
	err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "bad@name", src})
	if err == nil || !strings.Contains(err.Error(), "@") {
		t.Fatalf("ingest with bad name = %v, want name error", err)
	}
}

func TestIngestOfflineTakesNoName(t *testing.T) {
	// Offline mode must NOT require a name or a user config.
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := filepath.Join(t.TempDir(), "t.amberpack")
	if err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "-o", pack, src}); err != nil {
		t.Fatalf("offline ingest: %v", err)
	}
	if _, err := os.Stat(pack); err != nil {
		t.Fatal(err)
	}
}
