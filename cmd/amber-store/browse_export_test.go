package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
)

func TestExportCmd_WritesFileAndRefusesOverwrite(t *testing.T) {
	fk := testKey(5)
	store := fakeStore{file: map[string][]byte{fk.String(): []byte("hello")}}
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")

	msg := exportCmd(context.Background(), store, false, fk, path)().(exportDoneMsg)
	if msg.err != nil {
		t.Fatalf("export: %v", msg.err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "hello" {
		t.Fatalf("file content = %q, want hello", got)
	}

	// Second export to the same path must refuse.
	msg2 := exportCmd(context.Background(), store, false, fk, path)().(exportDoneMsg)
	if msg2.err == nil {
		t.Fatal("expected overwrite refusal")
	}
}

func TestModel_ExportPromptDefaultName(t *testing.T) {
	root, dk := testKey(1), testKey(2)
	store := fakeStore{ls: map[string][]client.Entry{
		root.String(): {dirEntry("docs", dk)},
	}}
	m := newTestModel(store, root)
	mi, _ := m.Update(dirLoadedMsg{entries: store.ls[root.String()]})
	m = mi.(browseModel)

	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	m = mi.(browseModel)
	if m.mode != modeExport {
		t.Fatalf("mode = %v, want modeExport", m.mode)
	}
	if want := "docs.tar"; filepath.Base(m.input.Value()) != want {
		t.Fatalf("default export name = %q, want suffix %q", m.input.Value(), want)
	}

	// Cancel.
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mi.(browseModel)
	if m.mode != modeList {
		t.Fatalf("mode = %v, want modeList after esc", m.mode)
	}
}
