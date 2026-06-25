package main

import (
	"bytes"
	"context"
	"io"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

func testKey(seed byte) key.Key {
	var h [32]byte
	h[0] = seed
	k, err := key.NewFromHash(key.Blob, 1, h)
	if err != nil {
		panic(err)
	}
	return k
}

type fakeStore struct {
	ls   map[string][]client.Entry
	file map[string][]byte
	tar  map[string][]byte
	refs []client.RefInfo
}

func (f fakeStore) Ls(_ context.Context, k key.Key, _ string) ([]client.Entry, error) {
	return f.ls[k.String()], nil
}
func (f fakeStore) File(_ context.Context, ck key.Key) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.file[ck.String()])), nil
}
func (f fakeStore) Tar(_ context.Context, k key.Key, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.tar[k.String()])), nil
}
func (f fakeStore) ListRefs(_ context.Context) ([]client.RefInfo, error) {
	return f.refs, nil
}

func dirEntry(name string, k key.Key) client.Entry {
	return client.Entry{Name: name, Mode: unix.S_IFDIR | 0o755, Key: k.String()}
}
func fileEntry(name string, k key.Key, size uint64) client.Entry {
	return client.Entry{Name: name, Mode: unix.S_IFREG | 0o644, Key: k.String(), Size: size}
}

func newTestModel(store browseStore, root key.Key) browseModel {
	m := newBrowseModel(context.Background(), store, "/tmp", 10<<20, root, "root")
	m.width, m.height = 80, 24
	return m
}

func TestModel_DescendAndAscend(t *testing.T) {
	root, sub := testKey(1), testKey(2)
	store := fakeStore{ls: map[string][]client.Entry{
		root.String(): {dirEntry("sub", sub)},
		sub.String():  {fileEntry("f", testKey(3), 5)},
	}}
	m := newTestModel(store, root)

	// Simulate the initial dir load.
	mi, _ := m.Update(dirLoadedMsg{entries: store.ls[root.String()]})
	m = mi.(browseModel)
	if len(m.entries) != 1 || m.entries[0].Name != "sub" {
		t.Fatalf("root entries wrong: %+v", m.entries)
	}

	// Enter the subdirectory.
	mi, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = mi.(browseModel)
	if cmd == nil {
		t.Fatal("expected a load command on descend")
	}
	mi, _ = m.Update(cmd().(dirLoadedMsg))
	m = mi.(browseModel)
	if len(m.stack) != 2 || m.stack[1].name != "sub" {
		t.Fatalf("stack after descend: %+v", m.stack)
	}
	if len(m.entries) != 1 || m.entries[0].Name != "f" {
		t.Fatalf("sub entries wrong: %+v", m.entries)
	}

	// Go back up.
	mi, cmd = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	m = mi.(browseModel)
	mi, _ = m.Update(cmd().(dirLoadedMsg))
	m = mi.(browseModel)
	if len(m.stack) != 1 {
		t.Fatalf("stack after ascend: %+v", m.stack)
	}
}

func TestModel_CursorClamp(t *testing.T) {
	root := testKey(1)
	store := fakeStore{ls: map[string][]client.Entry{
		root.String(): {dirEntry("a", testKey(2)), fileEntry("b", testKey(3), 1)},
	}}
	m := newTestModel(store, root)
	mi, _ := m.Update(dirLoadedMsg{entries: store.ls[root.String()]})
	m = mi.(browseModel)

	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp}) // already at 0
	m = mi.(browseModel)
	if m.cursor != 0 {
		t.Fatalf("cursor = %d, want 0", m.cursor)
	}
	for i := 0; i < 5; i++ {
		mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
		m = mi.(browseModel)
	}
	if m.cursor != 1 {
		t.Fatalf("cursor = %d, want 1 (clamped)", m.cursor)
	}
}

func TestModel_FileModeSwitchAndExit(t *testing.T) {
	root, fk := testKey(1), testKey(9)
	store := fakeStore{
		ls:   map[string][]client.Entry{root.String(): {fileEntry("f.bin", fk, 4)}},
		file: map[string][]byte{fk.String(): {0x00, 0x01, 0x02, 0xff}},
	}
	m := newTestModel(store, root)
	mi, _ := m.Update(dirLoadedMsg{entries: store.ls[root.String()]})
	m = mi.(browseModel)

	// Open the file.
	mi, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = mi.(browseModel)
	mi, _ = m.Update(cmd().(fileLoadedMsg))
	m = mi.(browseModel)
	if m.mode != modeFile {
		t.Fatalf("mode = %v, want modeFile", m.mode)
	}
	if m.file.mode != modeHex {
		t.Fatalf("file mode = %v, want modeHex", m.file.mode)
	}

	// Switch to text.
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	m = mi.(browseModel)
	if m.file.mode != modeText {
		t.Fatalf("file mode = %v, want modeText", m.file.mode)
	}

	// Exit back to the list.
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mi.(browseModel)
	if m.mode != modeList {
		t.Fatalf("mode = %v, want modeList after esc", m.mode)
	}
}

// In a file view, the same "back" gestures as the directory list (left/h, plus
// esc/backspace) must return to the list.
func TestModel_FileModeBackKeys(t *testing.T) {
	root, fk := testKey(1), testKey(9)
	store := fakeStore{
		ls:   map[string][]client.Entry{root.String(): {fileEntry("notes.txt", fk, 5)}},
		file: map[string][]byte{fk.String(): []byte("hello")},
	}
	for _, k := range []tea.KeyMsg{
		{Type: tea.KeyLeft},
		{Type: tea.KeyRunes, Runes: []rune{'h'}},
		{Type: tea.KeyBackspace},
		{Type: tea.KeyEsc},
	} {
		m := newTestModel(store, root)
		mi, _ := m.Update(dirLoadedMsg{entries: store.ls[root.String()]})
		m = mi.(browseModel)
		mi, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = mi.(browseModel)
		mi, _ = m.Update(cmd().(fileLoadedMsg))
		m = mi.(browseModel)
		if m.mode != modeFile {
			t.Fatalf("setup: mode = %v, want modeFile", m.mode)
		}
		mi, _ = m.Update(k)
		m = mi.(browseModel)
		if m.mode != modeList {
			t.Fatalf("key %v: mode = %v, want modeList", k, m.mode)
		}
	}
}
