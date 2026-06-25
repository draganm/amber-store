package main

import (
	"context"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
)

func TestModel_RefPickerFilterAndSelect(t *testing.T) {
	ka, kb := testKey(10), testKey(11)
	store := fakeStore{
		refs: []client.RefInfo{
			{Name: "alpha", Key: ka.String(), User: "u"},
			{Name: "beta", Key: kb.String(), User: "u"},
		},
		ls: map[string][]client.Entry{
			ka.String(): {fileEntry("f", testKey(3), 1)},
		},
	}
	m := newRefPickerModel(context.Background(), store, "/tmp", 10<<20)
	m.width, m.height = 80, 24

	mi, _ := m.Update(refsLoadedMsg{refs: store.refs})
	m = mi.(browseModel)
	if len(m.filteredRefs()) != 2 {
		t.Fatalf("want 2 refs, got %d", len(m.filteredRefs()))
	}

	// '/' enters filter mode, then type "al" to filter down to alpha.
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = mi.(browseModel)
	if !m.refFiltering {
		t.Fatal("expected refFiltering after '/'")
	}
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	m = mi.(browseModel)
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	m = mi.(browseModel)
	fr := m.filteredRefs()
	if len(fr) != 1 || fr[0].Name != "alpha" {
		t.Fatalf("filter failed: %+v", fr)
	}

	// Select it; the model switches to the directory list rooted at the ref.
	mi, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = mi.(browseModel)
	if m.mode != modeList {
		t.Fatalf("mode = %v, want modeList", m.mode)
	}
	if len(m.stack) != 1 || m.stack[0].name != "ref:alpha" {
		t.Fatalf("stack: %+v", m.stack)
	}
	if cmd == nil {
		t.Fatal("expected a loadDir command after selecting a ref")
	}
	mi, _ = m.Update(cmd().(dirLoadedMsg))
	m = mi.(browseModel)
	if len(m.entries) != 1 || m.entries[0].Name != "f" {
		t.Fatalf("entries after opening ref: %+v", m.entries)
	}
}

func isQuit(cmd tea.Cmd) bool {
	if cmd == nil {
		return false
	}
	_, ok := cmd().(tea.QuitMsg)
	return ok
}

func TestModel_RefFilterTriggers(t *testing.T) {
	store := fakeStore{refs: []client.RefInfo{{Name: "x"}}}
	for _, trigger := range []rune{'/', 'f'} {
		m := newRefPickerModel(context.Background(), store, "/tmp", 10<<20)
		m.width, m.height = 80, 24
		mi, _ := m.Update(refsLoadedMsg{refs: store.refs})
		m = mi.(browseModel)
		mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{trigger}})
		m = mi.(browseModel)
		if !m.refFiltering {
			t.Fatalf("%q did not start ref filtering", trigger)
		}
		// esc leaves filtering (one level up), staying on the ref list.
		mi, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
		m = mi.(browseModel)
		if m.refFiltering {
			t.Fatalf("%q: esc did not leave filtering", trigger)
		}
		if m.mode != modeRefs {
			t.Fatalf("%q: esc should stay on ref list, got mode %v", trigger, m.mode)
		}
		if isQuit(cmd) {
			t.Fatalf("%q: esc from filter must not quit", trigger)
		}
	}
}

func TestModel_RefEscQuits(t *testing.T) {
	store := fakeStore{refs: []client.RefInfo{{Name: "x"}}}
	m := newRefPickerModel(context.Background(), store, "/tmp", 10<<20)
	m.width, m.height = 80, 24
	mi, _ := m.Update(refsLoadedMsg{refs: store.refs})
	m = mi.(browseModel)
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if !isQuit(cmd) {
		t.Fatal("esc at the reference list should quit")
	}
}

func TestModel_RefPickerCursorClampsOnFilter(t *testing.T) {
	store := fakeStore{refs: []client.RefInfo{
		{Name: "alpha"}, {Name: "alpine"}, {Name: "beta"},
	}}
	m := newRefPickerModel(context.Background(), store, "/tmp", 10<<20)
	m.width, m.height = 80, 24
	mi, _ := m.Update(refsLoadedMsg{refs: store.refs})
	m = mi.(browseModel)

	// Move cursor to the last entry (nav mode), then filter so the list shrinks.
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = mi.(browseModel)
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = mi.(browseModel)
	if m.refCursor != 2 {
		t.Fatalf("refCursor = %d, want 2", m.refCursor)
	}
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}}) // enter filter mode
	m = mi.(browseModel)
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}}) // only "beta"
	m = mi.(browseModel)
	if got := len(m.filteredRefs()); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}
	if m.refCursor != 0 {
		t.Fatalf("refCursor = %d, want clamped to 0", m.refCursor)
	}
}
