package main

import (
	"context"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
)

// newRefPickerModel builds a model that opens on a searchable reference list
// instead of a directory. Used when `browse` is invoked without a SPEC. The list
// starts in navigation mode; '/' or 'f' begins filtering, matching directories.
func newRefPickerModel(ctx context.Context, store browseStore, cwd string, maxView int64) browseModel {
	return browseModel{
		ctx:       ctx,
		store:     store,
		cwd:       cwd,
		maxView:   maxView,
		input:     newTextInput("export to: "),
		filter:    newTextInput("filter: "),
		dirFilter: newTextInput("filter: "),
		mode:      modeRefs,
	}
}

type refsLoadedMsg struct {
	refs []client.RefInfo
	err  error
}

func (m browseModel) loadRefs() tea.Cmd {
	ctx, store := m.ctx, m.store
	return func() tea.Msg {
		refs, err := store.ListRefs(ctx)
		return refsLoadedMsg{refs: refs, err: err}
	}
}

// filteredRefs returns the references whose name contains the filter text,
// case-insensitively; an empty filter matches everything.
func (m browseModel) filteredRefs() []client.RefInfo {
	q := strings.ToLower(m.filter.Value())
	if q == "" {
		return m.refs
	}
	var out []client.RefInfo
	for _, r := range m.refs {
		if strings.Contains(strings.ToLower(r.Name), q) {
			out = append(out, r)
		}
	}
	return out
}

func (m browseModel) updateRefsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.refFiltering {
		return m.updateRefsFilterKey(msg)
	}
	switch msg.String() {
	case "ctrl+c", "q", "esc":
		// The reference list is the top level: esc (and q) exit.
		return m, tea.Quit
	case "/", "f":
		m.refFiltering = true
		m.filter.Focus()
		return m, nil
	case "down", "j":
		if m.refCursor < len(m.filteredRefs())-1 {
			m.refCursor++
		}
		return m, nil
	case "up", "k":
		if m.refCursor > 0 {
			m.refCursor--
		}
		return m, nil
	case "home":
		m.refCursor = 0
		return m, nil
	case "end":
		m.refCursor = len(m.filteredRefs()) - 1
		if m.refCursor < 0 {
			m.refCursor = 0
		}
		return m, nil
	case "enter":
		return m.openRef()
	}
	return m, nil
}

// updateRefsFilterKey handles keys while the reference filter input is focused.
func (m browseModel) updateRefsFilterKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "esc":
		// One level up: leave filtering, back to reference navigation.
		m.refFiltering = false
		m.filter.Blur()
		m.filter.SetValue("")
		m.refCursor = 0
		return m, nil
	case "enter":
		return m.openRef()
	case "up":
		if m.refCursor > 0 {
			m.refCursor--
		}
		return m, nil
	case "down":
		if m.refCursor < len(m.filteredRefs())-1 {
			m.refCursor++
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.filter, cmd = m.filter.Update(msg)
	// Editing the filter can shrink the list out from under the cursor.
	if n := len(m.filteredRefs()); m.refCursor >= n {
		m.refCursor = 0
	}
	return m, cmd
}

// openRef opens the highlighted reference: a file ref goes straight to the file
// viewer (empty stack marks it as a root file), a directory ref starts listing.
func (m browseModel) openRef() (tea.Model, tea.Cmd) {
	fr := m.filteredRefs()
	if len(fr) == 0 {
		return m, nil
	}
	sel := fr[m.refCursor]
	k, err := parseHexKey(sel.Key)
	if err != nil {
		m.status = err.Error()
		return m, nil
	}
	m.filter.Blur()
	m.refFiltering = false
	name := "ref:" + sel.Name
	if isFileKey(k) {
		m.stack = nil
		return m, m.fetchFile(k, name, k.Length())
	}
	m.stack = []frame{{key: k, name: name}}
	m.mode = modeList
	return m, m.loadDir(k)
}

func (m browseModel) viewRefs() string {
	var b strings.Builder
	b.WriteString("references\n")

	// header + separator + status reserved; the filter input takes one more line
	// when active.
	body := m.height - 3
	if m.refFiltering {
		fmt.Fprintf(&b, "%s\n", m.filter.View())
		body--
	}
	fmt.Fprintf(&b, "%s\n", hr(m.width))
	if body < 1 {
		body = 1
	}

	fr := m.filteredRefs()
	if len(fr) == 0 {
		b.WriteString("  (no matching references)")
		if m.status != "" {
			fmt.Fprintf(&b, "\n\n%s", m.status)
		}
		return b.String()
	}

	nameW := 0
	for _, r := range fr {
		nameW = max(nameW, len(r.Name))
	}

	top := 0
	if m.refCursor >= body {
		top = m.refCursor - body + 1
	}
	for i := top; i < top+body && i < len(fr); i++ {
		r := fr[i]
		cursor := "  "
		if i == m.refCursor {
			cursor = "> "
		}
		fmt.Fprintf(&b, "%s%-*s  %s  %s\n", cursor, nameW, r.Name, r.User, r.CreatedAt)
	}
	if m.status != "" {
		fmt.Fprintf(&b, "\n%s", m.status)
	}
	return b.String()
}
