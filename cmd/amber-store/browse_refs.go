package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
)

// newRefPickerModel builds a model that opens on a searchable reference list
// instead of a directory. Used when `browse` is invoked without a SPEC.
func newRefPickerModel(ctx context.Context, store browseStore, cwd string, maxView int64) browseModel {
	ti := textinput.New()
	ti.Prompt = "export to: "
	f := textinput.New()
	f.Prompt = "filter: "
	f.Focus()
	return browseModel{
		ctx:     ctx,
		store:   store,
		cwd:     cwd,
		maxView: maxView,
		input:   ti,
		filter:  f,
		mode:    modeRefs,
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
	switch msg.String() {
	case "ctrl+c", "esc":
		return m, tea.Quit
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
	case "enter":
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
		m.stack = []frame{{key: k, name: "ref:" + sel.Name}}
		m.mode = modeList
		return m, m.loadDir(k)
	}
	var cmd tea.Cmd
	m.filter, cmd = m.filter.Update(msg)
	// Editing the filter can shrink the list out from under the cursor.
	if n := len(m.filteredRefs()); m.refCursor >= n {
		m.refCursor = 0
	}
	return m, cmd
}

func (m browseModel) viewRefs() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n\n", m.filter.View())

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

	body := m.height - 3
	if body < 1 {
		body = 1
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
