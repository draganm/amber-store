package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

type modelMode int

const (
	modeList modelMode = iota
	modeFile
	modeExport
	modeRefs
)

// frame is one level of the navigation stack; the top frame's key is the
// directory currently listed.
type frame struct {
	key    key.Key
	name   string
	cursor int
}

type browseStore interface {
	Ls(ctx context.Context, k key.Key, path string) ([]client.Entry, error)
	File(ctx context.Context, ck key.Key) (io.ReadCloser, error)
	Tar(ctx context.Context, k key.Key, path string) (io.ReadCloser, error)
	ListRefs(ctx context.Context) ([]client.RefInfo, error)
}

type browseModel struct {
	ctx     context.Context
	store   browseStore
	cwd     string
	maxView int64

	stack   []frame
	entries []client.Entry
	cursor  int
	listTop int

	width, height int

	mode   modelMode
	status string

	file    fileView
	fileKey key.Key

	input       textinput.Model
	exportIsDir bool
	exportKey   key.Key
	exportName  string

	refs      []client.RefInfo
	filter    textinput.Model
	refCursor int
}

// newBrowseModel builds a model rooted at the resolved spec key.
func newBrowseModel(ctx context.Context, store browseStore, cwd string, maxView int64, root key.Key, rootName string) browseModel {
	ti := textinput.New()
	ti.Prompt = "export to: "
	return browseModel{
		ctx:     ctx,
		store:   store,
		cwd:     cwd,
		maxView: maxView,
		stack:   []frame{{key: root, name: rootName}},
		input:   ti,
	}
}

func (m browseModel) cur() frame { return m.stack[len(m.stack)-1] }

func (m browseModel) Init() tea.Cmd {
	if m.mode == modeRefs {
		return m.loadRefs()
	}
	return m.loadDir(m.cur().key)
}

// --- messages ---

type dirLoadedMsg struct {
	entries []client.Entry
	err     error
}
type fileLoadedMsg struct {
	key       key.Key
	name      string
	size      uint64
	data      []byte
	truncated bool
	err       error
}
type exportDoneMsg struct {
	path string
	err  error
}

// --- commands ---

func (m browseModel) loadDir(k key.Key) tea.Cmd {
	ctx, store := m.ctx, m.store
	return func() tea.Msg {
		ents, err := store.Ls(ctx, k, "")
		return dirLoadedMsg{entries: ents, err: err}
	}
}

func (m browseModel) loadFile(e client.Entry) tea.Cmd {
	ctx, store, cap := m.ctx, m.store, m.maxView
	return func() tea.Msg {
		k, err := parseHexKey(e.Key)
		if err != nil {
			return fileLoadedMsg{err: err}
		}
		rc, err := store.File(ctx, k)
		if err != nil {
			return fileLoadedMsg{err: err}
		}
		defer rc.Close()
		data, err := io.ReadAll(io.LimitReader(rc, cap+1))
		if err != nil {
			return fileLoadedMsg{err: err}
		}
		truncated := int64(len(data)) > cap
		if truncated {
			data = data[:cap]
		}
		return fileLoadedMsg{key: k, name: e.Name, size: e.Size, data: data, truncated: truncated}
	}
}

// --- helpers ---

func entryIsDir(e client.Entry) bool { return e.Mode&unix.S_IFMT == unix.S_IFDIR }
func entryIsReg(e client.Entry) bool { return e.Mode&unix.S_IFMT == unix.S_IFREG }

// --- update ---

func (m browseModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case dirLoadedMsg:
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.entries = msg.entries
		m.cursor = m.cur().cursor
		if m.cursor >= len(m.entries) {
			m.cursor = 0
		}
		m.listTop = 0
		m.status = ""
		return m, nil
	case fileLoadedMsg:
		if msg.err != nil {
			m.status = msg.err.Error()
			m.mode = modeList
			return m, nil
		}
		m.file = newFileView(msg.name, msg.size, msg.data, msg.truncated)
		m.fileKey = msg.key
		m.mode = modeFile
		return m, nil
	case exportDoneMsg:
		if msg.err != nil {
			m.status = "export failed: " + msg.err.Error()
		} else {
			m.status = "exported to " + msg.path
		}
		return m, nil
	case refsLoadedMsg:
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.refs = msg.refs
		m.refCursor = 0
		return m, nil
	case tea.KeyMsg:
		return m.updateKey(msg)
	}
	return m, nil
}

func (m browseModel) updateKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.mode {
	case modeList:
		return m.updateListKey(msg)
	case modeFile:
		return m.updateFileKey(msg)
	case modeExport:
		return m.updateExportKey(msg)
	case modeRefs:
		return m.updateRefsKey(msg)
	default:
		return m, nil
	}
}

func (m browseModel) updateListKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "down", "j":
		if m.cursor < len(m.entries)-1 {
			m.cursor++
		}
		return m, nil
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
		return m, nil
	case "home":
		m.cursor = 0
		return m, nil
	case "end":
		m.cursor = len(m.entries) - 1
		if m.cursor < 0 {
			m.cursor = 0
		}
		return m, nil
	case "enter", "l", "right":
		if len(m.entries) == 0 {
			return m, nil
		}
		e := m.entries[m.cursor]
		if entryIsDir(e) {
			k, err := parseHexKey(e.Key)
			if err != nil {
				m.status = err.Error()
				return m, nil
			}
			m.stack[len(m.stack)-1].cursor = m.cursor
			m.stack = append(m.stack, frame{key: k, name: e.Name})
			return m, m.loadDir(k)
		}
		if entryIsReg(e) {
			return m, m.loadFile(e)
		}
		return m, nil
	case "backspace", "h", "left":
		if len(m.stack) > 1 {
			m.stack = m.stack[:len(m.stack)-1]
			return m, m.loadDir(m.cur().key)
		}
		return m, nil
	case "e":
		if len(m.entries) == 0 {
			return m, nil
		}
		e := m.entries[m.cursor]
		k, err := parseHexKey(e.Key)
		if err != nil {
			m.status = err.Error()
			return m, nil
		}
		m.beginExport(entryIsDir(e), k, e.Name)
		return m, nil
	}
	return m, nil
}

func (m browseModel) updateFileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q", "esc", "backspace":
		m.mode = modeList
		return m, nil
	case "t":
		m.file.setMode(modeText)
		return m, nil
	case "x":
		m.file.setMode(modeHex)
		return m, nil
	case "j":
		m.file.setMode(modeJSON)
		return m, nil
	case "down":
		m.file.scroll(1, m.height)
		return m, nil
	case "up":
		m.file.scroll(-1, m.height)
		return m, nil
	case "pgdown", " ":
		m.file.scroll(m.height-1, m.height)
		return m, nil
	case "pgup":
		m.file.scroll(-(m.height - 1), m.height)
		return m, nil
	case "home":
		m.file.scroll(-1<<30, m.height)
		return m, nil
	case "end":
		m.file.scroll(1<<30, m.height)
		return m, nil
	case "e":
		m.beginExport(false, m.fileKey, m.file.name)
		return m, nil
	}
	return m, nil
}

func (m browseModel) updateExportKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.input.Blur()
		m.mode = modeList
		return m, nil
	case "enter":
		path := m.input.Value()
		m.input.Blur()
		m.mode = modeList
		return m, exportCmd(m.ctx, m.store, m.exportIsDir, m.exportKey, path)
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

// --- view ---

func (m browseModel) View() string {
	switch m.mode {
	case modeFile:
		return m.file.render(m.height)
	case modeExport:
		return m.viewList() + "\n" + m.input.View()
	case modeRefs:
		return m.viewRefs()
	default:
		return m.viewList()
	}
}

func (m browseModel) viewList() string {
	var b strings.Builder
	crumb := make([]string, len(m.stack))
	for i, f := range m.stack {
		crumb[i] = f.name
	}
	fmt.Fprintf(&b, "%s\n", strings.Join(crumb, "/"))

	body := m.height - 2
	if body < 1 {
		body = 1
	}
	if m.cursor < m.listTop {
		m.listTop = m.cursor
	}
	if m.cursor >= m.listTop+body {
		m.listTop = m.cursor - body + 1
	}
	now := time.Now()
	for i := m.listTop; i < m.listTop+body && i < len(m.entries); i++ {
		e := m.entries[i]
		cursor := "  "
		if i == m.cursor {
			cursor = "> "
		}
		name := e.Name
		if e.LinkTarget != "" {
			name += " -> " + e.LinkTarget
		}
		fmt.Fprintf(&b, "%s%s %s %s %s\n",
			cursor, modeString(e.Mode), sizeString(e),
			formatMtime(time.Unix(0, e.MtimeNs), now), name)
	}
	if m.status != "" {
		fmt.Fprintf(&b, "\n%s", m.status)
	}
	return b.String()
}
