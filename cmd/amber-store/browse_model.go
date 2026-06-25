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

	refs         []client.RefInfo
	filter       textinput.Model
	refCursor    int
	refFiltering bool

	dirFilter textinput.Model
	filtering bool

	rootKey  key.Key
	rootName string
}

// isFileKey reports whether k addresses file content (a Blob or FileNode) rather
// than a directory.
func isFileKey(k key.Key) bool {
	return k.Type() == key.Blob || k.Type() == key.FileNode
}

// newTextInput builds a textinput with the given prompt.
func newTextInput(prompt string) textinput.Model {
	ti := textinput.New()
	ti.Prompt = prompt
	return ti
}

// newBrowseModel builds a model rooted at the resolved spec key. A directory
// root seeds the navigation stack; a file root has no directory to list, so the
// stack stays empty and Init opens the file viewer directly.
func newBrowseModel(ctx context.Context, store browseStore, cwd string, maxView int64, root key.Key, rootName string) browseModel {
	m := browseModel{
		ctx:       ctx,
		store:     store,
		cwd:       cwd,
		maxView:   maxView,
		input:     newTextInput("export to: "),
		filter:    newTextInput("filter: "),
		dirFilter: newTextInput("filter: "),
		rootKey:   root,
		rootName:  rootName,
	}
	if isFileKey(root) {
		m.mode = modeFile
	} else {
		m.stack = []frame{{key: root, name: rootName}}
	}
	return m
}

func (m browseModel) cur() frame { return m.stack[len(m.stack)-1] }

func (m browseModel) Init() tea.Cmd {
	switch {
	case m.mode == modeRefs:
		return m.loadRefs()
	case isFileKey(m.rootKey):
		return m.fetchFile(m.rootKey, m.rootName, m.rootKey.Length())
	default:
		return m.loadDir(m.cur().key)
	}
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
	k, err := parseHexKey(e.Key)
	if err != nil {
		return func() tea.Msg { return fileLoadedMsg{err: err} }
	}
	return m.fetchFile(k, e.Name, e.Size)
}

// fetchFile streams the file content object k (up to the view cap) and reports it
// as a fileLoadedMsg. Used both for files chosen in a directory and for a file
// that is itself the browse root.
func (m browseModel) fetchFile(k key.Key, name string, size uint64) tea.Cmd {
	ctx, store, cap := m.ctx, m.store, m.maxView
	return func() tea.Msg {
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
		return fileLoadedMsg{key: k, name: name, size: size, data: data, truncated: truncated}
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
		// Each directory starts unfiltered.
		m.filtering = false
		m.dirFilter.Blur()
		m.dirFilter.SetValue("")
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
			// A file with no parent directory (the browse root) falls back to
			// the reference list rather than an empty directory view.
			if len(m.stack) == 0 {
				return m.gotoRefs()
			}
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

// visibleEntries returns the directory entries matching the dirFilter text
// (case-insensitive substring on name); an empty filter matches everything.
func (m browseModel) visibleEntries() []client.Entry {
	q := strings.ToLower(m.dirFilter.Value())
	if q == "" {
		return m.entries
	}
	var out []client.Entry
	for _, e := range m.entries {
		if strings.Contains(strings.ToLower(e.Name), q) {
			out = append(out, e)
		}
	}
	return out
}

// openEntry descends into a directory or opens a file; used by both normal and
// filtered navigation. Descending leaves the dirLoadedMsg handler to reset the
// filter.
func (m browseModel) openEntry(e client.Entry) (tea.Model, tea.Cmd) {
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
}

func (m browseModel) updateListKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.filtering {
		return m.updateListFilterKey(msg)
	}
	ve := m.visibleEntries()
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "/", "f":
		m.filtering = true
		m.dirFilter.Focus()
		return m, nil
	case "down", "j":
		if m.cursor < len(ve)-1 {
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
		m.cursor = len(ve) - 1
		if m.cursor < 0 {
			m.cursor = 0
		}
		return m, nil
	case "enter", "l", "right":
		if len(ve) == 0 {
			return m, nil
		}
		return m.openEntry(ve[m.cursor])
	case "esc", "backspace", "h", "left":
		if len(m.stack) > 1 {
			m.stack = m.stack[:len(m.stack)-1]
			return m, m.loadDir(m.cur().key)
		}
		// Already at the root: drop down to the reference list.
		return m.gotoRefs()
	case "e":
		if len(ve) == 0 {
			return m, nil
		}
		e := ve[m.cursor]
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

// updateListFilterKey handles keys while the directory filter input is focused.
func (m browseModel) updateListFilterKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "esc":
		m.filtering = false
		m.dirFilter.Blur()
		m.dirFilter.SetValue("")
		m.cursor = 0
		return m, nil
	case "enter":
		ve := m.visibleEntries()
		if len(ve) == 0 {
			return m, nil
		}
		return m.openEntry(ve[m.cursor])
	case "up":
		if m.cursor > 0 {
			m.cursor--
		}
		return m, nil
	case "down":
		if m.cursor < len(m.visibleEntries())-1 {
			m.cursor++
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.dirFilter, cmd = m.dirFilter.Update(msg)
	// Editing the filter can shrink the list out from under the cursor.
	if m.cursor >= len(m.visibleEntries()) {
		m.cursor = 0
	}
	return m, cmd
}

// gotoRefs switches to the searchable reference picker (in navigation mode) and
// reloads the refs.
func (m browseModel) gotoRefs() (tea.Model, tea.Cmd) {
	m.mode = modeRefs
	m.refCursor = 0
	m.refFiltering = false
	m.filter.SetValue("")
	m.filter.Blur()
	return m, m.loadRefs()
}

func (m browseModel) updateFileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "q", "esc", "backspace", "h", "left":
		// A file opened as the browse root has no parent directory; back goes to
		// the reference list instead.
		if len(m.stack) == 0 {
			return m.gotoRefs()
		}
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

// View truncates every line of the active screen to the terminal width. A line
// longer than the width would be wrapped by the terminal, which throws off
// Bubble Tea's line accounting and leaves stale characters behind when a later,
// shorter screen is drawn (e.g. returning from a file to the reference list).
func (m browseModel) View() string {
	return truncateLines(m.viewBody(), m.width)
}

func (m browseModel) viewBody() string {
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

// truncateLines clamps each line of s to w display columns. A non-positive w
// (no window size yet) leaves the text untouched.
func truncateLines(s string, w int) string {
	if w <= 0 {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		r := []rune(line)
		if len(r) > w {
			lines[i] = string(r[:w])
		}
	}
	return strings.Join(lines, "\n")
}

func (m browseModel) viewList() string {
	var b strings.Builder
	crumb := make([]string, len(m.stack))
	for i, f := range m.stack {
		crumb[i] = f.name
	}
	fmt.Fprintf(&b, "%s\n", strings.Join(crumb, "/"))

	entries := m.visibleEntries()

	// Reserve lines for the breadcrumb (already written), the column header, the
	// status line, and the filter input when active.
	body := m.height - 3
	if m.filtering {
		fmt.Fprintf(&b, "%s\n", m.dirFilter.View())
		body--
	}
	if body < 1 {
		body = 1
	}

	// Column widths over all visible entries, so columns stay aligned while
	// scrolling. Name comes first (directories get a trailing slash), then size.
	nameW, sizeW := len("NAME"), len("SIZE")
	for _, e := range entries {
		nameW = max(nameW, len(listName(e)))
		sizeW = max(sizeW, len(listSize(e)))
	}

	fmt.Fprintf(&b, "  %-*s  %*s  %-12s  %s\n",
		nameW, "NAME", sizeW, "SIZE", "MODIFIED", "MODE")

	if m.cursor < m.listTop {
		m.listTop = m.cursor
	}
	if m.cursor >= m.listTop+body {
		m.listTop = m.cursor - body + 1
	}
	now := time.Now()
	for i := m.listTop; i < m.listTop+body && i < len(entries); i++ {
		e := entries[i]
		cursor := "  "
		if i == m.cursor {
			cursor = "> "
		}
		link := ""
		if e.LinkTarget != "" {
			link = "  -> " + e.LinkTarget
		}
		fmt.Fprintf(&b, "%s%-*s  %*s  %-12s  %s%s\n",
			cursor, nameW, listName(e), sizeW, listSize(e),
			formatMtime(time.Unix(0, e.MtimeNs), now), modeString(e.Mode), link)
	}
	if m.filtering && len(entries) == 0 {
		b.WriteString("  (no matching entries)\n")
	}
	if m.status != "" {
		fmt.Fprintf(&b, "\n%s", m.status)
	}
	return b.String()
}

// listName is an entry's display name in the directory list: directories get a
// trailing slash so they stand out from files.
func listName(e client.Entry) string {
	if entryIsDir(e) {
		return e.Name + "/"
	}
	return e.Name
}

// listSize is the size column: a dash for directories, the byte size (or
// major,minor for devices) otherwise.
func listSize(e client.Entry) string {
	if entryIsDir(e) {
		return "-"
	}
	return sizeString(e)
}
