package main

import (
	"context"
	"fmt"
	"io"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/key"
)

// exportCmd streams a directory tar or a file's raw bytes to path. It refuses to
// overwrite an existing file (O_EXCL) so a browse session never clobbers data.
func exportCmd(ctx context.Context, store browseStore, isDir bool, k key.Key, path string) tea.Cmd {
	return func() tea.Msg {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return exportDoneMsg{path: path, err: err}
		}
		defer f.Close()
		var rc io.ReadCloser
		if isDir {
			rc, err = store.Tar(ctx, k, "")
		} else {
			rc, err = store.File(ctx, k)
		}
		if err != nil {
			os.Remove(path)
			return exportDoneMsg{path: path, err: err}
		}
		defer rc.Close()
		if _, err := io.Copy(f, rc); err != nil {
			os.Remove(path)
			return exportDoneMsg{path: path, err: err}
		}
		return exportDoneMsg{path: path, err: nil}
	}
}

// beginExport opens the export prompt for the given entry data, prefilling the
// input with a default path under the launch cwd.
func (m *browseModel) beginExport(isDir bool, k key.Key, name string) {
	m.exportIsDir = isDir
	m.exportKey = k
	m.exportName = name
	def := name
	if isDir {
		def = name + ".tar"
	}
	m.input.SetValue(joinCwd(m.cwd, def))
	m.input.CursorEnd()
	m.input.Focus()
	m.mode = modeExport
}

// joinCwd joins a default name onto the launch directory.
func joinCwd(cwd, name string) string {
	return fmt.Sprintf("%s/%s", cwd, name)
}
