package main

import (
	"fmt"
	"strings"
)

// fileView holds the in-memory bytes of one file and the state needed to render
// a scrolling window of them in text, hex, or JSON form. Only the visible window
// is rendered each frame, so cost is bounded by terminal height, not file size.
type fileView struct {
	name      string
	size      uint64
	data      []byte
	truncated bool

	mode viewMode
	top  int // first visible body line

	textLines []string // built once in newFileView
	jsonLines []string // built lazily on first JSON view
	jsonBuilt bool
	jsonErr   error
}

// newFileView builds a viewer for data, choosing the default mode by detection
// and pre-splitting the text lines.
func newFileView(name string, size uint64, data []byte, truncated bool) fileView {
	fv := fileView{
		name:      name,
		size:      size,
		data:      data,
		truncated: truncated,
		textLines: strings.Split(string(data), "\n"),
	}
	fv.mode = detectMode(data)
	if fv.mode == modeJSON {
		fv.buildJSON()
	}
	return fv
}

// setMode switches the active render mode, building JSON lines on first use, and
// resets the scroll position.
func (fv *fileView) setMode(m viewMode) {
	if m == modeJSON {
		fv.buildJSON()
	}
	fv.mode = m
	fv.top = 0
}

// buildJSON decodes the CBOR once. A file truncated at the cap cannot be a
// complete CBOR value, so it is reported rather than partially decoded.
func (fv *fileView) buildJSON() {
	if fv.jsonBuilt {
		return
	}
	fv.jsonBuilt = true
	if fv.truncated {
		fv.jsonErr = fmt.Errorf("file truncated at cap — cannot decode CBOR (export to view full)")
		return
	}
	s, err := cborToJSON(fv.data)
	if err != nil {
		fv.jsonErr = err
		return
	}
	fv.jsonLines = strings.Split(s, "\n")
}

// lineCount is the number of body lines in the active mode.
func (fv *fileView) lineCount() int {
	switch fv.mode {
	case modeText:
		return len(fv.textLines)
	case modeJSON:
		if fv.jsonErr != nil {
			return 1
		}
		return len(fv.jsonLines)
	default: // modeHex
		return (len(fv.data) + 15) / 16
	}
}

// scroll moves the window by delta lines, clamped so the last line stays
// reachable for the given total height (2 lines are the header and separator).
func (fv *fileView) scroll(delta, height int) {
	body := height - 2
	if body < 1 {
		body = 1
	}
	fv.top += delta
	maxTop := fv.lineCount() - body
	if maxTop < 0 {
		maxTop = 0
	}
	if fv.top > maxTop {
		fv.top = maxTop
	}
	if fv.top < 0 {
		fv.top = 0
	}
}

// render returns the header, a separator rule, and up to height-2 visible body
// lines.
func (fv *fileView) render(width, height int) string {
	body := height - 2
	if body < 1 {
		body = 1
	}
	header := fmt.Sprintf("%s  %d bytes  [%s]", fv.name, fv.size, fv.mode)
	if fv.truncated {
		header += "  (truncated)"
	}
	lines := []string{header, hr(width)}
	switch fv.mode {
	case modeText:
		lines = append(lines, window(fv.textLines, fv.top, body)...)
	case modeJSON:
		if fv.jsonErr != nil {
			lines = append(lines, fv.jsonErr.Error())
		} else {
			lines = append(lines, window(fv.jsonLines, fv.top, body)...)
		}
	default: // modeHex
		total := fv.lineCount()
		for i := fv.top; i < fv.top+body && i < total; i++ {
			end := (i + 1) * 16
			if end > len(fv.data) {
				end = len(fv.data)
			}
			lines = append(lines, hexDumpLine(i*16, fv.data[i*16:end]))
		}
	}
	return strings.Join(lines, "\n")
}

// window returns lines[top:top+n], clamped to the slice bounds.
func window(lines []string, top, n int) []string {
	if top >= len(lines) {
		return nil
	}
	end := top + n
	if end > len(lines) {
		end = len(lines)
	}
	return lines[top:end]
}
