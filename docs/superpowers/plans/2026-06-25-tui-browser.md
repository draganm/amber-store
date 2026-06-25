# TUI Filesystem Browser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an interactive `amber-store browse SPEC` terminal UI for navigating trees, viewing files (text/hex/CBOR-as-JSON), and exporting directories (tar) or files (raw bytes).

**Architecture:** A bubbletea Elm-architecture model lives in package `main` under `cmd/amber-store`. Pure helpers (content detection, hex rendering, CBOR→JSON, windowed line slicing) are testable without the TUI runtime; the model is driven through a narrow `browseStore` interface satisfied by `*client.Client`, so unit tests feed `tea.Msg`s to `Update` against a fake store. Navigation is by content key; the file viewer fetches up to a byte cap and renders only the visible window.

**Tech Stack:** Go, `github.com/charmbracelet/bubbletea`, `github.com/charmbracelet/bubbles/textinput`, `github.com/charmbracelet/lipgloss`, existing `github.com/fxamacker/cbor/v2`, `github.com/urfave/cli/v2`, `github.com/draganm/amber-store/client`, `golang.org/x/term`, `golang.org/x/sys/unix`.

## Global Constraints

- Go 1.26.3 (module `github.com/draganm/amber-store`).
- New CLI code lives in package `main` under `cmd/amber-store` (repo convention).
- Reuse existing helpers: `resolveSpec` (spec.go), `parseHexKey` (ref.go/daemon), `modeString`/`sizeString`/`formatMtime` (ls.go), `socketFlag` (ref.go), `socketpath.Resolve`, `client.New`.
- CBOR→JSON uses the already-present `github.com/fxamacker/cbor/v2`; add no other CBOR dep.
- Default view cap `--max-view-bytes` = `10 << 20` (10 MB).
- Commit after each task. Remove any binaries produced by `go build`.

---

### Task 1: CBOR→JSON conversion helper

**Files:**
- Create: `cmd/amber-store/browse_cbor.go`
- Test: `cmd/amber-store/browse_cbor_test.go`

**Interfaces:**
- Consumes: `github.com/fxamacker/cbor/v2`.
- Produces: `func cborToJSON(data []byte) (string, error)` — decodes one complete CBOR value (errors on invalid or trailing data) and returns indented JSON; byte strings → hex, non-string map keys → stringified, tags → unwrapped to content.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestCborToJSON_Map(t *testing.T) {
	in, err := cbor.Marshal(map[string]any{"a": 1, "b": "x"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatalf("cborToJSON: %v", err)
	}
	if !strings.Contains(out, `"a": 1`) || !strings.Contains(out, `"b": "x"`) {
		t.Fatalf("unexpected JSON:\n%s", out)
	}
}

func TestCborToJSON_ByteStringToHex(t *testing.T) {
	in, err := cbor.Marshal(map[string]any{"k": []byte{0xde, 0xad}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"dead"`) {
		t.Fatalf("byte string not hex-encoded:\n%s", out)
	}
}

func TestCborToJSON_IntKey(t *testing.T) {
	in, err := cbor.Marshal(map[int]string{7: "v"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"7": "v"`) {
		t.Fatalf("int key not stringified:\n%s", out)
	}
}

func TestCborToJSON_TrailingDataErrors(t *testing.T) {
	one, err := cbor.Marshal(1)
	if err != nil {
		t.Fatal(err)
	}
	in := append(one, one...) // two CBOR values back-to-back
	if _, err := cborToJSON(in); err == nil {
		t.Fatal("expected error on trailing data")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestCborToJSON -v`
Expected: FAIL — `undefined: cborToJSON`.

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// cborToJSON decodes exactly one CBOR value from data and renders it as indented
// JSON. cbor.Unmarshal returns an error on invalid input or trailing bytes, so a
// successful call means data is exactly one complete CBOR value. CBOR types JSON
// lacks are mapped: byte strings to hex strings, non-string map keys to their
// stringified form, and tags to their unwrapped content.
func cborToJSON(data []byte) (string, error) {
	var v any
	if err := cbor.Unmarshal(data, &v); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(toJSONable(v), "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// toJSONable rewrites a CBOR-decoded value into something json.Marshal renders
// the way we want.
func toJSONable(v any) any {
	switch t := v.(type) {
	case []byte:
		return hex.EncodeToString(t)
	case map[any]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[keyString(k)] = toJSONable(val)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = toJSONable(val)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, e := range t {
			s[i] = toJSONable(e)
		}
		return s
	case cbor.Tag:
		return toJSONable(t.Content)
	default:
		return v
	}
}

// keyString renders a CBOR map key as a JSON object key.
func keyString(k any) string {
	switch t := k.(type) {
	case string:
		return t
	case []byte:
		return hex.EncodeToString(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestCborToJSON -v`
Expected: PASS (all four).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_cbor.go cmd/amber-store/browse_cbor_test.go
git commit -m "feat(browse): CBOR-to-JSON conversion helper"
```

---

### Task 2: Content-mode detection

**Files:**
- Create: `cmd/amber-store/browse_detect.go`
- Test: `cmd/amber-store/browse_detect_test.go`

**Interfaces:**
- Consumes: `cborToJSON` (Task 1).
- Produces:
  - `type viewMode int` with `modeText`, `modeHex`, `modeJSON` (`modeText` = 0).
  - `func (m viewMode) String() string` → `"TEXT"`/`"HEX"`/`"JSON"`.
  - `func detectMode(data []byte) viewMode` — text if textual; else JSON if `cborToJSON` succeeds; else hex.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestDetectMode_Text(t *testing.T) {
	if got := detectMode([]byte("hello\nworld\n")); got != modeText {
		t.Fatalf("got %v, want modeText", got)
	}
}

func TestDetectMode_BinaryNonCBOR(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x7f, 0x80}
	if got := detectMode(data); got != modeHex {
		t.Fatalf("got %v, want modeHex", got)
	}
}

func TestDetectMode_CBOR(t *testing.T) {
	// A CBOR map of small ints — not valid UTF-8 text, decodes cleanly.
	data, err := cbor.Marshal(map[string]int{"x": 1, "y": 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := detectMode(data); got != modeJSON {
		t.Fatalf("got %v, want modeJSON", got)
	}
}

func TestViewModeString(t *testing.T) {
	for m, want := range map[viewMode]string{modeText: "TEXT", modeHex: "HEX", modeJSON: "JSON"} {
		if got := m.String(); got != want {
			t.Fatalf("%d.String() = %q, want %q", m, got, want)
		}
	}
}
```

Note: pick a CBOR payload whose first byte is not printable ASCII so `isTextual` returns false. `map[string]int{"x":1,"y":2}` encodes with a leading `0xa2` (map of 2) — non-text. Good.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run 'TestDetectMode|TestViewModeString' -v`
Expected: FAIL — `undefined: detectMode` / `undefined: modeText`.

- [ ] **Step 3: Write minimal implementation**

```go
package main

import "unicode/utf8"

// viewMode is how the file viewer renders the loaded bytes.
type viewMode int

const (
	modeText viewMode = iota
	modeHex
	modeJSON
)

func (m viewMode) String() string {
	switch m {
	case modeText:
		return "TEXT"
	case modeHex:
		return "HEX"
	case modeJSON:
		return "JSON"
	default:
		return "?"
	}
}

// detectMode chooses the default view for freshly loaded bytes: text when the
// content looks textual, else JSON when it is one complete CBOR value, else hex.
func detectMode(data []byte) viewMode {
	if isTextual(data) {
		return modeText
	}
	if _, err := cborToJSON(data); err == nil {
		return modeJSON
	}
	return modeHex
}

// isTextual reports whether the first 8 KB is valid UTF-8 with no NUL byte and a
// low ratio of control characters (tab/newline/carriage-return excepted).
func isTextual(data []byte) bool {
	sample := data
	if len(sample) > 8192 {
		sample = sample[:8192]
	}
	if len(sample) == 0 {
		return true
	}
	if !utf8.Valid(sample) {
		return false
	}
	ctrl := 0
	for _, r := range string(sample) {
		switch {
		case r == '\t' || r == '\n' || r == '\r':
		case r < 0x20:
			ctrl++
		}
		if r == 0 {
			return false
		}
	}
	return ctrl*100 <= len(sample)*30
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run 'TestDetectMode|TestViewModeString' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_detect.go cmd/amber-store/browse_detect_test.go
git commit -m "feat(browse): content-mode detection"
```

---

### Task 3: Hex dump line rendering

**Files:**
- Create: `cmd/amber-store/browse_hex.go`
- Test: `cmd/amber-store/browse_hex_test.go`

**Interfaces:**
- Produces: `func hexDumpLine(offset int, b []byte) string` — one `xxd`-style line for up to 16 bytes: 8-hex offset, 16 space-separated hex bytes (gap after byte 8, padding for short final lines), then an ASCII gutter `|...|` (printable bytes literal, others `.`).

- [ ] **Step 1: Write the failing test**

```go
package main

import "testing"

func TestHexDumpLine_Full(t *testing.T) {
	b := []byte("ABCDEFGHIJKLMNOP") // 16 printable bytes
	got := hexDumpLine(0, b)
	want := "00000000  41 42 43 44 45 46 47 48  49 4a 4b 4c 4d 4e 4f 50  |ABCDEFGHIJKLMNOP|"
	if got != want {
		t.Fatalf("\ngot:  %q\nwant: %q", got, want)
	}
}

func TestHexDumpLine_ShortWithNonPrintable(t *testing.T) {
	b := []byte{0x00, 0x41, 0xff}
	got := hexDumpLine(16, b)
	want := "00000010  00 41 ff                                          |.A.|"
	if got != want {
		t.Fatalf("\ngot:  %q\nwant: %q", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestHexDumpLine -v`
Expected: FAIL — `undefined: hexDumpLine`.

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"fmt"
	"strings"
)

// hexDumpLine renders up to 16 bytes as one xxd-style line: an 8-digit hex
// offset, 16 hex byte columns (an extra space after the 8th, blanks where the
// run is short), and an ASCII gutter with non-printable bytes shown as '.'.
func hexDumpLine(offset int, b []byte) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%08x  ", offset)
	for i := 0; i < 16; i++ {
		if i < len(b) {
			fmt.Fprintf(&sb, "%02x ", b[i])
		} else {
			sb.WriteString("   ")
		}
		if i == 7 {
			sb.WriteByte(' ')
		}
	}
	sb.WriteString(" |")
	for _, c := range b {
		if c >= 0x20 && c < 0x7f {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('.')
		}
	}
	sb.WriteByte('|')
	return sb.String()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestHexDumpLine -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_hex.go cmd/amber-store/browse_hex_test.go
git commit -m "feat(browse): xxd-style hex dump line"
```

---

### Task 4: File viewer state + windowed rendering

**Files:**
- Create: `cmd/amber-store/browse_file.go`
- Test: `cmd/amber-store/browse_file_test.go`

**Interfaces:**
- Consumes: `viewMode`/`modeText`/`modeHex`/`modeJSON` (Task 2), `detectMode` (Task 2), `cborToJSON` (Task 1), `hexDumpLine` (Task 3).
- Produces:
  - `type fileView struct { name string; size uint64; data []byte; truncated bool; mode viewMode; top int; textLines []string; jsonLines []string; jsonErr error }`
  - `func newFileView(name string, size uint64, data []byte, truncated bool) fileView` — sets default mode via `detectMode`, pre-splits text lines.
  - `func (fv *fileView) setMode(m viewMode)` — switches mode, lazily building JSON lines (or recording `jsonErr`), resets `top`.
  - `func (fv *fileView) lineCount() int` — total lines in the active mode.
  - `func (fv *fileView) scroll(delta, height int)` — clamps `top` into `[0, max(0,lineCount-height)]`.
  - `func (fv *fileView) render(height int) string` — header line + the visible window of `height-1` body lines for the active mode.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestNewFileView_DefaultModeAndTextLines(t *testing.T) {
	fv := newFileView("a.txt", 12, []byte("l1\nl2\nl3\n"), false)
	if fv.mode != modeText {
		t.Fatalf("mode = %v, want modeText", fv.mode)
	}
	// "l1\nl2\nl3\n" splits into l1,l2,l3,"" — trailing empty line.
	if fv.lineCount() != 4 {
		t.Fatalf("lineCount = %d, want 4", fv.lineCount())
	}
}

func TestFileView_SetModeJSON(t *testing.T) {
	data, err := cbor.Marshal(map[string]int{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	fv := newFileView("o.cbor", uint64(len(data)), data, false)
	if fv.mode != modeJSON {
		t.Fatalf("auto mode = %v, want modeJSON", fv.mode)
	}
	out := fv.render(10)
	if !strings.Contains(out, `"x": 1`) {
		t.Fatalf("json not rendered:\n%s", out)
	}
}

func TestFileView_JSONErrorOnTruncated(t *testing.T) {
	fv := newFileView("big", 999, []byte{0xa1, 0x61}, true) // truncated, undecodable
	fv.setMode(modeJSON)
	out := fv.render(10)
	if !strings.Contains(out, "truncated at cap") {
		t.Fatalf("expected truncation notice in JSON mode:\n%s", out)
	}
}

func TestFileView_ScrollClamp(t *testing.T) {
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, "x")
	}
	fv := newFileView("a", 0, []byte(strings.Join(lines, "\n")), false)
	fv.scroll(1000, 10) // far past end
	maxTop := fv.lineCount() - (10 - 1)
	if fv.top != maxTop {
		t.Fatalf("top = %d, want clamped %d", fv.top, maxTop)
	}
	fv.scroll(-1000, 10)
	if fv.top != 0 {
		t.Fatalf("top = %d, want 0", fv.top)
	}
}

func TestFileView_RenderHexWindow(t *testing.T) {
	fv := newFileView("b", 0, []byte{0x00, 0x01, 0x02, 0xff}, false) // binary -> hex
	if fv.mode != modeHex {
		t.Fatalf("mode = %v, want modeHex", fv.mode)
	}
	out := fv.render(10)
	if !strings.Contains(out, "00000000  00 01 02 ff") {
		t.Fatalf("hex not rendered:\n%s", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestFileView -v`
Expected: FAIL — `undefined: newFileView`.

- [ ] **Step 3: Write minimal implementation**

```go
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
// reachable for the given total height (1 line is the header).
func (fv *fileView) scroll(delta, height int) {
	body := height - 1
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

// render returns the header plus up to height-1 visible body lines.
func (fv *fileView) render(height int) string {
	body := height - 1
	if body < 1 {
		body = 1
	}
	header := fmt.Sprintf("%s  %d bytes  [%s]", fv.name, fv.size, fv.mode)
	if fv.truncated {
		header += "  (truncated)"
	}
	lines := []string{header}
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestFileView -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_file.go cmd/amber-store/browse_file_test.go
git commit -m "feat(browse): windowed file viewer (text/hex/json)"
```

---

### Task 5: Browser model — directory navigation

**Files:**
- Create: `cmd/amber-store/browse_model.go`
- Test: `cmd/amber-store/browse_model_test.go`

**Interfaces:**
- Consumes: `client.Entry`, `key.Key`, `parseHexKey`, `modeString`/`sizeString`/`formatMtime` (ls.go), `fileView` (Task 4).
- Produces:
  - `type browseStore interface { Ls(ctx, k key.Key, path string) ([]client.Entry, error); File(ctx, ck key.Key) (io.ReadCloser, error); Tar(ctx, k key.Key, path string) (io.ReadCloser, error) }`
  - `type modelMode int` with `modeList`, `modeFile`, `modeExport` (`modeList` = 0).
  - `type frame struct { key key.Key; name string; cursor int }`
  - `type browseModel struct { ... }` implementing `tea.Model`.
  - `func newBrowseModel(ctx context.Context, store browseStore, cwd string, maxView int64, root key.Key, rootName string) browseModel`
  - message types `dirLoadedMsg`, `fileLoadedMsg`, `exportDoneMsg`, and command builders `loadDir`, `loadFile`.
  - helpers `entryIsDir`, `entryIsReg`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	tea "github.com/charmbracelet/bubbletea"
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestModel_ -v`
Expected: FAIL — `undefined: newBrowseModel` / `undefined: browseModel`.

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

type modelMode int

const (
	modeList modelMode = iota
	modeFile
	modeExport
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

	file fileView

	input        textinput.Model
	exportIsDir  bool
	exportKey    key.Key
	exportName   string
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

func (m browseModel) Init() tea.Cmd { return m.loadDir(m.cur().key) }

// --- messages ---

type dirLoadedMsg struct {
	entries []client.Entry
	err     error
}
type fileLoadedMsg struct {
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
		return fileLoadedMsg{name: e.Name, size: e.Size, data: data, truncated: truncated}
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
		m.mode = modeFile
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
	}
	return m, nil
}

// --- view ---

func (m browseModel) View() string {
	switch m.mode {
	case modeFile:
		return m.file.render(m.height)
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
```

Note: `textinput` and `exportIsDir`/`exportKey`/`exportName` fields are declared here but wired in Task 7; they compile unused-field-free because struct fields need no use.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestModel_ -v`
Expected: PASS. (This downloads the bubbletea/bubbles/lipgloss modules on first build.)

- [ ] **Step 5: Tidy modules and commit**

```bash
go mod tidy
git add cmd/amber-store/browse_model.go cmd/amber-store/browse_model_test.go go.mod go.sum
git commit -m "feat(browse): directory navigation model"
```

---

### Task 6: File-view key handling in the model

**Files:**
- Modify: `cmd/amber-store/browse_model.go`
- Test: `cmd/amber-store/browse_model_test.go`

**Interfaces:**
- Consumes: `fileView.setMode`, `fileView.scroll` (Task 4); `modeText`/`modeHex`/`modeJSON`.
- Produces: file-mode key handling (`t`/`x`/`j` switch mode; `j`/`k`/arrows/pgup/pgdn/home/end scroll; `q`/`esc`/`backspace` return to list). Note `j`/`k` mean scroll in file mode (not mode-switch); JSON is bound to `J`? No — keep `j` = JSON and use arrows/space for scroll in file mode. See implementation: in file mode, `down`/`up`/`pgdown`/`pgup`/`home`/`end` scroll; `t`/`x`/`j` set mode; `esc`/`backspace`/`q` exit. (`j`/`k` are NOT scroll in file mode to avoid clashing with the `j`=JSON binding.)

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestModel_FileModeSwitchAndExit -v`
Expected: FAIL — keys ignored in file mode, `m.file.mode` stays `modeHex` / mode stays `modeFile`.

- [ ] **Step 3: Add file-mode handling**

In `updateKey`, replace the `default: return m, nil` branch so file mode is handled:

```go
func (m browseModel) updateKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.mode {
	case modeList:
		return m.updateListKey(msg)
	case modeFile:
		return m.updateFileKey(msg)
	default:
		return m, nil
	}
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
	}
	return m, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run TestModel_ -v`
Expected: PASS (all model tests).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_model.go cmd/amber-store/browse_model_test.go
git commit -m "feat(browse): file-view key handling"
```

---

### Task 7: Export prompt and writing

**Files:**
- Create: `cmd/amber-store/browse_export.go`
- Modify: `cmd/amber-store/browse_model.go`
- Test: `cmd/amber-store/browse_export_test.go`

**Interfaces:**
- Consumes: `browseModel`, `textinput.Model`, `exportDoneMsg`, `browseStore`, `entryIsDir`.
- Produces:
  - `func exportCmd(ctx context.Context, store browseStore, isDir bool, k key.Key, path string) tea.Cmd` — refuses to overwrite (O_EXCL), streams `Tar`(dir) or `File`(file) into `path`.
  - model handling: `e` in list mode opens the prompt for the highlighted entry (dir → `<name>.tar`, file → `<name>`); `e` in file mode opens it for the open file; `enter` confirms (runs `exportCmd`), `esc` cancels; `exportDoneMsg` sets the status line and returns to the prior mode.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run 'TestExportCmd|TestModel_ExportPrompt' -v`
Expected: FAIL — `undefined: exportCmd` / `e` ignored.

- [ ] **Step 3: Implement export**

Create `cmd/amber-store/browse_export.go`:

```go
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
```

Then wire the model. In `browse_model.go`:

1. In `updateListKey`, add an `e` case before the final `return`:

```go
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
```

2. In `updateFileKey`, add an `e` case to export the open file. The open file's key is not retained in `fileView`, so store it when loading. Add a field `fileKey key.Key` to `browseModel`, set it in the `fileLoadedMsg` handler by carrying the key through the message:

   - Add `key key.Key` to `fileLoadedMsg`.
   - In `loadFile`, set `return fileLoadedMsg{key: k, ...}`.
   - In the `fileLoadedMsg` handler, add `m.fileKey = msg.key`.
   - Add the `e` case in `updateFileKey`:

```go
	case "e":
		m.beginExport(false, m.fileKey, m.file.name)
		return m, nil
```

3. Add a `modeExport` branch to `updateKey`:

```go
	case modeExport:
		return m.updateExportKey(msg)
```

4. Add the export-key handler in `browse_model.go`:

```go
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
```

5. Handle `exportDoneMsg` in `Update`:

```go
	case exportDoneMsg:
		if msg.err != nil {
			m.status = "export failed: " + msg.err.Error()
		} else {
			m.status = "exported to " + msg.path
		}
		return m, nil
```

6. Add a `modeExport` branch to `View`:

```go
	case modeExport:
		return m.viewList() + "\n" + m.input.View()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/amber-store/ -run 'TestExportCmd|TestModel' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/browse_export.go cmd/amber-store/browse_model.go cmd/amber-store/browse_export_test.go
git commit -m "feat(browse): export prompt and writing"
```

---

### Task 8: Command wiring

**Files:**
- Create: `cmd/amber-store/browse.go`
- Modify: `cmd/amber-store/main.go:21-32` (command list)
- Test: `cmd/amber-store/browse_cmd_test.go`

**Interfaces:**
- Consumes: `resolveSpec` (spec.go), `socketpath.Resolve`, `client.New`, `socketFlag` (ref.go), `newBrowseModel` (Task 5).
- Produces: `func browseCommand() *cli.Command`, registered in `newApp`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestBrowseCommand_RequiresTerminal(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{browseCommand()}}
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	// Stdout in `go test` is not a TTY, so browse must refuse before any daemon I/O.
	err := app.Run([]string{"amber-store", "browse", "ref:does-not-matter"})
	if err == nil {
		t.Fatal("expected an error when not attached to a terminal")
	}
}

func TestBrowseCommand_ArgCount(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{browseCommand()}}
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	err := app.Run([]string{"amber-store", "browse"})
	if err == nil {
		t.Fatal("expected an error with no SPEC argument")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestBrowseCommand -v`
Expected: FAIL — `undefined: browseCommand`.

- [ ] **Step 3: Implement the command**

Create `cmd/amber-store/browse.go`:

```go
package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/socketpath"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

func browseCommand() *cli.Command {
	var socket string
	var maxView int64
	return &cli.Command{
		Name:      "browse",
		Usage:     "interactively browse a tree: navigate dirs, view files (text/hex/json), export; accepts KEY[/PATH] or ref:NAME[@PATH]",
		ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.Int64Flag{
				Name:        "max-view-bytes",
				Usage:       "cap on bytes fetched into the file viewer",
				Value:       10 << 20,
				Destination: &maxView,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("browse requires exactly one KEY[/PATH] argument, got %d", c.NArg())
			}
			if !term.IsTerminal(int(os.Stdout.Fd())) {
				return fmt.Errorf("browse requires a terminal")
			}
			cl := client.New(socketpath.Resolve(socket))
			k, path, err := resolveSpec(c.Context, cl, c.Args().First())
			if err != nil {
				return err
			}
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			rootName := c.Args().First()
			m := newBrowseModel(c.Context, cl, cwd, maxView, k, rootName)
			_ = path // root navigation starts at k; a non-empty PATH is part of rootName for display
			p := tea.NewProgram(m, tea.WithAltScreen())
			_, err = p.Run()
			return err
		},
	}
}
```

Note on `path`: `resolveSpec` returns the key for `ref:NAME` (the reference root) and the sub-path separately. For v1 the browser roots navigation at the resolved key `k`; the spec's PATH is shown verbatim as the breadcrumb root label (`rootName`). Honoring a deep initial PATH would require resolving it to a key first; that is out of scope per the design (navigation starts at the reference/key root). If a non-empty `path` is supplied, prepend a status note. Replace the `_ = path` line with:

```go
			m := newBrowseModel(c.Context, cl, cwd, maxView, k, rootName)
			if path != "" {
				m.status = fmt.Sprintf("note: initial subpath %q not auto-opened; navigate from the root", path)
			}
```

(Remove the earlier `m := ...` duplicate; construct the model once, then set status if needed.)

- [ ] **Step 4: Register the command in `main.go`**

In `cmd/amber-store/main.go`, add `browseCommand()` to the `commands` slice (after `refCommand()`):

```go
	commands := []*cli.Command{
		daemonCommand(),
		serveCommand(),
		ingestCommand(),
		loadCommand(),
		dumpCommand(),
		restoreCommand(),
		lsCommand(),
		contentKeysCommand(),
		configUserCommand(),
		refCommand(),
		remoteCommand(),
		browseCommand(),
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run TestBrowseCommand -v`
Expected: PASS.

- [ ] **Step 6: Build, verify the command is registered, then remove the binary**

Run:
```bash
go build -o /tmp/amber-store-build ./cmd/amber-store && /tmp/amber-store-build browse --help && rm -f /tmp/amber-store-build
```
Expected: help text for `browse` prints; binary removed (per the always-remove-generated-binaries rule).

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/browse.go cmd/amber-store/browse_cmd_test.go cmd/amber-store/main.go
git commit -m "feat(browse): wire browse command into the CLI"
```

---

### Task 9: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run the whole package test suite**

Run: `go test ./cmd/amber-store/ -count=1`
Expected: `ok  github.com/draganm/amber-store/cmd/amber-store`.

- [ ] **Step 2: Vet**

Run: `go vet ./cmd/amber-store/`
Expected: no output.

- [ ] **Step 3: Confirm no stray binaries**

Run: `git status --porcelain`
Expected: clean (no untracked binaries; only committed files).

---

## Self-Review

**Spec coverage:**
- Command + flags (`--socket`, `--max-view-bytes`) → Task 8. ✓
- TTY check, spec resolution, launch cwd → Task 8. ✓
- `browseStore` interface → Task 5. ✓
- Key-based navigation, breadcrumb, windowed list, `ls` column reuse → Task 5. ✓
- File viewer cap + truncation, text/hex/JSON, auto-detect, dedicated mode keys, windowed render → Tasks 1–4, 6. ✓
- CBOR→JSON with byte-string/key/tag rules + trailing-data error → Task 1. ✓
- JSON cap interaction message → Task 4. ✓
- Export prompt with default path, overwrite refusal, off-loop write → Task 7. ✓
- Error handling via status line → Tasks 5–7. ✓
- Tests: model transitions, detection, CBOR conversion, hex golden → Tasks 1–8. ✓
- Dependencies added + `go mod tidy` → Task 5. ✓

**Placeholder scan:** No TBD/TODO; all code shown in full. ✓

**Type consistency:** `viewMode`/`modeText|modeHex|modeJSON` (Task 2) reused in Tasks 4, 6. `modelMode`/`modeList|modeFile|modeExport` (Task 5) reused in Tasks 6–8. `fileView` API (Task 4) used by model in Tasks 5–7. `fileLoadedMsg` gains a `key` field in Task 7 (noted as a modification). `browseStore` (Task 5) consumed by `exportCmd`/`loadFile`. ✓

**Note on potential name clash:** `modeText`/`modeHex`/`modeJSON` (viewMode) and `modeList`/`modeFile`/`modeExport` (modelMode) are distinct identifiers — no collision. Both enums use `iota` starting at 0, but they are separate types.
