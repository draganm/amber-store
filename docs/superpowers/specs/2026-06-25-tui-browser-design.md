# TUI Filesystem Browser — Design

**Date:** 2026-06-25
**Status:** Approved, ready for implementation plan

## Summary

Add an interactive terminal browser for amber-store trees:

```
amber-store browse SPEC
```

where `SPEC` is the same content spec every read command accepts —
`KEY[/PATH]` or `ref:NAME[@PATH]` — so `amber-store browse ref:xy@/abc` opens
the tree at reference `xy`, path `/abc`. The browser lets the user navigate
directories, view file content (text, hex, or CBOR-as-JSON), and export a
directory as a tar or a file as raw bytes.

## Goals

- Single-pane directory navigation by content key.
- File viewer with three modes: text, hex, and JSON (CBOR-decoded).
- Export the highlighted directory (tar) or file (raw bytes) to a
  user-chosen path.
- No daemon protocol changes; reuse the existing client API.
- Bounded memory and render cost regardless of file/tree size.

## Non-goals (YAGNI)

- No search/filter, no in-TUI reference switching.
- No editing or writing into the store.
- No following symlinks.
- No HTTP range support — a configurable byte cap covers the viewer's needs.

## Command

Registered in `main.go` alongside the other commands.

```
amber-store browse SPEC
```

Flags:

- `--socket` — daemon unix socket (consistent with `ls`/`dump`; via the shared
  `socketFlag`).
- `--max-view-bytes` — cap on bytes fetched into the file viewer
  (default `10 << 20`, i.e. 10 MB).

Startup behaviour:

- Requires a terminal. If stdin/stdout is not a TTY, the command returns the
  error `browse requires a terminal` before starting the UI (checked with
  `golang.org/x/term.IsTerminal`).
- The spec is resolved with the existing `resolveSpec`; bad spec / daemon-down
  errors are returned through the normal CLI error path *before* the TUI starts.
- The launch working directory is captured (`os.Getwd`) for resolving relative
  export paths.

## Dependencies

Adds the Charmbracelet TUI stack and its indirect subtree:

- `github.com/charmbracelet/bubbletea` — the Elm-architecture runtime.
- `github.com/charmbracelet/bubbles` — `textinput` for the export prompt.
- `github.com/charmbracelet/lipgloss` — help-bar / header styling.

CBOR→JSON reuses the already-present `github.com/fxamacker/cbor/v2`. No new
CBOR dependency.

The directory list and the file viewer are **hand-rendered** (for column and
windowing control); `bubbles` is used only for the export text input.

## Where the code lives

Kept in `cmd/amber-store` (package `main`), matching the repo convention that
command logic lives there (e.g. `driver.go`, `ls.go`). The bubbletea model is
pure-update and unit-testable in-package without a running daemon.

- `browse.go` — command/flag wiring, TTY check, spec resolution, client
  construction, launch of the tea program.
- `browse_model.go` — root bubbletea `Model` (`Init`/`Update`/`View`) and the
  directory-list state.
- `browse_file.go` — file viewer: windowed text/hex/JSON renderer, the
  text-vs-binary heuristic, and CBOR→JSON conversion.
- `browse_export.go` — export prompt and the write `tea.Cmd`.
- `browse_*_test.go` — model and helper unit tests against a fake store.

### Store interface

The browser depends on a narrow interface so tests can use a fake; the concrete
`*client.Client` already satisfies it:

```go
type browseStore interface {
    Ls(ctx context.Context, k key.Key, path string) ([]client.Entry, error)
    File(ctx context.Context, ck key.Key) (io.ReadCloser, error)
    Tar(ctx context.Context, k key.Key, path string) (io.ReadCloser, error)
}
```

## Navigation model

Navigation is by **content key**, not by re-walking from the root each level.
The model holds a stack of frames `{key, name, cursor}`; the top frame's key is
the current directory. Entries come from `Ls(currentKey, "")`. Directory entries
carry their own `Key` (per `client.Entry`), so descent uses the highlighted
entry's key directly.

Keys in directory-list mode:

- `j`/`k` or `↓`/`↑` — move cursor.
- `PgUp`/`PgDn`, `Home`/`End` — scroll.
- `Enter` / `l` / `→` on a directory → push `{entry.Key, entry.Name, 0}` and
  list it.
- `Enter` / `l` / `→` on a regular file → open the file viewer on `entry.Key`.
- `Backspace` / `h` / `←` → pop a frame (no-op at root), restoring the saved
  cursor.
- `e` → export the highlighted entry (see Export).
- `q` / `Ctrl-C` → quit (terminal restored via alt-screen).

Symlinks and special files are not followed; the list shows `name -> target`
(reusing `ls`'s rendering). A top breadcrumb line shows the spec root plus the
name stack; a bottom help bar shows context-sensitive keys.

The list is a **windowed view**: only the visible rows (computed from the cursor
and terminal height) are rendered, reusing `ls.go`'s `modeString`,
`sizeString`, and `formatMtime` for the `ls -l`-style columns (mode, uid, gid,
size, mtime, name).

## File viewer (text / hex / JSON)

On open, fetch `File(entry.Key)` reading up to `--max-view-bytes`+1 into memory;
if it would exceed the cap, keep the cap and flag the view **truncated** (shown
in the header). No daemon changes.

Three modes selected by dedicated keys:

- `t` — text
- `x` — hex (`xxd`-style: offset · 16 hex bytes · ASCII gutter)
- `j` — JSON (CBOR-decoded)

### Auto-detect default

1. Sample the first 8 KB. If it is valid UTF-8 with no NUL and an acceptable
   printable ratio ⇒ default **text**.
2. Otherwise the content is binary; attempt a CBOR decode of the loaded buffer.
   If it decodes **cleanly, consuming every byte with no trailing data**,
   default **JSON**; otherwise default **hex**.

Requiring full consumption keeps false positives rare, and text files are never
auto-routed to JSON. The `j` key always forces a decode attempt regardless of
the default.

### JSON rendering

Decode the CBOR bytes into a Go value (`cbor.Unmarshal` into `interface{}`),
then pretty-print as indented JSON, with explicit rules for CBOR types that JSON
lacks:

- byte strings → hex string,
- non-string map keys (integers, etc.) → stringified keys,
- CBOR tags → unwrapped to their content.

If the bytes are not valid CBOR, the decode fails partway, or there is trailing
data, JSON mode shows a one-line decode error and the user can switch to `t`/`x`.

### Cap interaction

CBOR needs the complete object, so if the file was truncated at the cap, JSON
mode reports `file truncated at cap — cannot decode CBOR (export to view full)`
instead of attempting a partial decode.

### Rendering

All three modes use the same **windowed renderer**: only the visible lines are
drawn each frame, so memory and render cost stay bounded by the cap and the
terminal height regardless of file size.

- Text: a one-time line-offset index over the loaded bytes; visible lines sliced
  out per frame.
- Hex: line `i` rendered from `bytes[i*16:]`.
- JSON: a one-time line-offset index over the pretty-printed JSON string.

Header shows the file name, size, active mode (TEXT/HEX/JSON), and any
truncation notice. Keys: `t`/`x`/`j` switch mode; `j`/`k`, `PgUp`/`PgDn`,
`Home`/`End` scroll; `e` exports the open file; `q` / `Esc` / `Backspace`
returns to the directory list.

## Export (prompt with default path)

- `e` in the directory list exports the highlighted entry; `e` in the file
  viewer exports the open file.
  - Directory → tar via `Tar(key, "")`, default name `<name>.tar`.
  - File → raw bytes via `File(key)`, default name `<name>`.
- Pressing `e` opens an input line (`bubbles/textinput`) prefilled with the
  default path, resolved against the launch working directory. `Enter` writes;
  `Esc` cancels; the path is editable.
- The browser **refuses to overwrite** an existing path: it shows an error and
  stays in the prompt.
- The copy (reader → file) runs in a `tea.Cmd` (off the UI loop) and reports
  success or failure to a status line, so the UI never blocks on a large tar.
  Export is unaffected by the current view mode.

## Error handling

- `Ls`/`File`/`Tar` errors during navigation are shown in a status line and are
  non-fatal; the current view is retained.
- Fatal startup errors (non-TTY, bad spec, daemon down) are returned before the
  TUI starts.
- A truncated body / store read error surfaces as a read error while loading the
  file viewer and is shown in the status line.

## Testing

Coverage comes from in-package model and helper unit tests; no pty-driven e2e
(brittle), and the existing daemon e2e suite already covers `Ls`/`File`/`Tar`.

- **Model unit tests** with a fake `browseStore`: descend/ascend with cursor
  restore, cursor + scroll bounds, open file, mode switching, export-prompt
  open/cancel and overwrite refusal — by feeding `tea.Msg`s to `Update` and
  asserting state and `View()` output.
- **Detection tests**: text content defaults to text; a complete CBOR blob
  defaults to JSON; a binary non-CBOR blob defaults to hex.
- **CBOR→JSON conversion tests**: byte strings, non-string map keys, tags, and
  nested structures.
- **Golden test** for hex rendering of a known byte slice.
