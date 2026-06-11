# .amberignore Support for Ingest

**Date:** 2026-06-11
**Status:** Approved

## Goal

When `amber-store ingest` walks a directory tree, honor `.amberignore` files
with the same semantics as git's `.gitignore`: entries matching the patterns
are excluded from the ingested tree.

## Decisions (confirmed with user)

1. **Per-directory composition.** An `.amberignore` in any directory applies
   to that subtree. Nested files can add patterns or negate inherited ones,
   exactly like `.gitignore`.
2. **`.amberignore` files are themselves ingested.** They appear in the stored
   tree like any other regular file, so a restored tree re-ingests
   identically.
3. **Matching is implemented with go-git's gitignore package**
   (`github.com/go-git/go-git/v5/plumbing/format/gitignore`), giving full
   gitignore semantics: comments, blank lines, negation (`!`), `**`,
   dir-only patterns (trailing `/`), anchored vs. floating patterns,
   last-match-wins ordering.
4. **`--no-ignore` flag** on `ingest` disables all `.amberignore` processing
   for that run.

## Architecture

### New package: `internal/amberignore`

A small wrapper around go-git's matcher with an immutable, tree-shaped API so
it threads naturally through recursive walkers (including the parallel one —
no shared mutable state):

```go
// Root returns the matcher for the ingest root directory,
// loading <root>/.amberignore if present.
func Root(rootDir string) (*Matcher, error)

// Descend returns a child matcher for the subdirectory with the given
// name, loading its .amberignore if present. relSegments is the path of
// the subdirectory relative to the root, split on "/".
func (m *Matcher) Descend(absDir string, relSegments []string) (*Matcher, error)

// Ignored reports whether the entry (relative path segments from the
// root) should be excluded.
func (m *Matcher) Ignored(relSegments []string, isDir bool) bool
```

Internally each `Matcher` holds the accumulated `[]gitignore.Pattern`
(parent's patterns + patterns parsed from this directory's `.amberignore`,
with the directory's relative segments as the pattern domain) and a
`gitignore.Matcher` built from them. `Descend` with no `.amberignore`
present shares the parent's pattern slice — no copying.

A `nil` *Matcher is valid and ignores nothing; `--no-ignore` simply passes
`nil` through the walk.

### Integration points

All three walkers filter entries immediately after `os.ReadDir`, and prune
ignored directories (no descent):

| Walker | Location | Change |
|---|---|---|
| Sequential builder | `cmd/amber-store/driver.go` `buildDir`/`buildEntry` | thread `*Matcher` + rel segments through recursion; skip ignored entries |
| Parallel builder | `cmd/amber-store/ingest.go` `pbuilder.buildDir` | same filtering before fan-out |
| Progress pre-scan | `cmd/amber-store/scan.go` `scanner.walk` | same filtering, so the progress total equals what is actually ingested |

`runIngest` constructs the root matcher once (or `nil` when `--no-ignore`)
and hands it to both the scanner and the builder.

### Semantics details

- The `.amberignore` file in a directory is read before that directory's
  entries are filtered, so it can ignore its siblings — but it is itself
  always ingested (decision 2). If a pattern matches `.amberignore` itself,
  the file is still stored; ignore files are never self-excluding. This keeps
  restored trees re-ingestable.
- Patterns in a nested `.amberignore` are domain-scoped: they only match
  inside that directory's subtree (go-git's `domain` mechanism).
- An ignored directory is pruned entirely: not descended, not stored, not
  counted by the progress scan.
- Symlinks, devices, fifos, sockets are matched by name like any entry,
  with `isDir == false`.

### Error handling

- Missing `.amberignore`: no-op.
- Unreadable `.amberignore` (permission error, etc.): the ingest fails with
  a wrapped error naming the file, consistent with existing read-error
  handling in the build path.
- Malformed pattern lines: gitignore has no invalid lines (anything parses);
  no special handling needed.

## Testing

- **`internal/amberignore` unit tests:** root patterns, nested file adding
  patterns, nested negation re-including a parent-ignored name, dir-only
  (`build/`) vs file patterns, `**` globs, anchored (`/x`) vs floating
  patterns, comments/blank lines, `.amberignore` never self-excluded,
  `nil` matcher ignores nothing.
- **Ingest tests (`cmd/amber-store/ingest_test.go` style):**
  - sequential vs. parallel parity on a tree containing `.amberignore` files
    (extend the existing parity helpers);
  - ignored files and pruned directories absent from emitted objects;
  - `.amberignore` files present in emitted objects;
  - `--no-ignore` ingests everything;
  - progress scan total equals ingested file count/bytes on a tree with
    ignores.

## Out of scope

- Global/user-level ignore configuration (analog of `core.excludesFile`).
- Honoring `.gitignore` files themselves.
- Ignore support in commands other than `ingest` (no other command walks a
  source directory).
