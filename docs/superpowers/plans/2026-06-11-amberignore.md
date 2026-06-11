# .amberignore Ingest Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `amber-store ingest` honors per-directory `.amberignore` files with full gitignore semantics; `--no-ignore` disables them.

**Architecture:** A new `internal/amberignore` package wraps go-git's `plumbing/format/gitignore` matcher in an immutable, tree-shaped API (`Root` → `Descend` → `Ignored`). The three directory walkers — the sequential builder (`driver.buildDir`), the parallel builder (`pbuilder.buildDir`), and the progress pre-scan (`scanner.walk`) — each thread a `*amberignore.Matcher` down their recursion, skip ignored entries right after `os.ReadDir`, and prune ignored directories without descending. A `nil` `*Matcher` is valid and ignores nothing; `--no-ignore` just passes `nil`.

**Tech Stack:** Go, `github.com/go-git/go-git/v5/plumbing/format/gitignore` (new dependency, already in the local module cache at v5.16.2), `urfave/cli/v2`, stdlib testing.

**Spec:** `docs/superpowers/specs/2026-06-11-amberignore-design.md`

**Conventions for this repo:**
- Run tests with `go test ./...` from the repo root (`/Users/dragan/draganm/amber-store`).
- Run `gofmt -w` on every file you touch before committing.
- Do not leave any compiled binaries behind (`go build` to a temp path or use `go vet` / `go test` only).
- Parameter order everywhere: the matcher goes immediately after the directory, e.g. `buildDir(path, ign, emit)`, `scanTree(dir, ign, jobs)`.

**File map:**

| File | Role |
|---|---|
| `internal/amberignore/amberignore.go` | Create — matcher wrapper |
| `internal/amberignore/amberignore_test.go` | Create — matcher unit tests |
| `cmd/amber-store/driver.go` | Modify — sequential walker filters entries |
| `cmd/amber-store/ingest.go` | Modify — parallel walker, `ingestObjects`, `writePack`, `--no-ignore` flag, `runIngest` wiring |
| `cmd/amber-store/scan.go` | Modify — progress pre-scan filters entries |
| `cmd/amber-store/amberignore_ingest_test.go` | Create — ingest-level tests + shared tree fixture |
| `cmd/amber-store/ingest_test.go`, `driver_test.go`, `scan_test.go` | Modify — adapt call sites to new signatures |
| `README.md` | Modify — document the feature |

---

### Task 1: `internal/amberignore` package

**Files:**
- Create: `internal/amberignore/amberignore.go`
- Create: `internal/amberignore/amberignore_test.go`

go-git API facts (verified against `go-git/v5@v5.16.2/plumbing/format/gitignore`):
- `gitignore.ParsePattern(line string, domain []string) Pattern` — `domain` is the path (segments, relative to the walk root) of the directory containing the ignore file; the pattern only matches paths under that domain. Handles `!` negation, trailing-`/` dir-only, `**`, anchoring (leading `/`), trailing-space trimming.
- `gitignore.NewMatcher(ps []Pattern) Matcher` — patterns in order of **increasing** priority (append deeper files after inherited ones); `Match(path []string, isDir bool) bool` returns true when the path is excluded (last match wins, negation handled internally).
- Comment (`#`-prefixed) and blank lines are not handled by `ParsePattern` — the caller skips them (mirroring go-git's own `ReadPatterns`).

- [ ] **Step 1: Write the failing tests**

Create `internal/amberignore/amberignore_test.go`:

```go
package amberignore

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestNilMatcherIgnoresNothing(t *testing.T) {
	var m *Matcher
	if m.Ignored("anything", false) || m.Ignored("anything", true) {
		t.Error("nil matcher must not ignore entries")
	}
	sub, err := m.Descend("/nonexistent", "x")
	if err != nil || sub != nil {
		t.Errorf("nil.Descend = (%v, %v), want (nil, nil)", sub, err)
	}
}

func TestNoIgnoreFileIgnoresNothing(t *testing.T) {
	m, err := Root(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if m.Ignored("a.txt", false) || m.Ignored("dir", true) {
		t.Error("matcher without patterns must not ignore entries")
	}
}

func TestRootPatterns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "# comment\n\n*.log\nbuild/\n/anchored.txt\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		isDir bool
		want  bool
	}{
		{"app.log", false, true},
		{"app.log", true, true}, // *.log has no trailing slash: matches dirs too
		{"a.txt", false, false},
		{"build", true, true},    // dir-only pattern matches the directory
		{"build", false, false},  // ...but not a regular file of the same name
		{"anchored.txt", false, true},
	}
	for _, c := range cases {
		if got := m.Ignored(c.name, c.isDir); got != c.want {
			t.Errorf("Ignored(%q, isDir=%v) = %v, want %v", c.name, c.isDir, got, c.want)
		}
	}
}

func TestAnchoredPatternOnlyMatchesAtItsDomain(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "/top.txt\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := m.Descend(filepath.Join(dir, "sub"), "sub")
	if err != nil {
		t.Fatal(err)
	}
	if !m.Ignored("top.txt", false) {
		t.Error("anchored pattern must match at the root")
	}
	if sub.Ignored("top.txt", false) {
		t.Error("anchored pattern must not match in a subdirectory")
	}
}

func TestNestedFileAddsPatterns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "sub/.amberignore", "*.tmp\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := m.Descend(filepath.Join(dir, "sub"), "sub")
	if err != nil {
		t.Fatal(err)
	}
	if m.Ignored("x.tmp", false) {
		t.Error("sub's patterns must not apply at the root")
	}
	if !sub.Ignored("x.tmp", false) {
		t.Error("*.tmp from sub/.amberignore must apply inside sub")
	}
}

func TestNestedNegationWins(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "*.log\n")
	writeFile(t, dir, "sub/.amberignore", "!keep.log\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := m.Descend(filepath.Join(dir, "sub"), "sub")
	if err != nil {
		t.Fatal(err)
	}
	if !m.Ignored("keep.log", false) {
		t.Error("keep.log must be ignored at the root")
	}
	if sub.Ignored("keep.log", false) {
		t.Error("nested negation must re-include keep.log in sub")
	}
	if !sub.Ignored("other.log", false) {
		t.Error("inherited *.log must still apply in sub")
	}
}

func TestDomainScopedToDefiningDirectory(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "sub/.amberignore", "*.tmp\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	other, err := m.Descend(filepath.Join(dir, "other"), "other")
	if err != nil {
		t.Fatal(err)
	}
	if other.Ignored("x.tmp", false) {
		t.Error("sub's patterns must not leak into a sibling directory")
	}
}

func TestPatternsApplyToDeeperDescendants(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "secret*\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := m.Descend(filepath.Join(dir, "sub"), "sub")
	if err != nil {
		t.Fatal(err)
	}
	deeper, err := sub.Descend(filepath.Join(dir, "sub", "deeper"), "deeper")
	if err != nil {
		t.Fatal(err)
	}
	if !deeper.Ignored("secret-2", false) {
		t.Error("floating pattern must apply at any depth")
	}
}

func TestDoubleStarGlob(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "doc/**/junk\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := m.Descend(filepath.Join(dir, "doc"), "doc")
	if err != nil {
		t.Fatal(err)
	}
	a, err := doc.Descend(filepath.Join(dir, "doc", "a"), "a")
	if err != nil {
		t.Fatal(err)
	}
	if !a.Ignored("junk", false) {
		t.Error("doc/**/junk must match doc/a/junk")
	}
	if m.Ignored("junk", false) {
		t.Error("doc/**/junk must not match junk at the root")
	}
}

func TestAmberignoreFileNeverSelfExcluded(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "*\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Ignored(FileName, false) {
		t.Error(".amberignore must always be ingested")
	}
	if !m.Ignored("anything-else", false) {
		t.Error("'*' must ignore other entries")
	}
}

func TestUnreadableIgnoreFileFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	writeFile(t, dir, FileName, "*.log\n")
	if err := os.Chmod(filepath.Join(dir, FileName), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := Root(dir); err == nil {
		t.Error("expected an error for an unreadable .amberignore")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/amberignore/`
Expected: FAIL (compile error — package `amberignore` does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `internal/amberignore/amberignore.go`:

```go
// Package amberignore filters ingest trees through .amberignore files with
// gitignore semantics: patterns compose per directory down the tree and
// support negation, ** globs, dir-only (trailing /) and anchored (leading /)
// forms; the last matching pattern wins.
package amberignore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// FileName is the per-directory ignore file honored during ingest.
const FileName = ".amberignore"

// Matcher answers whether entries of one directory are ignored, carrying the
// patterns accumulated from the ingest root down to that directory. A nil
// *Matcher is valid and ignores nothing (used for --no-ignore). Matchers are
// immutable, so sibling subtrees can Descend and match concurrently.
type Matcher struct {
	rel      []string // path of this matcher's directory relative to the root
	patterns []gitignore.Pattern
	m        gitignore.Matcher
}

// Root returns the matcher for the ingest root, loading <rootDir>/.amberignore
// if present.
func Root(rootDir string) (*Matcher, error) {
	return load(rootDir, nil, nil)
}

// Descend returns the matcher for the subdirectory name of m's directory
// (absolute path absDir), loading its .amberignore if present.
func (m *Matcher) Descend(absDir, name string) (*Matcher, error) {
	if m == nil {
		return nil, nil
	}
	return load(absDir, appendCopy(m.rel, name), m)
}

// Ignored reports whether the entry name (of type isDir) inside m's directory
// is excluded. .amberignore files are never themselves excluded, so a
// restored tree re-ingests to the same root.
func (m *Matcher) Ignored(name string, isDir bool) bool {
	if m == nil {
		return false
	}
	if !isDir && name == FileName {
		return false
	}
	return m.m.Match(appendCopy(m.rel, name), isDir)
}

// load builds the matcher for the directory dir (path rel relative to the
// root), extending parent's patterns with dir/.amberignore if it exists.
func load(dir string, rel []string, parent *Matcher) (*Matcher, error) {
	data, err := os.ReadFile(filepath.Join(dir, FileName))
	if errors.Is(err, fs.ErrNotExist) {
		if parent != nil {
			// No new patterns: share the parent's matcher, only rel changes.
			return &Matcher{rel: rel, patterns: parent.patterns, m: parent.m}, nil
		}
		return &Matcher{m: gitignore.NewMatcher(nil)}, nil
	}
	if err != nil {
		return nil, err
	}
	var inherited []gitignore.Pattern
	if parent != nil {
		inherited = parent.patterns
	}
	ps := make([]gitignore.Pattern, len(inherited), len(inherited)+8)
	copy(ps, inherited)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		ps = append(ps, gitignore.ParsePattern(line, rel))
	}
	return &Matcher{rel: rel, patterns: ps, m: gitignore.NewMatcher(ps)}, nil
}

// appendCopy returns a fresh slice s+[v]. Sibling directories are processed
// concurrently during the parallel build, so the parent's shared slice must
// never be appended to in place.
func appendCopy(s []string, v string) []string {
	out := make([]string, len(s)+1)
	copy(out, s)
	out[len(s)] = v
	return out
}
```

- [ ] **Step 4: Add the go-git dependency**

Run from the repo root:
```bash
go get github.com/go-git/go-git/v5@v5.16.2
go mod tidy
```
Expected: `go.mod` gains `github.com/go-git/go-git/v5 v5.16.2` plus a few indirect deps (go-billy, gcfg, etc.). v5.16.2 is already in the local module cache, so this works offline.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/amberignore/ -v`
Expected: PASS, all 11 tests.

- [ ] **Step 6: Verify the rest of the repo still builds and commit**

Run: `gofmt -l internal/amberignore/` (expect no output), then `go test ./...`
Expected: PASS.

```bash
git add internal/amberignore/ go.mod go.sum
git commit -m "feat: amberignore matcher with gitignore semantics

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Filter the sequential and parallel builders

The two builders share `driver.buildEntry` (its `buildDir` callback parameter is how the parallel builder injects itself), so both change in one task to keep the package compiling.

**Files:**
- Modify: `cmd/amber-store/driver.go` (`buildDir` lines 24-41, `buildEntry` lines 48-114)
- Modify: `cmd/amber-store/ingest.go` (`ingestObjects` line 43, `pbuilder.buildDir` lines 116-156, `writePack` line 204, `runIngest` writePack call sites lines 289 and 307 — pass `nil` for now; real wiring is Task 4)
- Modify: `cmd/amber-store/ingest_test.go` (helpers `collectSequential` line 25, `collectParallel` line 41; callers at lines 85-86, 97-99, 236)
- Modify: `cmd/amber-store/driver_test.go` (line 23)
- Create: `cmd/amber-store/amberignore_ingest_test.go`

- [ ] **Step 1: Write the failing tests**

Create `cmd/amber-store/amberignore_ingest_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/internal/amberignore"
)

// writeIgnoredTree populates dir with a tree containing .amberignore files
// and entries they exclude. With pruned=true the excluded entries are not
// written, producing exactly the tree a filtered ingest of the full variant
// should store (including the .amberignore files, which are always ingested).
func writeIgnoredTree(t *testing.T, dir string, pruned bool) {
	t.Helper()
	write := func(rel, content string) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Present in both variants.
	write(".amberignore", "*.log\nbuild/\n!keep.log\n")
	write("a.txt", "alpha")
	write("keep.log", "negated, kept")
	write("sub/.amberignore", "secret*\n")
	write("sub/ok.txt", "ok")
	write("sub/build", "a file named build: dir-only pattern must not match")
	write("sub/deeper/data.txt", "data")
	if err := os.Symlink("a.txt", filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if pruned {
		return
	}
	// Excluded by the patterns above.
	write("app.log", "ignored by *.log")
	write("build/x.txt", "build/ prunes the whole directory")
	write("old.log/inside.txt", "*.log without trailing slash matches dirs too")
	write("sub/secret.txt", "ignored by sub/.amberignore")
	write("sub/deeper/secret-2", "floating pattern applies in deeper subdirs")
	if err := os.Symlink("a.txt", filepath.Join(dir, "link.log")); err != nil {
		t.Fatal(err)
	}
}

// rootMatcher is a tiny helper for tests: the matcher for dir's own
// .amberignore.
func rootMatcher(t *testing.T, dir string) *amberignore.Matcher {
	t.Helper()
	m, err := amberignore.Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestBuildDir_HonorsAmberignore: filtering the full tree must produce the
// exact same root as ingesting a tree that never contained the ignored
// entries — files pruned, directories not descended, .amberignore stored.
func TestBuildDir_HonorsAmberignore(t *testing.T) {
	full := t.TempDir()
	writeIgnoredTree(t, full, false)
	prunedDir := t.TempDir()
	writeIgnoredTree(t, prunedDir, true)

	ic := chunkers.NewItemChunker(7)
	gotRoot, _ := collectSequential(t, full, rootMatcher(t, full), ic, nil, 256)
	wantRoot, _ := collectSequential(t, prunedDir, nil, ic, nil, 256)
	if gotRoot != wantRoot {
		t.Fatalf("filtered ingest root %s != pruned tree root %s", gotRoot, wantRoot)
	}
}

func TestIngestObjects_AmberignoreParity(t *testing.T) {
	dir := t.TempDir()
	writeIgnoredTree(t, dir, false)
	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, rootMatcher(t, dir), ic, nil, 256)
	parRoot, parObjs := collectParallel(t, dir, rootMatcher(t, dir), ic, nil, 256, 4)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	assertSameObjects(t, seqObjs, parObjs)
}

func TestBuildDir_NilMatcherIngestsEverything(t *testing.T) {
	full := t.TempDir()
	writeIgnoredTree(t, full, false)
	prunedDir := t.TempDir()
	writeIgnoredTree(t, prunedDir, true)

	ic := chunkers.NewItemChunker(7)
	fullRoot, _ := collectSequential(t, full, nil, ic, nil, 256)
	prunedRoot, _ := collectSequential(t, prunedDir, nil, ic, nil, 256)
	if fullRoot == prunedRoot {
		t.Fatal("nil matcher must ingest the ignored entries")
	}
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'Amberignore|NilMatcherIngests' -v`
Expected: FAIL (compile error — `collectSequential` does not take a matcher yet).

- [ ] **Step 3: Update `driver.go`**

Add `"github.com/draganm/amber-store/internal/amberignore"` to the imports, then replace `buildDir` and `buildEntry`:

```go
// buildDir builds the directory at path and returns its root key, emitting every
// object in its subtree (children before parents). Entries excluded by ign are
// skipped; excluded directories are pruned without being read.
func (d *driver) buildDir(path string, ign *amberignore.Matcher, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	db := fstree.NewDirBuilder(d.ic)
	for _, de := range ents {
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
		full := filepath.Join(path, de.Name())
		e, err := d.buildEntry(full, de.Name(), ign, emit, d.buildDir)
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}
```

In `buildEntry`, change the signature and the directory case (the rest of the function body is unchanged):

```go
func (d *driver) buildEntry(full, name string, ign *amberignore.Matcher, emit fstree.Emit, buildDir func(string, *amberignore.Matcher, fstree.Emit) (key.Key, error)) (fstree.Entry, error) {
```

```go
	case unix.S_IFDIR:
		sub, err := ign.Descend(full, name)
		if err != nil {
			return fstree.Entry{}, err
		}
		ck, err := buildDir(full, sub, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
```

- [ ] **Step 4: Update `ingest.go`**

Add the `amberignore` import. Change `ingestObjects` to accept and forward the matcher:

```go
func ingestObjects(dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, p *Progress, root *key.Key) iter.Seq2[fstree.Object, error] {
```
and inside the producer goroutine:
```go
			rk, err := b.buildDir(dir, ign, emit)
```

Replace `pbuilder.buildDir` (filter first so the result arrays are sized to the kept entries):

```go
// buildDir builds the directory at path and returns its root key. Entries
// excluded by ign are skipped (excluded directories are pruned without being
// read). Sibling entries are built concurrently; the directory's leaf/index
// objects are then emitted in sorted-entry order, identical to the sequential
// walk.
func (b *pbuilder) buildDir(path string, ign *amberignore.Matcher, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}
	kept := make([]os.DirEntry, 0, len(ents))
	for _, de := range ents {
		if !ign.Ignored(de.Name(), de.IsDir()) {
			kept = append(kept, de)
		}
	}

	entries := make([]fstree.Entry, len(kept))
	errs := make([]error, len(kept))
	var wg sync.WaitGroup

	for i, de := range kept {
		full := filepath.Join(path, de.Name())
		name := de.Name()
		build := func(i int, full, name string) {
			entries[i], errs[i] = b.d.buildEntry(full, name, ign, b.emit, b.buildDir)
		}
		select {
		case b.sem <- struct{}{}:
			wg.Add(1)
			go func(i int, full, name string) {
				defer wg.Done()
				defer func() { <-b.sem }()
				build(i, full, name)
			}(i, full, name)
		default:
			build(i, full, name)
		}
	}
	wg.Wait()

	db := fstree.NewDirBuilder(b.d.ic)
	for i, e := range entries {
		if errs[i] != nil {
			return key.Key{}, errs[i]
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}
```

Change `writePack` to accept and forward the matcher:

```go
func writePack(dst io.Writer, dir string, ign *amberignore.Matcher, cc *chunkConfig, jobs int, p *Progress) (key.Key, error) {
```
```go
	for o, err := range ingestObjects(dir, ign, cc.itemChunker(), byteOpts, cc.xattrInlineMax, jobs, p, &root) {
```

In `runIngest`, pass `nil` at both `writePack` call sites for now (real wiring is Task 4):
```go
		root, err = writePack(f, dir, nil, &cfg.chunk, cfg.jobs, prog)
```
```go
			r, err := writePack(pw, dir, nil, &cfg.chunk, cfg.jobs, prog)
```

- [ ] **Step 5: Update existing test call sites**

In `cmd/amber-store/ingest_test.go`, add the `amberignore` import and change the helpers:

```go
func collectSequential(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, map[key.Key][]byte) {
```
with `root, err := d.buildDir(dir, ign, emit)`, and

```go
func collectParallel(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax, jobs int) (key.Key, map[key.Key][]byte) {
```
with `for o, err := range ingestObjects(dir, ign, ic, byteOpts, xattrInlineMax, jobs, nil, &root)`.

Update their existing callers to pass `nil` after `dir`:
- line 85: `collectSequential(t, dir, nil, ic, nil, 256)`
- line 86: `collectParallel(t, dir, nil, ic, nil, 256, 4)`
- line 97: `collectSequential(t, dir, nil, ic, nil, 256)`
- line 99: `collectParallel(t, dir, nil, ic, nil, 256, jobs)`
- line 236 (`TestIngestObjects_ReportsProgress`): `ingestObjects(dir, nil, chunkers.NewItemChunker(7), nil, 256, 2, p, &root)`

In `cmd/amber-store/driver_test.go` line 23: `d.buildDir(dir, nil, emit)`.

- [ ] **Step 6: Run the full test suite**

Run: `gofmt -l cmd/amber-store/` (expect no output), then `go test ./...`
Expected: PASS — including the three new tests and all pre-existing parity/determinism tests (nil matcher leaves behavior identical).

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/driver.go cmd/amber-store/ingest.go cmd/amber-store/ingest_test.go cmd/amber-store/driver_test.go cmd/amber-store/amberignore_ingest_test.go
git commit -m "feat: ingest builders honor .amberignore files

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Filter the progress pre-scan

The progress bar is sized by `scanTree`; it must apply the identical filter or the totals overcount.

**Files:**
- Modify: `cmd/amber-store/scan.go` (`scanTree` line 20, `scanner.walk` lines 55-93)
- Modify: `cmd/amber-store/scan_test.go` (call sites at lines 25, 45, 55)
- Modify: `cmd/amber-store/ingest.go` (`runIngest` scanTree call at line 272 — pass `nil` for now)
- Modify: `cmd/amber-store/amberignore_ingest_test.go` (add two tests)

- [ ] **Step 1: Write the failing tests**

Append to `cmd/amber-store/amberignore_ingest_test.go` (add `"github.com/draganm/amber-store/key"` to its imports):

```go
// TestScanTree_HonorsAmberignore: the pre-scan must count exactly the entries
// the filtered ingest will read.
func TestScanTree_HonorsAmberignore(t *testing.T) {
	dir := t.TempDir()
	writeIgnoredTree(t, dir, false)
	prunedDir := t.TempDir()
	writeIgnoredTree(t, prunedDir, true)

	gotFiles, gotBytes, err := scanTree(dir, rootMatcher(t, dir), 4)
	if err != nil {
		t.Fatal(err)
	}
	wantFiles, wantBytes, err := scanTree(prunedDir, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	if gotFiles != wantFiles || gotBytes != wantBytes {
		t.Errorf("scanTree = (%d files, %d bytes), want (%d, %d)", gotFiles, gotBytes, wantFiles, wantBytes)
	}
}

// TestProgressTotalsMatchIngestWithAmberignore: end-to-end consistency — the
// scan's totals equal what the filtered ingest actually processes.
func TestProgressTotalsMatchIngestWithAmberignore(t *testing.T) {
	dir := t.TempDir()
	writeIgnoredTree(t, dir, false)
	files, bytes, err := scanTree(dir, rootMatcher(t, dir), 4)
	if err != nil {
		t.Fatal(err)
	}
	p := NewProgress(files, bytes)
	var root key.Key
	for _, err := range ingestObjects(dir, rootMatcher(t, dir), chunkers.NewItemChunker(7), nil, 256, 2, p, &root) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := p.filesDone.Load(); got != files {
		t.Errorf("filesDone = %d, scan predicted %d", got, files)
	}
	if got := p.bytesDone.Load(); got != bytes {
		t.Errorf("bytesDone = %d, scan predicted %d", got, bytes)
	}
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'ScanTree_HonorsAmberignore|ProgressTotalsMatch' -v`
Expected: FAIL (compile error — `scanTree` takes 2 arguments).

- [ ] **Step 3: Update `scan.go`**

Add the `amberignore` import. Update `scanTree`'s signature and doc comment tail, and `walk`:

```go
func scanTree(dir string, ign *amberignore.Matcher, jobs int) (files int64, bytes int64, err error) {
	if jobs < 1 {
		jobs = 1
	}
	s := &scanner{sem: make(chan struct{}, jobs)}
	s.walk(dir, ign)
	if e := s.err(); e != nil {
		return 0, 0, e
	}
	return s.files.Load(), s.bytes.Load(), nil
}
```

(Also extend the function's doc comment with: `Entries excluded by ign are skipped, mirroring the build walk, so the progress totals match what ingest actually reads.`)

```go
func (s *scanner) walk(dir string, ign *amberignore.Matcher) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		s.setErr(err)
		return
	}
	var wg sync.WaitGroup
	for _, de := range ents {
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
		full := filepath.Join(dir, de.Name())
		name := de.Name()
		do := func(full, name string) {
			info, err := os.Lstat(full)
			if err != nil {
				s.setErr(err)
				return
			}
			switch info.Mode() & os.ModeType {
			case os.ModeDir:
				sub, err := ign.Descend(full, name)
				if err != nil {
					s.setErr(err)
					return
				}
				s.walk(full, sub)
			case 0: // regular file
				s.files.Add(1)
				s.bytes.Add(info.Size())
			default:
				// symlink, device, socket, fifo — not read during ingest.
			}
		}
		select {
		case s.sem <- struct{}{}:
			wg.Add(1)
			go func(full, name string) {
				defer wg.Done()
				defer func() { <-s.sem }()
				do(full, name)
			}(full, name)
		default:
			do(full, name)
		}
	}
	wg.Wait()
}
```

- [ ] **Step 4: Update call sites**

- `cmd/amber-store/scan_test.go`: lines 25, 45, 55 become `scanTree(dir, nil, 4)`, `scanTree(dir, nil, 2)`, `scanTree(t.TempDir(), nil, 4)`.
- `cmd/amber-store/ingest.go` line 272 (`runIngest`): `scanTree(dir, nil, cfg.jobs)` (real wiring is Task 4).

- [ ] **Step 5: Run the full test suite**

Run: `gofmt -l cmd/amber-store/` (expect no output), then `go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/scan.go cmd/amber-store/scan_test.go cmd/amber-store/ingest.go cmd/amber-store/amberignore_ingest_test.go
git commit -m "feat: progress pre-scan honors .amberignore files

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Wire `runIngest` and add `--no-ignore`

**Files:**
- Modify: `cmd/amber-store/ingest.go` (`ingestConfig` line 158, `ingestCommand` flags lines 168-192, `runIngest` lines 232-366)
- Modify: `cmd/amber-store/amberignore_ingest_test.go` (add CLI-level tests)

- [ ] **Step 1: Write the failing tests**

Append to `cmd/amber-store/amberignore_ingest_test.go` (add `"bytes"` and `"strings"` to its imports):

```go
// ingestRootHex runs `amber-store ingest --no-progress -o <tmp> [args...]`
// through the real CLI and returns the printed root key (hex).
func ingestRootHex(t *testing.T, args ...string) string {
	t.Helper()
	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	out := filepath.Join(t.TempDir(), "p.amberpack")
	full := append([]string{"amber-store", "ingest", "--no-progress", "-o", out}, args...)
	if err := app.Run(full); err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(buf.String())
}

func TestRunIngest_HonorsAmberignore(t *testing.T) {
	full := t.TempDir()
	writeIgnoredTree(t, full, false)
	prunedDir := t.TempDir()
	writeIgnoredTree(t, prunedDir, true)
	if got, want := ingestRootHex(t, full), ingestRootHex(t, prunedDir); got != want {
		t.Errorf("ingest root %s != pruned tree root %s", got, want)
	}
}

func TestRunIngest_NoIgnoreFlag(t *testing.T) {
	full := t.TempDir()
	writeIgnoredTree(t, full, false)
	withIgnore := ingestRootHex(t, full)
	withoutIgnore := ingestRootHex(t, "--no-ignore", full)
	if withIgnore == withoutIgnore {
		t.Error("--no-ignore must include the ignored entries")
	}
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'TestRunIngest_HonorsAmberignore|TestRunIngest_NoIgnoreFlag' -v`
Expected: `TestRunIngest_NoIgnoreFlag` FAILS with `flag provided but not defined: -no-ignore`; `TestRunIngest_HonorsAmberignore` FAILS (roots differ — runIngest still passes nil).

- [ ] **Step 3: Implement**

In `ingestConfig` add a field:
```go
type ingestConfig struct {
	chunk      chunkConfig
	socket     string
	output     string
	jobs       int
	noProgress bool
	noIgnore   bool
}
```

In `ingestCommand`, append a flag after the `no-progress` one:
```go
		&cli.BoolFlag{
			Name:        "no-ignore",
			Usage:       "do not honor .amberignore files",
			Destination: &cfg.noIgnore,
		},
```

In `runIngest`, after the `if cfg.output != "" { ... } else { ... }` argument block (i.e. immediately before the `// Progress (client-side) ...` comment), construct the matcher:

```go
	// .amberignore filtering applies to the build walk and the progress
	// pre-scan alike, so the bar's totals match what is actually ingested.
	var ign *amberignore.Matcher
	if !cfg.noIgnore {
		m, err := amberignore.Root(dir)
		if err != nil {
			return err
		}
		ign = m
	}
```

Then replace the three `nil` placeholders:
- `scanTree(dir, ign, cfg.jobs)`
- `writePack(f, dir, ign, &cfg.chunk, cfg.jobs, prog)`
- `writePack(pw, dir, ign, &cfg.chunk, cfg.jobs, prog)`

- [ ] **Step 4: Run the full test suite**

Run: `gofmt -l cmd/amber-store/` (expect no output), then `go test ./...`
Expected: PASS, including both new CLI tests and the existing `TestRunIngest_*` tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/ingest.go cmd/amber-store/amberignore_ingest_test.go
git commit -m "feat: ingest --no-ignore flag; wire .amberignore into the CLI

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Document the feature

**Files:**
- Modify: `README.md` (after the parallelism/chunking paragraph, lines 127-130)

- [ ] **Step 1: Add the documentation**

In `README.md`, insert a new paragraph after the paragraph ending `...spill to an `XattrSet` object.` (line 130) and before `## Development`:

```markdown
`.amberignore` files exclude entries from ingestion, with `.gitignore`
semantics: negation (`!pattern`), `**` globs, directory-only (`name/`) and
anchored (`/name`) patterns; a file in any subdirectory applies to that
subtree and composes with inherited patterns (last match wins). Ignored
directories are pruned without being read. The `.amberignore` files
themselves are always stored, so a restored tree re-ingests to the same
root. `--no-ignore` disables all ignore processing.
```

- [ ] **Step 2: Verify and commit**

Run: `go test ./...` (one last full pass)
Expected: PASS.

```bash
git add README.md
git commit -m "docs: describe .amberignore ingest filtering

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-review notes (already applied)

- **Spec coverage:** per-directory composition (Task 1 `Descend` + Task 2 nested fixture), `.amberignore` always stored (Task 1 self-exclusion test + Task 2 root-equality oracle), go-git matcher (Task 1), `--no-ignore` (Task 4), all three walkers (Tasks 2-3), progress-total consistency (Task 3), directory pruning (Task 2 fixture's `build/`), unreadable-file error (Task 1), symlinks matched with `isDir=false` (Task 2 fixture's `link.log`).
- **Concurrency:** matchers are immutable; `appendCopy` prevents sibling goroutines from appending into a shared backing array during the parallel build.
- **Compile-green at every commit:** signature changes and their call-site updates (including `runIngest` `nil` placeholders) land in the same task.
