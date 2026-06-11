package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/internal/amberignore"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
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
	if !pruned {
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
	normalizeMtimes(t, dir)
}

// normalizeMtimes pins every entry's mtime to a fixed instant. Ingested
// entries carry their Lstat mtime, so the equal-root oracle (filtered full
// tree vs. separately written pruned tree) only holds when both trees have
// identical timestamps. Children are touched before their parent (reverse
// pre-order), so directory mtimes are not disturbed afterwards.
func normalizeMtimes(t *testing.T, dir string) {
	t.Helper()
	fixed := time.Unix(1700000000, 0)
	var paths []string
	err := filepath.WalkDir(dir, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := len(paths) - 1; i >= 0; i-- {
		p := paths[i]
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			tv := unix.NsecToTimeval(fixed.UnixNano())
			if err := unix.Lutimes(p, []unix.Timeval{tv, tv}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.Chtimes(p, fixed, fixed); err != nil {
			t.Fatal(err)
		}
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
	if got := p.filesDone.Load(); int64(got) != files {
		t.Errorf("filesDone = %d, scan predicted %d", got, files)
	}
	if got := p.bytesDone.Load(); int64(got) != bytes {
		t.Errorf("bytesDone = %d, scan predicted %d", got, bytes)
	}
}

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
