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
		{"build", true, true},   // dir-only pattern matches the directory
		{"build", false, false}, // ...but not a regular file of the same name
		{"anchored.txt", false, true},
	}
	for _, c := range cases {
		if got := m.Ignored(c.name, c.isDir); got != c.want {
			t.Errorf("Ignored(%q, isDir=%v) = %v, want %v", c.name, c.isDir, got, c.want)
		}
	}
}

func TestCRLFLineEndings(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "*.log\r\nbuild/\r\n")
	m, err := Root(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Ignored("app.log", false) {
		t.Error("CRLF-terminated *.log must still match")
	}
	if !m.Ignored("build", true) {
		t.Error("CRLF-terminated build/ must still match")
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
	sub, err := m.Descend(filepath.Join(dir, "sub"), "sub")
	if err != nil {
		t.Fatal(err)
	}
	if sub.Ignored(FileName, false) {
		t.Error("nested .amberignore must always be ingested")
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
