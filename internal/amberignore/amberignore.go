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
	// Only the ignore *file* is protected: a directory named .amberignore is
	// matched like any other directory, mirroring gitignore semantics.
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
	ps := append([]gitignore.Pattern(nil), inherited...)
	for line := range strings.SplitSeq(string(data), "\n") {
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
