// Package allowfile manages the remote server's allowed-keys file as a
// live, editable object: the file on disk stays the source of truth
// (hand edits plus SIGHUP keep working), while the admin API adds and
// removes keys through atomic rewrites that hot-swap the in-memory
// allowlist the server consults per request.
package allowfile

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/draganm/amber-store/internal/allowlist"
	"golang.org/x/crypto/ssh"
)

// Key is one allowed key as the admin UI sees it.
type Key struct {
	Line        string `json:"line"`        // the authorized_keys line, verbatim
	Type        string `json:"type"`        // e.g. ssh-ed25519
	Fingerprint string `json:"fingerprint"` // SHA256:…
	Comment     string `json:"comment"`
	Admin       bool   `json:"admin"`
}

// File owns one allowed-keys file. Mutations rewrite it atomically
// (temp file + rename) and swap the parsed list; Current is lock-free
// so the server's per-request allowlist lookup never contends with an
// admin edit.
type File struct {
	path string
	mu   sync.Mutex // serializes file mutations and reloads
	list atomic.Pointer[allowlist.List]
}

// Open loads and validates the allowed-keys file at path.
func Open(path string) (*File, error) {
	f := &File{path: path}
	if err := f.Reload(); err != nil {
		return nil, err
	}
	return f, nil
}

// Current returns the live allowlist.
func (f *File) Current() *allowlist.List { return f.list.Load() }

// Reload re-reads the file from disk, e.g. after an external edit.
// On parse failure the previous list stays in effect.
func (f *File) Reload() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, err := allowlist.Load(f.path)
	if err != nil {
		return err
	}
	f.list.Store(l)
	return nil
}

// List returns the file's keys in file order.
func (f *File) List() ([]Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines()
	if err != nil {
		return nil, err
	}
	keys := []Key{}
	for _, line := range lines {
		k, ok := parseLine(line)
		if ok {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// Add validates line as a single authorized_keys entry and appends it.
// admin marks the key as an ops key (the "admin" option); the stored
// line is rebuilt in canonical form. Keys already in the file are
// rejected.
func (f *File) Add(line string, admin bool) error {
	pub, comment, options, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return fmt.Errorf("parsing key: %w", err)
	}
	if len(strings.TrimSpace(string(rest))) > 0 {
		return fmt.Errorf("trailing content after the key: %q", rest)
	}
	if admin && !slices.Contains(options, "admin") {
		options = append([]string{"admin"}, options...)
	}
	parts := []string{}
	if len(options) > 0 {
		parts = append(parts, strings.Join(options, ","))
	}
	parts = append(parts, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))))
	if comment != "" {
		parts = append(parts, comment)
	}
	canonical := strings.Join(parts, " ")

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.list.Load().Lookup(pub.Marshal()); ok {
		return fmt.Errorf("key %s is already in the allowlist", ssh.FingerprintSHA256(pub))
	}
	lines, err := f.readLines()
	if err != nil {
		return err
	}
	return f.write(append(lines, canonical))
}

// Remove drops every line carrying the key with the given SHA256
// fingerprint. An unknown fingerprint is an error.
func (f *File) Remove(fingerprint string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines()
	if err != nil {
		return err
	}
	kept := lines[:0]
	removed := false
	for _, line := range lines {
		if k, ok := parseLine(line); ok && k.Fingerprint == fingerprint {
			removed = true
			continue
		}
		kept = append(kept, line)
	}
	if !removed {
		return fmt.Errorf("no key with fingerprint %s in the allowlist", fingerprint)
	}
	return f.write(kept)
}

// readLines returns the file's lines without trailing newlines.
func (f *File) readLines() ([]string, error) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		return nil, fmt.Errorf("reading allowed keys: %w", err)
	}
	content := strings.TrimSuffix(string(b), "\n")
	if content == "" {
		return nil, nil
	}
	return strings.Split(content, "\n"), nil
}

// write validates the new content as a whole, persists it atomically
// and swaps the live list — callers hold f.mu.
func (f *File) write(lines []string) error {
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}
	l, err := allowlist.Parse([]byte(content))
	if err != nil {
		return err
	}
	mode := os.FileMode(0o600)
	if st, err := os.Stat(f.path); err == nil {
		mode = st.Mode().Perm()
	}
	tmp := filepath.Join(filepath.Dir(f.path), "."+filepath.Base(f.path)+".tmp")
	if err := os.WriteFile(tmp, []byte(content), mode); err != nil {
		return fmt.Errorf("writing allowed keys: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replacing allowed keys: %w", err)
	}
	f.list.Store(l)
	return nil
}

// parseLine reports whether line is an authorized_keys entry and, if
// so, the Key describing it. Comments and blanks return ok=false.
func parseLine(line string) (Key, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return Key{}, false
	}
	pub, comment, options, _, err := ssh.ParseAuthorizedKey([]byte(trimmed))
	if err != nil {
		return Key{}, false
	}
	return Key{
		Line:        trimmed,
		Type:        pub.Type(),
		Fingerprint: ssh.FingerprintSHA256(pub),
		Comment:     comment,
		Admin:       slices.Contains(options, "admin"),
	}, true
}
