// Package allowlist parses the remote server's allowed-keys file: standard
// authorized_keys format, one public key per line, comments and blank lines
// allowed. The options field may carry "admin", marking keys that bypass
// reference ownership and may delete references.
package allowlist

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"
)

// Entry is what the list records about one allowed key.
type Entry struct {
	Admin bool
}

// List is an immutable set of allowed keys; build a new one to reload.
// Lookup keys are SSH wire-format public keys.
type List struct {
	entries map[string]Entry
}

// Parse reads an authorized_keys-format buffer. Lines that are blank or
// start with '#' are skipped; any other unparsable line is an error (a
// silently dropped key would deny access with no trace).
// Duplicate keys are allowed; the last occurrence wins.
func Parse(b []byte) (*List, error) {
	l := &List{entries: map[string]Entry{}}
	sc := bufio.NewScanner(bytes.NewReader(b))
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		pub, _, options, _, err := ssh.ParseAuthorizedKey([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("allowed-keys line %d: %w", lineNo, err)
		}
		e := Entry{}
		for _, o := range options {
			if o == "admin" {
				e.Admin = true
			}
		}
		l.entries[string(pub.Marshal())] = e
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("allowed-keys line %d: %w", lineNo+1, err)
	}
	return l, nil
}

// Load reads and parses the file at path.
func Load(path string) (*List, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading allowed keys: %w", err)
	}
	l, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return l, nil
}

// Lookup reports whether the wire-format key is allowed, and its entry.
func (l *List) Lookup(pubWire []byte) (Entry, bool) {
	e, ok := l.entries[string(pubWire)]
	return e, ok
}
