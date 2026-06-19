// Package allowlist is the remote server's allowed-keys lookup set: SSH
// wire-format public key → entry. Parse builds one from authorized_keys-
// format content; New builds one from explicit entries. A key marked
// "admin" bypasses reference ownership and may delete references.
package allowlist

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
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

// New builds a List from wire-format key → Entry pairs, copying the map.
func New(entries map[string]Entry) *List {
	m := make(map[string]Entry, len(entries))
	maps.Copy(m, entries)
	return &List{entries: m}
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

// Lookup reports whether the wire-format key is allowed, and its entry.
func (l *List) Lookup(pubWire []byte) (Entry, bool) {
	e, ok := l.entries[string(pubWire)]
	return e, ok
}
