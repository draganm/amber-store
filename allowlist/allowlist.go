// Package allowlist is the remote server's allowed-keys lookup set: SSH
// wire-format public key → entry. Parse builds one from authorized_keys-
// format content; New builds one from explicit entries.
//
// Entry describes a key's capabilities. Each key may be granted any
// combination of five capabilities as authorized_keys options:
// read (fetch objects, read/list refs), push-objects (upload packs),
// write-refs (store signed ref records), delegate (issue capability grants),
// and admin (ownership bypass, reference deletion; implies all other caps).
// A line with no options gets full non-admin access (legacy FullAccess).
// Unknown options are an error.
package allowlist

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"strings"

	"golang.org/x/crypto/ssh"
)

// Entry is what the list records about one allowed key: its capabilities.
// The zero value allows nothing. Admin implies every other capability (see
// Allows); a legacy line with no options gets FullAccess.
type Entry struct {
	Read        bool // fetch objects, read/list references
	PushObjects bool // upload packs
	WriteRefs   bool // store signed reference records
	Delegate    bool // issue capability grants (package grant)
	Admin       bool // ownership bypass and reference deletion; implies all
}

// Capability names, as authorized_keys options, allowstore records and grant
// caps lists spell them.
const (
	CapRead        = "read"
	CapPushObjects = "push-objects"
	CapWriteRefs   = "write-refs"
	CapDelegate    = "delegate"
	CapAdmin       = "admin"
)

// FullAccess is the legacy entry an option-less line gets: everything except
// delegation and admin.
func FullAccess() Entry { return Entry{Read: true, PushObjects: true, WriteRefs: true} }

// ParseCaps builds an Entry from capability names; an unknown name is an
// error — silently ignoring a typo would grant unintended access.
func ParseCaps(names []string) (Entry, error) {
	var e Entry
	for _, n := range names {
		switch n {
		case CapRead:
			e.Read = true
		case CapPushObjects:
			e.PushObjects = true
		case CapWriteRefs:
			e.WriteRefs = true
		case CapDelegate:
			e.Delegate = true
		case CapAdmin:
			e.Admin = true
		default:
			return Entry{}, fmt.Errorf("unknown capability %q", n)
		}
	}
	return e, nil
}

// Allows reports whether e includes the named capability. Admin implies
// everything, so an admin entry needs no other flags set regardless of how it
// was constructed.
func (e Entry) Allows(cap string) bool {
	if e.Admin {
		return true
	}
	switch cap {
	case CapRead:
		return e.Read
	case CapPushObjects:
		return e.PushObjects
	case CapWriteRefs:
		return e.WriteRefs
	case CapDelegate:
		return e.Delegate
	}
	return false
}

// Caps renders the canonical capability names for e; an admin entry renders
// as just "admin" (it implies the rest).
func (e Entry) Caps() []string {
	if e.Admin {
		return []string{CapAdmin}
	}
	var out []string
	if e.Read {
		out = append(out, CapRead)
	}
	if e.PushObjects {
		out = append(out, CapPushObjects)
	}
	if e.WriteRefs {
		out = append(out, CapWriteRefs)
	}
	if e.Delegate {
		out = append(out, CapDelegate)
	}
	return out
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
		e := FullAccess()
		if len(options) > 0 {
			var perr error
			e, perr = ParseCaps(options)
			if perr != nil {
				return nil, fmt.Errorf("allowed-keys line %d: %w", lineNo, perr)
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
