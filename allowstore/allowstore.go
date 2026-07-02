// Package allowstore persists the remote server's allowed client keys in a
// Pebble DB under the store directory: SSH wire-format public key → JSON
// record {admin, comment, caps}. The DB is the sole source of truth and the
// admin API is its only writer; a lock-free snapshot of the allowlist
// serves the per-request lookup. A record with an empty caps field keeps
// the legacy meaning: admin if admin:true, else full non-admin access.
package allowstore

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/draganm/amber-store/allowlist"
	"golang.org/x/crypto/ssh"
)

// Key is one allowed key as the admin UI sees it. Line is the canonical
// authorized_keys rendering of the stored record.
type Key struct {
	Line        string   `json:"line"`
	Type        string   `json:"type"`        // e.g. ssh-ed25519
	Fingerprint string   `json:"fingerprint"` // SHA256:…
	Comment     string   `json:"comment"`
	Admin       bool     `json:"admin"`
	Caps        []string `json:"caps,omitempty"`
}

// record is the stored value for one key. An empty Caps keeps the pre-caps
// meaning: admin if Admin, full non-admin access otherwise. A non-empty Caps
// is authoritative.
type record struct {
	Admin   bool     `json:"admin"`
	Comment string   `json:"comment"`
	Caps    []string `json:"caps,omitempty"`
}

// entry resolves the record to its effective capabilities. Undecodable caps
// fail closed (scan validates, so this is belt and braces).
func (r record) entry() allowlist.Entry {
	if len(r.Caps) == 0 {
		if r.Admin {
			return allowlist.Entry{Admin: true}
		}
		return allowlist.FullAccess()
	}
	e, err := allowlist.ParseCaps(r.Caps)
	if err != nil {
		return allowlist.Entry{}
	}
	return e
}

// Store is a Pebble-backed allowed-keys set. Current is lock-free so the
// server's per-request allowlist lookup never contends with an admin
// edit; mutations serialize under mu, write to the DB, and swap the
// in-memory state.
type Store struct {
	db        *pebble.DB
	writeOpts *pebble.WriteOptions

	mu   sync.Mutex        // guards recs and DB writes
	recs map[string]record // wire-format key → record, mirrors the DB
	list atomic.Pointer[allowlist.List]
}

// discardLogger silences pebble's internal logging (same trick as refstore).
type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("allowstore: pebble fatal: "+format, args...))
}

// Open opens (creating if missing) the allowed-keys DB at dir. sync
// selects the write durability, matching the server's --sync flag.
func Open(dir string, sync bool) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		return nil, fmt.Errorf("allowstore: opening pebble: %w", err)
	}
	wo := pebble.Sync
	if !sync {
		wo = pebble.NoSync
	}
	s := &Store{db: db, writeOpts: wo, recs: map[string]record{}}
	if err := s.scan(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.swap()
	return s, nil
}

// scan loads every record from the DB into recs.
func (s *Store) scan() error {
	it, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		pub, err := ssh.ParsePublicKey(it.Key())
		if err != nil {
			return fmt.Errorf("allowstore: stored key does not parse: %w", err)
		}
		var rec record
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return fmt.Errorf("allowstore: decoding record: %w", err)
		}
		if _, err := allowlist.ParseCaps(rec.Caps); err != nil {
			return fmt.Errorf("allowstore: record for %s: %w", ssh.FingerprintSHA256(pub), err)
		}
		s.recs[string(it.Key())] = rec
	}
	return it.Error()
}

// swap rebuilds the lock-free allowlist snapshot from recs — callers hold
// mu (or have exclusive access, as in Open).
func (s *Store) swap() {
	entries := make(map[string]allowlist.Entry, len(s.recs))
	for wire, rec := range s.recs {
		entries[wire] = rec.entry()
	}
	s.list.Store(allowlist.New(entries))
}

// Current returns the live allowlist.
func (s *Store) Current() *allowlist.List { return s.list.Load() }

// List returns the keys ordered bytewise by wire-format key — the DB's
// own deterministic order. File-era insertion order is gone.
func (s *Store) List() []Key {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]Key, 0, len(s.recs))
	for _, wire := range slices.Sorted(maps.Keys(s.recs)) {
		pub, err := ssh.ParsePublicKey([]byte(wire))
		if err != nil {
			continue // unreachable: scan and Add only store parsed keys
		}
		keys = append(keys, makeKey(pub, s.recs[wire]))
	}
	return keys
}

// makeKey renders one stored record for the admin UI.
func makeKey(pub ssh.PublicKey, rec record) Key {
	parts := []string{}
	switch {
	case rec.Admin && len(rec.Caps) == 0:
		parts = append(parts, "admin")
	case len(rec.Caps) > 0:
		parts = append(parts, strings.Join(rec.Caps, ","))
	}
	parts = append(parts, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))))
	if rec.Comment != "" {
		parts = append(parts, rec.Comment)
	}
	return Key{
		Line:        strings.Join(parts, " "),
		Type:        pub.Type(),
		Fingerprint: ssh.FingerprintSHA256(pub),
		Comment:     rec.Comment,
		Admin:       rec.Admin,
		Caps:        rec.Caps,
	}
}

// Add validates line as a single authorized_keys entry and stores it.
// admin marks the key as an ops key, equivalent to the "admin" option on
// the line; any other option is rejected — the record has nowhere to keep
// it, and silently dropping it would lie to the operator. Keys already in
// the store are rejected.
func (s *Store) Add(line string, admin bool) error {
	pub, comment, options, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return fmt.Errorf("parsing key: %w", err)
	}
	if len(strings.TrimSpace(string(rest))) > 0 {
		return fmt.Errorf("trailing content after the key: %q", rest)
	}
	rec := record{Comment: comment}
	if len(options) > 0 {
		ent, err := allowlist.ParseCaps(options)
		if err != nil {
			return fmt.Errorf("parsing key options: %w", err)
		}
		rec.Caps = ent.Caps()
		rec.Admin = ent.Admin
	}
	if admin {
		rec.Admin = true
		rec.Caps = nil // admin is total; the legacy empty-caps form says it best
	}
	val, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	wire := string(pub.Marshal())
	if _, ok := s.recs[wire]; ok {
		return fmt.Errorf("key %s is already in the allowlist", ssh.FingerprintSHA256(pub))
	}
	if err := s.db.Set([]byte(wire), val, s.writeOpts); err != nil {
		return fmt.Errorf("storing key: %w", err)
	}
	s.recs[wire] = rec
	s.swap()
	return nil
}

// Remove drops the key with the given SHA256 fingerprint. An unknown
// fingerprint is an error.
func (s *Store) Remove(fingerprint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for wire := range s.recs {
		pub, err := ssh.ParsePublicKey([]byte(wire))
		if err != nil {
			continue // unreachable: scan and Add only store parsed keys
		}
		if ssh.FingerprintSHA256(pub) != fingerprint {
			continue
		}
		if err := s.db.Delete([]byte(wire), s.writeOpts); err != nil {
			return fmt.Errorf("deleting key: %w", err)
		}
		delete(s.recs, wire)
		s.swap()
		return nil
	}
	return fmt.Errorf("no key with fingerprint %s in the allowlist", fingerprint)
}

// Close closes the DB.
func (s *Store) Close() error { return s.db.Close() }
