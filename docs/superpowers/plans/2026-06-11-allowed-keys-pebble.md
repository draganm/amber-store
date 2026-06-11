# Allowed Keys in Pebble Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the remote server's allowed client keys from an authorized_keys file into a Pebble database under the store directory, managed exclusively through the admin UI.

**Architecture:** A new `internal/allowstore` package owns a Pebble DB at `<store>/allowed-keys` (wire-format SSH public key → JSON `{admin, comment}`) and keeps a lock-free `*allowlist.List` snapshot for the server's per-request lookup, mirroring the concurrency model of the old `internal/allowfile`. The admin handler swaps its dependency to the new store; `serve.go` drops the `--allowed-keys` flag and the SIGHUP reload. `internal/allowfile` and `allowlist.Load` are deleted.

**Tech Stack:** Go, `github.com/cockroachdb/pebble/v2` (already a dependency; copy the patterns in `refstore/refstore.go`), `golang.org/x/crypto/ssh`.

**Spec:** `docs/superpowers/specs/2026-06-11-allowed-keys-pebble-design.md`

Run all commands from the repo root `/Users/dragan/draganm/amber-store`.

---

### Task 1: `allowlist.New` constructor

The new store builds its lookup list from a map, not from authorized_keys text. Add a constructor to `internal/allowlist`. (`allowlist.Load` is NOT removed here — `allowfile` still uses it until Task 5.)

**Files:**
- Modify: `internal/allowlist/allowlist.go`
- Test: `internal/allowlist/allowlist_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/allowlist/allowlist_test.go`:

```go
func TestNewCopiesEntries(t *testing.T) {
	pub := testPub(t)
	entries := map[string]allowlist.Entry{
		string(pub.Marshal()): {Admin: true},
	}
	l := allowlist.New(entries)
	// mutating the caller's map must not affect the built list
	delete(entries, string(pub.Marshal()))
	if e, ok := l.Lookup(pub.Marshal()); !ok || !e.Admin {
		t.Fatalf("key: ok=%v admin=%v, want ok, admin", ok, e.Admin)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/allowlist/ -run TestNewCopiesEntries -v`
Expected: FAIL to build with `undefined: allowlist.New`

- [ ] **Step 3: Implement `New`**

In `internal/allowlist/allowlist.go`, add `"maps"` to the imports and add after the `List` type declaration:

```go
// New builds a List from wire-format key → Entry pairs, copying the map.
func New(entries map[string]Entry) *List {
	m := make(map[string]Entry, len(entries))
	maps.Copy(m, entries)
	return &List{entries: m}
}
```

- [ ] **Step 4: Run the package tests**

Run: `go test ./internal/allowlist/ -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/allowlist/
git commit -m "feat: allowlist.New builds a list from explicit entries"
```

---

### Task 2: `internal/allowstore` package

The Pebble-backed replacement for `allowfile`. Same external shape (`Current`, `List`, `Add`, `Remove`), plus `Close`. One deliberate difference: `List()` returns no error (it reads an in-memory mirror) and its order is bytewise by wire-format key, not insertion order.

**Files:**
- Create: `internal/allowstore/allowstore.go`
- Create: `internal/allowstore/allowstore_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/allowstore/allowstore_test.go`:

```go
package allowstore_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/allowstore"
	"golang.org/x/crypto/ssh"
)

func testKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))
}

func open(t *testing.T, dir string) *allowstore.Store {
	t.Helper()
	s, err := allowstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

// findKey returns the listed key with the given fingerprint, or fails.
func findKey(t *testing.T, keys []allowstore.Key, fingerprint string) allowstore.Key {
	t.Helper()
	for _, k := range keys {
		if k.Fingerprint == fingerprint {
			return k
		}
	}
	t.Fatalf("no key with fingerprint %s in %+v", fingerprint, keys)
	return allowstore.Key{}
}

func TestOpenEmpty(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	if keys := s.List(); len(keys) != 0 {
		t.Fatalf("fresh store lists %d keys, want 0", len(keys))
	}
	pub, _ := testKey(t)
	if _, ok := s.Current().Lookup(pub.Marshal()); ok {
		t.Fatal("fresh store allows a key")
	}
}

func TestAddListsAndSwapsList(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	plain, plainLine := testKey(t)
	admin, adminLine := testKey(t)
	if err := s.Add(plainLine+" laptop", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("admin "+adminLine+" ops", false); err != nil {
		t.Fatal(err)
	}
	keys := s.List()
	if len(keys) != 2 {
		t.Fatalf("listed %d keys, want 2", len(keys))
	}
	p := findKey(t, keys, ssh.FingerprintSHA256(plain))
	if p.Admin || p.Comment != "laptop" || p.Type != "ssh-ed25519" || p.Line != plainLine+" laptop" {
		t.Fatalf("plain key listed wrong: %+v", p)
	}
	a := findKey(t, keys, ssh.FingerprintSHA256(admin))
	if !a.Admin || a.Comment != "ops" || a.Line != "admin "+adminLine+" ops" {
		t.Fatalf("admin key listed wrong: %+v", a)
	}
	if e, ok := s.Current().Lookup(plain.Marshal()); !ok || e.Admin {
		t.Fatalf("plain key live lookup: ok=%v admin=%v, want allowed non-admin", ok, e.Admin)
	}
	if e, ok := s.Current().Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key live lookup: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
}

func TestAddAdminFlagSetsOption(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	added, line := testKey(t)
	if err := s.Add(line, true); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.Current().Lookup(added.Marshal()); !ok || !e.Admin {
		t.Fatalf("added key: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
	k := findKey(t, s.List(), ssh.FingerprintSHA256(added))
	if !k.Admin || !strings.HasPrefix(k.Line, "admin ") {
		t.Fatalf("listed key not admin: %+v", k)
	}
}

func TestAddRejectsBadLines(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	_, existing := testKey(t)
	if err := s.Add(existing, false); err != nil {
		t.Fatal(err)
	}
	_, fresh := testKey(t)
	for name, line := range map[string]string{
		"garbage":            "not a key",
		"duplicate":          existing,
		"trailing content":   fresh + " comment\n" + fresh,
		"unsupported option": "no-pty " + fresh,
	} {
		if err := s.Add(line, false); err == nil {
			t.Fatalf("Add(%s) succeeded, want error", name)
		}
	}
}

func TestRemove(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	gone, goneLine := testKey(t)
	stay, stayLine := testKey(t)
	if err := s.Add(goneLine, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("admin "+stayLine, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ssh.FingerprintSHA256(gone)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Current().Lookup(gone.Marshal()); ok {
		t.Fatal("removed key still in live list")
	}
	if _, ok := s.Current().Lookup(stay.Marshal()); !ok {
		t.Fatal("unrelated key lost")
	}
	if keys := s.List(); len(keys) != 1 {
		t.Fatalf("list after remove has %d keys, want 1", len(keys))
	}
}

func TestRemoveUnknownFingerprint(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	if err := s.Remove("SHA256:doesnotexist"); err == nil {
		t.Fatal("want error for unknown fingerprint")
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "allowed-keys")
	s, err := allowstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	plain, plainLine := testKey(t)
	admin, adminLine := testKey(t)
	if err := s.Add(plainLine+" laptop", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(adminLine+" ops", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := open(t, dir)
	keys := s2.List()
	if len(keys) != 2 {
		t.Fatalf("reopened store lists %d keys, want 2", len(keys))
	}
	p := findKey(t, keys, ssh.FingerprintSHA256(plain))
	if p.Admin || p.Comment != "laptop" {
		t.Fatalf("plain key survived wrong: %+v", p)
	}
	if e, ok := s2.Current().Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key after reopen: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/allowstore/ -v`
Expected: FAIL to build — the package does not exist yet.

- [ ] **Step 3: Implement the store**

Create `internal/allowstore/allowstore.go`:

```go
// Package allowstore persists the remote server's allowed client keys in a
// Pebble DB under the store directory: SSH wire-format public key → JSON
// record {admin, comment}. The DB is the sole source of truth and the
// admin API is its only writer; a lock-free snapshot of the allowlist
// serves the per-request lookup.
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
	"github.com/draganm/amber-store/internal/allowlist"
	"golang.org/x/crypto/ssh"
)

// Key is one allowed key as the admin UI sees it. Line is the canonical
// authorized_keys rendering of the stored record.
type Key struct {
	Line        string `json:"line"`
	Type        string `json:"type"`        // e.g. ssh-ed25519
	Fingerprint string `json:"fingerprint"` // SHA256:…
	Comment     string `json:"comment"`
	Admin       bool   `json:"admin"`
}

// record is the stored value for one key.
type record struct {
	Admin   bool   `json:"admin"`
	Comment string `json:"comment"`
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
		if _, err := ssh.ParsePublicKey(it.Key()); err != nil {
			return fmt.Errorf("allowstore: stored key does not parse: %w", err)
		}
		var rec record
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return fmt.Errorf("allowstore: decoding record: %w", err)
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
		entries[wire] = allowlist.Entry{Admin: rec.Admin}
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
	if rec.Admin {
		parts = append(parts, "admin")
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
	rec := record{Admin: admin, Comment: comment}
	for _, o := range options {
		if o != "admin" {
			return fmt.Errorf("unsupported key option %q", o)
		}
		rec.Admin = true
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/allowstore/ -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/allowstore/
git commit -m "feat: pebble-backed allowed-keys store"
```

---

### Task 3: Admin handler uses the store

Swap `*allowfile.File` for `*allowstore.Store` in the admin package. `List()` no longer returns an error, so `listKeys` simplifies. The admin JSON API shape is unchanged — the SPA is untouched.

**Files:**
- Modify: `admin/admin.go`
- Modify: `admin/admin_test.go`

- [ ] **Step 1: Update the tests to the new dependency**

In `admin/admin_test.go`:

1. In the imports, replace `"os"` and the allowfile import:

```go
	"github.com/draganm/amber-store/internal/allowstore"
```

(keep `"path/filepath"`; delete the `"os"` import — nothing reads or writes files anymore).

2. Replace `testServer` (the store replaces the file; there is no path to return):

```go
// testServer returns a started admin server and the allowed-keys store
// behind it, seeded with one key.
func testServer(t *testing.T) (*httptest.Server, *allowstore.Store) {
	t.Helper()
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := keys.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	_, line := testKey(t)
	if err := keys.Add(line, false); err != nil {
		t.Fatal(err)
	}
	h, err := admin.New(admin.Config{Password: password, Keys: keys, UI: testUI})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, keys
}
```

3. Every `srv, _, _ := testServer(t)` becomes `srv, _ := testServer(t)` (in `TestSessionLifecycle`, `TestKeysRequireSession`, `TestAddInvalidKey`, `TestDeleteUnknownKey`, `TestCrossOriginMutationRejected`, `TestServesSPA`).

4. In `keysOf`, change `[]allowfile.Key` to `[]allowstore.Key` (both the return type and the `out` struct field).

5. Replace `TestKeysCRUD`. Two adaptations: persistence is no longer checked by reading a file (allowstore's own reopen test covers it), and the added key is found by comment instead of by index — `List()` orders by wire-key bytes, so the new key may sort anywhere:

```go
func TestKeysCRUD(t *testing.T) {
	srv, keys := testServer(t)
	cookie := login(t, srv)

	listed := keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 1 {
		t.Fatalf("initial list has %d keys, want 1", len(listed))
	}

	pub, line := testKey(t)
	resp := do(t, "POST", srv.URL+"/admin/api/keys", cookie,
		`{"line":"`+line+` web-added","admin":true}`)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("add key = %d, want 204 (%s)", resp.StatusCode, b)
	}
	if e, ok := keys.Current().Lookup(pub.Marshal()); !ok || !e.Admin {
		t.Fatalf("added key live lookup: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}

	listed = keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 2 {
		t.Fatalf("list after add has %d keys, want 2", len(listed))
	}
	var added *allowstore.Key
	for i := range listed {
		if listed[i].Comment == "web-added" {
			added = &listed[i]
		}
	}
	if added == nil {
		t.Fatalf("added key not listed: %+v", listed)
	}
	if !added.Admin {
		t.Fatalf("added key listed wrong: %+v", added)
	}

	resp = do(t, "DELETE",
		srv.URL+"/admin/api/keys?fingerprint="+strings.ReplaceAll(added.Fingerprint, "+", "%2B"),
		cookie, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete key = %d, want 204", resp.StatusCode)
	}
	if _, ok := keys.Current().Lookup(pub.Marshal()); ok {
		t.Fatal("deleted key still in live list")
	}
	listed = keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 1 {
		t.Fatalf("list after delete has %d keys, want 1", len(listed))
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./admin/ -v`
Expected: FAIL to build — `admin.Config.Keys` is still `*allowfile.File`.

- [ ] **Step 3: Switch the handler to the store**

In `admin/admin.go`:

1. In the imports, replace the allowfile import with:

```go
	"github.com/draganm/amber-store/internal/allowstore"
```

2. In `Config`, change the `Keys` field:

```go
	Keys      *allowstore.Store // the live allowed-keys store
```

3. In `handler`, change the `keys` field:

```go
	keys      *allowstore.Store
```

4. Replace `listKeys` (the store's `List` cannot fail):

```go
func (h *handler) listKeys(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": h.keys.List()})
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./admin/ -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add admin/
git commit -m "feat: admin API manages keys in the pebble allowstore"
```

---

### Task 4: serve wiring — drop the flag and SIGHUP, open the store

`serve.go` opens the allowstore at `<store>/allowed-keys`, the `--allowed-keys` flag and SIGHUP reload disappear, and an empty allowlist logs a warning at startup. The CLI tests adapt.

**Files:**
- Modify: `cmd/amber-store/serve.go`
- Modify: `cmd/amber-store/serve_test.go`
- Modify: `cmd/amber-store/serve_admin_test.go`

- [ ] **Step 1: Update the CLI tests**

In `cmd/amber-store/serve_test.go`:

1. Replace `writeServeFixtures` with an identity-only fixture (the imports lose nothing — every package is still used):

```go
// writeIdentityFixture writes an unencrypted SSH identity key file.
func writeIdentityFixture(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(t.TempDir(), "identity")
	if err := os.WriteFile(identityPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return identityPath
}
```

2. Replace `TestServeRequiresFlags` — `--store` is the only required flag now:

```go
func TestServeRequiresStoreFlag(t *testing.T) {
	identity := writeIdentityFixture(t)
	if err := newApp().Run([]string{"amber-store", "serve", "--identity", identity}); err == nil {
		t.Fatal("serve without --store succeeded, want missing-flag error")
	}
}
```

3. In `TestServeAutoIdentity`, delete the `_, allowed := writeServeFixtures(t)` line and the `"--allowed-keys", allowed,` arguments:

```go
	go func() {
		done <- newApp().RunContext(ctx, []string{
			"amber-store", "serve", "--store", storeDir,
			"--listen", addr, "--sync=false",
		})
	}()
```

4. In `TestServeRejectsTLSHalfConfig`:

```go
func TestServeRejectsTLSHalfConfig(t *testing.T) {
	identity := writeIdentityFixture(t)
	err := newApp().Run([]string{
		"amber-store", "serve", "--store", t.TempDir(),
		"--identity", identity,
		"--tls-cert", "/nonexistent/cert.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("err = %v, want tls flag pairing error", err)
	}
}
```

5. In `TestServeRejectsEncryptedIdentityFile`, delete the `_, allowed := writeServeFixtures(t)` line and the `"--allowed-keys", allowed,` argument:

```go
	err = newApp().Run([]string{
		"amber-store", "serve", "--store", t.TempDir(),
		"--identity", encPath,
	})
```

In `cmd/amber-store/serve_admin_test.go`:

6. Add to the imports:

```go
	"crypto/ed25519"
	"crypto/rand"

	"golang.org/x/crypto/ssh"
```

7. In `startServe`, replace the fixtures line and the serve arguments:

```go
	identity := writeIdentityFixture(t)
```

```go
		done <- newApp().RunContext(ctx, []string{
			"amber-store", "serve", "--store", t.TempDir(),
			"--identity", identity,
			"--listen", addr, "--sync=false",
		})
```

8. In `TestServeAdminUI`, the store starts empty, so the test now adds a key through the API before listing. Replace everything after the `if session == nil { t.Fatal(...) }` block with:

```go
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub)))
	req, err := http.NewRequest("POST", base+"/admin/api/keys",
		strings.NewReader(`{"line":"`+line+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(session)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("add key = %d, want 204", resp.StatusCode)
	}

	req, err = http.NewRequest("GET", base+"/admin/api/keys", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(session)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "ssh-ed25519") {
		t.Fatalf("keys = %d %q, want the added key listed", resp.StatusCode, body)
	}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'TestServe' -v`
Expected: FAIL — `serve` still requires the removed `--allowed-keys` flag (and `writeServeFixtures` no longer exists, so the package fails to build until serve.go changes too; either failure mode is fine).

- [ ] **Step 3: Rewire serve.go**

In `cmd/amber-store/serve.go`:

1. In the imports, replace the allowfile import with:

```go
	"github.com/draganm/amber-store/internal/allowstore"
```

(`os`, `os/signal`, and `syscall` stay — `signal.NotifyContext`, `os.Interrupt`, and `syscall.SIGTERM` still use them.)

2. In `serveConfig`, delete the `allowedKeys string` field.

3. In `serveCommand`, delete the whole `&cli.StringFlag{Name: "allowed-keys", ...}` block.

4. In `runServe`, delete this whole block (the allowfile open and the SIGHUP loop):

```go
	keys, err := allowfile.Open(cfg.allowedKeys)
	if err != nil {
		return err
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for range hup {
			if err := keys.Reload(); err != nil {
				logger.Error("allowlist reload failed; keeping the previous list", "error", err)
				continue
			}
			logger.Info("allowlist reloaded", "path", cfg.allowedKeys)
		}
	}()
```

5. After the `refs, err := refstore.Open(...)` block (and its `defer refs.Close()`), add:

```go
	keys, err := allowstore.Open(filepath.Join(cfg.store, "allowed-keys"), cfg.sync)
	if err != nil {
		return err
	}
	defer keys.Close()
	if len(keys.List()) == 0 {
		if cfg.adminPassword == "" {
			logger.Warn("the allowlist is empty and the admin UI is disabled; this server cannot authorize anyone (set AMBER_ADMIN_PASSWORD to manage keys)")
		} else {
			logger.Warn("the allowlist is empty; add keys via the admin UI at /admin/")
		}
	}
```

The `server.New(...)` and `admin.New(...)` calls are unchanged — `keys.Current` and `Keys: keys` keep working against the new type.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./cmd/amber-store/ -v`
Expected: all PASS (including the remote tests, which build the server directly and are untouched).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/
git commit -m "feat: serve stores allowed keys in a pebble database

The --allowed-keys flag and the SIGHUP reload are gone; the admin UI is
the only way to manage keys. A fresh server warns that its allowlist is
empty."
```

---

### Task 5: Delete allowfile and the file-loading path

Nothing imports `internal/allowfile` anymore; `allowlist.Load` loses its last caller.

**Files:**
- Delete: `internal/allowfile/allowfile.go`
- Delete: `internal/allowfile/allowfile_test.go`
- Modify: `internal/allowlist/allowlist.go`
- Modify: `internal/allowlist/allowlist_test.go`

- [ ] **Step 1: Delete the allowfile package**

```bash
git rm -r internal/allowfile
```

- [ ] **Step 2: Remove `allowlist.Load`**

In `internal/allowlist/allowlist.go`:

1. Delete the `Load` function (the whole block):

```go
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
```

2. Remove `"os"` from the imports.

3. Replace the package doc comment — the package no longer reads files:

```go
// Package allowlist is the remote server's allowed-keys lookup set: SSH
// wire-format public key → entry. Parse builds one from authorized_keys-
// format content; New builds one from explicit entries. A key marked
// "admin" bypasses reference ownership and may delete references.
```

4. In `internal/allowlist/allowlist_test.go`, delete `TestLoad` and remove the now-unused `"os"` and `"path/filepath"` imports.

- [ ] **Step 3: Verify everything still builds and passes**

Run: `go build ./... && go test ./internal/... ./admin/ ./cmd/amber-store/`
Expected: build OK, all PASS

- [ ] **Step 4: Commit**

```bash
git add -A internal/allowlist internal/allowfile
git commit -m "refactor: drop allowfile and the allowed-keys file loader"
```

---

### Task 6: Documentation

Update the three documents that describe the allowed-keys file. Historical specs and plans under `docs/superpowers/` stay as written.

**Files:**
- Modify: `architecture/remote.md`
- Modify: `architecture/references.md`
- Modify: `README.md`

- [ ] **Step 1: Update `architecture/remote.md`**

1. In "The shape" code block, delete the `--allowed-keys FILE \` line (and its comment).

2. Replace the "**Server side:**" paragraph in "Identity and trust":

```markdown
**Server side:** allowed client keys live in a Pebble database at
`<store>/allowed-keys`, one entry per SSH public key. An entry may be marked
`admin` for operations keys that bypass ownership checks and are permitted
to delete references. The admin UI is the only way to manage the set; a
fresh server allows nobody and logs a warning until keys are added.
```

3. Replace the "**Admin UI.**" paragraph:

```markdown
**Admin UI.** Setting `AMBER_ADMIN_PASSWORD` (or `--admin-password`) enables a
browser console at `/admin/` — a solid-js SPA embedded in the binary
(`go generate ./cmd/amber-store` rebuilds it) — where an operator signs in
with that password and inspects, adds, and removes allowed keys. Edits write
straight to the allowed-keys database and take effect immediately. Sessions
are in-memory cookies (12h); when the password is not configured, the
`/admin/` surface does not exist.
```

4. In "**Default identities.**", replace the last sentence:

```markdown
`identity.pub` is what an operator pastes into a server's admin UI to
authorize a daemon.
```

5. In "Server-side verification order", replace item 4:

```markdown
4. Public key present in the allowed-keys database.
```

- [ ] **Step 2: Update `architecture/references.md`**

In the "Ownership" paragraph (~line 97), replace:

```markdown
same signer key. A different signer → `403`. Transport keys marked `admin` in
the server's allowed-keys database bypass this check — they are the operations
override for lockout or migration.
```

- [ ] **Step 3: Update `README.md`**

Replace the serve example (~lines 105–107):

```markdown
# on the server host — owns its own store, signs every response
amber-store serve --store /path/to/remote-store --listen :8590 \
  --identity /etc/amber/server_key
# authorize clients via the admin UI: set AMBER_ADMIN_PASSWORD and open /admin/
```

- [ ] **Step 4: Commit**

```bash
git add architecture/remote.md architecture/references.md README.md
git commit -m "docs: allowed keys live in a pebble database"
```

---

### Task 7: Full verification

- [ ] **Step 1: Format, vet, build, test everything**

Run: `gofmt -l . && go vet ./... && go build ./... && go test ./...`
Expected: `gofmt -l` prints nothing; vet, build, and the full test suite pass.

- [ ] **Step 2: Commit any stragglers**

Only if step 1 required fixes:

```bash
git add -A
git commit -m "chore: fix formatting/vet findings"
```
