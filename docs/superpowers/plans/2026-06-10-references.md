# References Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Named, overwritable pointers (references) from a global name to a store key, kept by the daemon in a Pebble DB, created by `ingest`/`ref create`, and usable as `ref:NAME[@PATH]` wherever commands take `KEY[/PATH]`.

**Architecture:** A new `reference` package defines the deterministic-CBOR record and name rules; a new `refstore` package wraps a second Pebble DB at `<store>/refs/`; the daemon gains four `/v1/refs` routes (name as `?name=` query parameter); the client gains ref CRUD methods; the CLI gains `config-user`, `ref` subcommands, a mandatory NAME on daemon-mode ingest, and `ref:NAME@PATH` resolution (client-side) in all read commands.

**Tech Stack:** Go 1.26, `github.com/fxamacker/cbor/v2` (deterministic mode, already used by `fstree`), `github.com/cockroachdb/pebble/v2` (already used by `diskstore`), `github.com/urfave/cli/v2`, stdlib `net/http`.

**Spec:** `docs/superpowers/specs/2026-06-10-references-design.md` — read it before starting.

**Conventions you must follow:**
- Run tests with `go test ./<pkg>/` from the repo root; the final task runs `go build ./... && go test ./...`.
- Commit after every green test, message style matches `git log` (short, lower-case, imperative).
- Never leave generated binaries behind (`go build -o` artifacts must be deleted).

---

### Task 1: `reference` package — record, name validation, encoding

**Files:**
- Create: `reference/reference.go`
- Test: `reference/reference_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package reference_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
)

// testKey returns a valid canonical key to point references at.
func testKey(t *testing.T) []byte {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	return o.Key[:]
}

func TestValidateName(t *testing.T) {
	long := strings.Repeat("x", 1025)
	ok := strings.Repeat("y", 1024)
	cases := []struct {
		name    string
		refName string
		wantErr bool
	}{
		{"simple", "backup", false},
		{"with slash", "backups/2026/06", false},
		{"dotdot segment", "a/../b", false},
		{"empty segment", "a//b", false},
		{"unicode", "snapshot-éñ", false},
		{"max length", ok, false},
		{"empty", "", true},
		{"too long", long, true},
		{"at sign", "a@b", true},
		{"control char", "a\x01b", true},
		{"del char", "a\x7fb", true},
		{"newline", "a\nb", true},
		{"invalid utf8", string([]byte{0xff, 0xfe}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := reference.ValidateName(tc.refName)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateName(%q) error = %v, wantErr %v", tc.refName, err, tc.wantErr)
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := reference.Reference{
		Name:      "backups/home",
		Key:       testKey(t),
		User:      "dragan",
		CreatedAt: 1765432100123456789,
		Signature: []byte{1, 2, 3},
	}
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := reference.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != r.Name || !bytes.Equal(got.Key, r.Key) || got.User != r.User ||
		got.CreatedAt != r.CreatedAt || !bytes.Equal(got.Signature, r.Signature) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, r)
	}
}

func TestEncodeDeterministic(t *testing.T) {
	r := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42}
	a, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("two encodes of the same record differ")
	}
}

func TestSignaturePayloadExcludesSignature(t *testing.T) {
	unsigned := reference.Reference{Name: "n", Key: testKey(t), User: "u", CreatedAt: 42}
	signed := unsigned
	signed.Signature = []byte{9, 9, 9}

	unsignedEnc, err := unsigned.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := signed.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, unsignedEnc) {
		t.Fatal("SignaturePayload differs from the encoding of the unsigned record")
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	if _, err := (reference.Reference{Name: "a@b", Key: testKey(t)}).Encode(); err == nil {
		t.Fatal("expected error for invalid name")
	}
	if _, err := (reference.Reference{Name: "ok", Key: []byte{1, 2}}).Encode(); err == nil {
		t.Fatal("expected error for non-canonical key")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := reference.Decode([]byte("not cbor at all")); err == nil {
		t.Fatal("expected error for garbage input")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./reference/`
Expected: FAIL — package does not exist / undefined symbols.

- [ ] **Step 3: Write the implementation**

```go
// Package reference defines the named-pointer record: a global name pointing
// at a store key, with creator, creation time, and an optional opaque
// signature. Encoding is RFC 8949 §4.2 core-deterministic CBOR, matching the
// fstree object convention (canonical map, integer keys).
package reference

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/draganm/amber-store/key"
	"github.com/fxamacker/cbor/v2"
)

// MaxNameLen is the maximum reference name length in bytes.
const MaxNameLen = 1024

// encMode is the shared deterministic encoder, mirroring fstree.encMode.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("reference: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Reference is the record stored under a name. Fields are encoded as a
// canonical CBOR map with integer keys 0-4; Signature (key 4) is omitted when
// absent so the unsigned encoding doubles as the signature payload.
type Reference struct {
	Name      string `cbor:"0,keyasint"`
	Key       []byte `cbor:"1,keyasint"` // 32-byte canonical store key
	User      string `cbor:"2,keyasint"`
	CreatedAt int64  `cbor:"3,keyasint"` // ns since the Unix epoch
	Signature []byte `cbor:"4,keyasint,omitempty"`
}

// ValidateName checks the reference-name rules: 1..MaxNameLen bytes of valid
// UTF-8, no '@' (the ref/path separator) and no control characters. '/' is
// allowed; names are opaque strings with no structural meaning.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("reference name must not be empty")
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("reference name exceeds %d bytes", MaxNameLen)
	}
	if !utf8.ValidString(name) {
		return errors.New("reference name must be valid UTF-8")
	}
	for _, r := range name {
		if r == '@' {
			return errors.New("reference name must not contain '@'")
		}
		if r < 0x20 || r == 0x7f {
			return errors.New("reference name must not contain control characters")
		}
	}
	return nil
}

// validate checks the whole record: name rules plus a canonical key.
func (r Reference) validate() error {
	if err := ValidateName(r.Name); err != nil {
		return err
	}
	if _, err := key.Parse(r.Key); err != nil {
		return fmt.Errorf("reference key: %w", err)
	}
	return nil
}

// Encode returns the deterministic CBOR encoding of a validated record.
func (r Reference) Encode() ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return encMode.Marshal(r)
}

// Decode parses and validates a record.
func Decode(b []byte) (Reference, error) {
	var r Reference
	if err := cbor.Unmarshal(b, &r); err != nil {
		return Reference{}, fmt.Errorf("decoding reference: %w", err)
	}
	if err := r.validate(); err != nil {
		return Reference{}, err
	}
	return r, nil
}

// SignaturePayload returns the bytes a signature runs over: the deterministic
// encoding of the record without its Signature field.
func (r Reference) SignaturePayload() ([]byte, error) {
	r.Signature = nil
	return r.Encode()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./reference/`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add reference/
git commit -m "add reference package: record, name rules, deterministic CBOR"
```

---

### Task 2: `refstore` package — Pebble-backed name→record store

**Files:**
- Create: `refstore/refstore.go`
- Test: `refstore/refstore_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package refstore_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/amber-store/refstore"
)

func open(t *testing.T, dir string) *refstore.Store {
	t.Helper()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := open(t, t.TempDir())

	if _, err := s.Get("missing"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := s.Put("a/b", []byte("rec1")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec1")) {
		t.Fatalf("Get = %q, want rec1", got)
	}
	// Overwrite is unconditional.
	if err := s.Put("a/b", []byte("rec2")); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec2")) {
		t.Fatalf("Get after overwrite = %q, want rec2", got)
	}
	if err := s.Delete("a/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Delete(absent) = %v, want ErrNotFound", err)
	}
}

func TestAllSortedByName(t *testing.T) {
	s := open(t, t.TempDir())
	for _, n := range []string{"zeta", "alpha", "mid/dle"} {
		if err := s.Put(n, []byte("v-"+n)); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"alpha", "mid/dle", "zeta"}
	if len(recs) != len(wantNames) {
		t.Fatalf("All returned %d records, want %d", len(recs), len(wantNames))
	}
	for i, want := range wantNames {
		if recs[i].Name != want {
			t.Fatalf("recs[%d].Name = %q, want %q", i, recs[i].Name, want)
		}
		if !bytes.Equal(recs[i].Data, []byte("v-"+want)) {
			t.Fatalf("recs[%d].Data = %q", i, recs[i].Data)
		}
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, dir)
	got, err := s2.Get("keep")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen = %q, want v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./refstore/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the implementation**

```go
// Package refstore persists reference records in a Pebble DB: name bytes →
// CBOR record bytes, stored verbatim. It is a dumb KV layer; record
// validation belongs to the daemon and the reference package.
package refstore

import (
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"
)

// ErrNotFound is returned by Get and Delete for an absent name.
var ErrNotFound = errors.New("refstore: reference not found")

// Store is a Pebble-backed name→record map. It is safe for concurrent use.
type Store struct {
	db        *pebble.DB
	writeOpts *pebble.WriteOptions
}

// discardLogger silences pebble's internal logging (same trick as diskstore).
type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("refstore: pebble fatal: "+format, args...))
}

// Open opens (creating if missing) the refs DB at dir. sync selects the
// write durability, matching the daemon's --sync flag.
func Open(dir string, sync bool) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		return nil, fmt.Errorf("refstore: opening pebble: %w", err)
	}
	wo := pebble.Sync
	if !sync {
		wo = pebble.NoSync
	}
	return &Store{db: db, writeOpts: wo}, nil
}

// Put stores record under name, overwriting unconditionally.
func (s *Store) Put(name string, record []byte) error {
	return s.db.Set([]byte(name), record, s.writeOpts)
}

// Get returns the record stored under name, or ErrNotFound.
func (s *Store) Get(name string) ([]byte, error) {
	v, closer, err := s.db.Get([]byte(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := slices.Clone(v)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes name, or returns ErrNotFound if absent. The daemon is the
// only writer, so the read-then-delete pair is not racy in practice.
func (s *Store) Delete(name string) error {
	if _, err := s.Get(name); err != nil {
		return err
	}
	return s.db.Delete([]byte(name), s.writeOpts)
}

// Record is one (name, record-bytes) pair from All.
type Record struct {
	Name string
	Data []byte
}

// All returns every record in lexicographic name order.
func (s *Store) All() ([]Record, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var recs []Record
	for it.First(); it.Valid(); it.Next() {
		recs = append(recs, Record{
			Name: string(it.Key()),
			Data: slices.Clone(it.Value()),
		})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return recs, nil
}

// Close closes the DB.
func (s *Store) Close() error {
	return s.db.Close()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./refstore/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add refstore/
git commit -m "add refstore: pebble-backed name to record store"
```

---

### Task 3: daemon `/v1/refs` routes

**Files:**
- Create: `daemon/refs.go`
- Modify: `daemon/daemon.go` (handler struct, `New` signature, route registration)
- Modify: `daemon/daemon_test.go` (`serveOnSocket` helper gains a refstore)
- Modify: `cmd/amber-store/daemon.go` (`runDaemon` opens the refstore)
- Modify: `cmd/amber-store/daemon_test.go` (`startDaemon` helper)
- Test: `daemon/refs_test.go`

The `New` signature changes to `New(store *diskstore.Store, refs *refstore.Store, logger *slog.Logger)`; every caller must be updated in this task or the build breaks.

- [ ] **Step 1: Write the failing tests**

Create `daemon/refs_test.go`. It uses `httptest.NewServer` with plain HTTP (the client's ref methods arrive in Task 4):

```go
package daemon_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

func openRefs(t *testing.T) *refstore.Store {
	t.Helper()
	rs, err := refstore.Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs
}

// refsServer serves the daemon handler over plain HTTP with one stored blob,
// returning the server, its base URL, and the blob's key bytes.
func refsServer(t *testing.T) (*httptest.Server, []byte) {
	t.Helper()
	store := openStore(t)
	o, err := fstree.EncodeBlob([]byte("blob content"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	t.Cleanup(srv.Close)
	return srv, o.Key[:]
}

func refURL(base, name string) string {
	return base + "/v1/refs?name=" + url.QueryEscape(name)
}

func doReq(t *testing.T, method, u string, body []byte) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func encodeRef(t *testing.T, r reference.Reference) []byte {
	t.Helper()
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRefs_PutGetDeleteRoundTrip(t *testing.T) {
	srv, kb := refsServer(t)
	name := "backups/2026/../06" // exercises '/', '..' through the query param
	rec := reference.Reference{Name: name, Key: kb, User: "u", CreatedAt: 42}

	if resp := doReq(t, http.MethodPut, refURL(srv.URL, name), encodeRef(t, rec)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", resp.StatusCode)
	}

	resp := doReq(t, http.MethodGet, refURL(srv.URL, name), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/cbor" {
		t.Fatalf("GET content type = %q, want application/cbor", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reference.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != name || got.User != "u" || got.CreatedAt != 42 {
		t.Fatalf("GET returned %+v", got)
	}

	if resp := doReq(t, http.MethodDelete, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodGet, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodDelete, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE absent status = %d, want 404", resp.StatusCode)
	}
}

func TestRefs_PutOverwrites(t *testing.T) {
	srv, kb := refsServer(t)
	first := reference.Reference{Name: "n", Key: kb, User: "alice", CreatedAt: 1}
	second := reference.Reference{Name: "n", Key: kb, User: "bob", CreatedAt: 2}
	doReq(t, http.MethodPut, refURL(srv.URL, "n"), encodeRef(t, first))
	doReq(t, http.MethodPut, refURL(srv.URL, "n"), encodeRef(t, second))

	resp := doReq(t, http.MethodGet, refURL(srv.URL, "n"), nil)
	body, _ := io.ReadAll(resp.Body)
	got, err := reference.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.User != "bob" || got.CreatedAt != 2 {
		t.Fatalf("overwrite did not take: %+v", got)
	}
}

func TestRefs_PutErrors(t *testing.T) {
	srv, kb := refsServer(t)
	missingKey := make([]byte, len(kb))
	copy(missingKey, kb)
	missingKey[len(missingKey)-1] ^= 0xff // valid encoding, absent from store

	cases := []struct {
		name   string
		url    string
		body   []byte
		status int
	}{
		{"missing name param", srv.URL + "/v1/refs", encodeRef(t, reference.Reference{Name: "n", Key: kb}), 422},
		{"bad cbor", refURL(srv.URL, "n"), []byte("garbage"), 422},
		{"name mismatch", refURL(srv.URL, "other"), encodeRef(t, reference.Reference{Name: "n", Key: kb}), 422},
		{"dangling key", refURL(srv.URL, "n"), encodeRef(t, reference.Reference{Name: "n", Key: missingKey}), 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doReq(t, http.MethodPut, tc.url, tc.body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}

	// Invalid name: cannot be encoded client-side, so craft via a valid record
	// under a different query name is covered above; an invalid *query* name
	// with undecodable body still 422s:
	resp := doReq(t, http.MethodDelete, srv.URL+"/v1/refs", nil)
	if resp.StatusCode != 422 {
		t.Fatalf("DELETE without name status = %d, want 422", resp.StatusCode)
	}
}

func TestRefs_ListNDJSON(t *testing.T) {
	srv, kb := refsServer(t)
	for _, n := range []string{"zeta", "alpha"} {
		rec := reference.Reference{Name: n, Key: kb, User: "u", CreatedAt: 1700000000000000000, Signature: []byte{1}}
		doReq(t, http.MethodPut, refURL(srv.URL, n), encodeRef(t, rec))
	}
	resp := doReq(t, http.MethodGet, srv.URL+"/v1/refs", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), body)
	}
	var first struct {
		Name      string `json:"name"`
		Key       string `json:"key"`
		User      string `json:"user"`
		CreatedAt string `json:"created_at"`
		Signed    bool   `json:"signed"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Name != "alpha" { // lexicographic order
		t.Fatalf("first line name = %q, want alpha", first.Name)
	}
	if first.Key == "" || first.User != "u" || !first.Signed || first.CreatedAt == "" {
		t.Fatalf("line fields wrong: %+v", first)
	}
}
```

- [ ] **Step 2: Update `New` and every caller so the package compiles (tests still red)**

In `daemon/daemon.go`:

```go
// handler gains the refs field:
type handler struct {
	store *diskstore.Store
	refs  *refstore.Store
	log   *slog.Logger
}

// New signature and body change to:
func New(store *diskstore.Store, refs *refstore.Store, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{store: store, refs: refs, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	mux.HandleFunc("GET /v1/ls/{key}", h.getLs)
	mux.HandleFunc("GET /v1/content-keys/{key}", h.getContentKeys)
	mux.HandleFunc("PUT /v1/refs", h.putRef)
	mux.HandleFunc("GET /v1/refs", h.getRefs)
	mux.HandleFunc("DELETE /v1/refs", h.deleteRef)
	return logRequests(logger, mux)
}
```

Add `"github.com/draganm/amber-store/refstore"` to the imports.

In `daemon/daemon_test.go`, `serveOnSocket` changes its `srv` line to:

```go
	srv := &http.Server{Handler: daemon.New(store, openRefs(t), nil)}
```

(`openRefs` comes from `refs_test.go`, same package.)

In `cmd/amber-store/daemon.go`, `runDaemon` opens the refstore right after the diskstore (add `"path/filepath"` and `"github.com/draganm/amber-store/refstore"` imports):

```go
	refs, err := refstore.Open(filepath.Join(cfg.store, "refs"), cfg.sync)
	if err != nil {
		return err
	}
	defer refs.Close()
```

and passes it to the handler construction further down (find `daemon.New(store,` in the file):

```go
	handler := daemon.New(store, refs, logger)
```

In `cmd/amber-store/daemon_test.go`, `startDaemon` opens a refstore the same way and passes it:

```go
	refs, err := refstore.Open(filepath.Join(t.TempDir(), "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	...
	srv := &http.Server{Handler: daemon.New(store, refs, nil)}
```

- [ ] **Step 3: Run tests — compile OK, ref tests fail**

Run: `go build ./... && go test ./daemon/`
Expected: builds; existing tests PASS; `TestRefs_*` FAIL (handlers undefined → actually compile error until Step 4; if so, stub the three handlers with `http.Error(w, "unimplemented", 500)` to see red, or proceed directly).

- [ ] **Step 4: Implement the handlers**

Create `daemon/refs.go`:

```go
package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// maxRefRecord bounds a PUT /v1/refs body: a record is a 1 KiB name plus a
// few small fields and a signature; 1 MiB is generous.
const maxRefRecord = 1 << 20

// refName extracts and validates the ?name= query parameter; on failure it
// writes a 422 and returns false.
func refName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return "", false
	}
	if err := reference.ValidateName(name); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return "", false
	}
	return name, true
}

// putRef stores the CBOR reference record from the body under ?name=,
// overwriting unconditionally. The record must decode, match the query name,
// carry a canonical key, and that key must exist in the store.
func (h *handler) putRef(w http.ResponseWriter, r *http.Request) {
	name, ok := refName(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRefRecord+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(body) > maxRefRecord {
		http.Error(w, "reference record too large", http.StatusUnprocessableEntity)
		return
	}
	rec, err := reference.Decode(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if rec.Name != name {
		http.Error(w, "record name does not match query name", http.StatusUnprocessableEntity)
		return
	}
	k, err := key.Parse(rec.Key) // canonical: Decode validated it
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		h.log.Error("ref key lookup failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "referenced key not found in store", http.StatusNotFound)
		return
	}
	// Store the body verbatim: it is the canonical encoding (Decode verified
	// it round-trips) and preserves the signature bytes untouched.
	if err := h.refs.Put(name, body); err != nil {
		h.log.Error("ref put failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("reference stored", "name", name, "key", k)
	w.WriteHeader(http.StatusNoContent)
}

// refLine is one NDJSON line of the GET /v1/refs listing.
type refLine struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"` // RFC 3339
	Signed    bool   `json:"signed"`
}

// getRefs serves a single record (?name=, application/cbor) or, without a
// name parameter, the NDJSON listing of all references in name order.
func (h *handler) getRefs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("name") == "" {
		h.listRefs(w)
		return
	}
	name, ok := refName(w, r)
	if !ok {
		return
	}
	data, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.log.Error("ref get failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/cbor")
	_, _ = w.Write(data)
}

// listRefs writes the NDJSON listing. Records are decoded fully before the
// first byte so a malformed record surfaces as a 500, not a truncated 200.
func (h *handler) listRefs(w http.ResponseWriter) {
	recs, err := h.refs.All()
	if err != nil {
		h.log.Error("ref list failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lines := make([]refLine, len(recs))
	for i, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			h.log.Error("stored reference is malformed", "name", rec.Name, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			h.log.Error("stored reference has invalid key", "name", rec.Name, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		lines[i] = refLine{
			Name:      ref.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Signed:    len(ref.Signature) > 0,
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, l := range lines {
		if err := enc.Encode(l); err != nil {
			h.log.Error("ref list stream aborted", "error", err)
			return
		}
	}
}

// deleteRef removes the reference named by ?name=.
func (h *handler) deleteRef(w http.ResponseWriter, r *http.Request) {
	name, ok := refName(w, r)
	if !ok {
		return
	}
	err := h.refs.Delete(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.log.Error("ref delete failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("reference deleted", "name", name)
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go build ./... && go test ./daemon/ ./cmd/amber-store/`
Expected: PASS (all, including the pre-existing suites).

- [ ] **Step 6: Commit**

```bash
git add daemon/ cmd/amber-store/
git commit -m "daemon: add /v1/refs routes backed by refstore"
```

---

### Task 4: client reference methods

**Files:**
- Create: `client/refs.go`
- Test: `daemon/refs_client_test.go` (uses the existing `serveOnSocket` helper)

- [ ] **Step 1: Write the failing test**

Create `daemon/refs_client_test.go`:

```go
package daemon_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
)

func TestRefs_ClientRoundTrip(t *testing.T) {
	store := openStore(t)
	o, err := fstree.EncodeBlob([]byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	cl := serveOnSocket(t, store)
	ctx := context.Background()

	rec := reference.Reference{Name: "a/b c", Key: o.Key[:], User: "u", CreatedAt: 7}
	if err := cl.PutRef(ctx, rec); err != nil {
		t.Fatalf("PutRef: %v", err)
	}

	got, err := cl.GetRef(ctx, "a/b c")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	if got.Name != rec.Name || !bytes.Equal(got.Key, rec.Key) || got.User != "u" || got.CreatedAt != 7 {
		t.Fatalf("GetRef = %+v", got)
	}

	infos, err := cl.ListRefs(ctx)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "a/b c" || infos[0].Key != o.Key.String() {
		t.Fatalf("ListRefs = %+v", infos)
	}

	if err := cl.DeleteRef(ctx, "a/b c"); err != nil {
		t.Fatalf("DeleteRef: %v", err)
	}
	if _, err := cl.GetRef(ctx, "a/b c"); !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("GetRef after delete = %v, want ErrRefNotFound", err)
	}
	if err := cl.DeleteRef(ctx, "a/b c"); !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("DeleteRef absent = %v, want ErrRefNotFound", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./daemon/ -run TestRefs_ClientRoundTrip`
Expected: FAIL — `cl.PutRef` etc. undefined.

- [ ] **Step 3: Implement `client/refs.go`**

```go
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/draganm/amber-store/reference"
)

// ErrRefNotFound reports an absent reference name.
var ErrRefNotFound = errors.New("reference not found")

// refsURL builds the /v1/refs URL, with the name as a query parameter (names
// may contain '/', '..' and other path-hostile characters).
func refsURL(name string) string {
	u := baseURL + "/v1/refs"
	if name != "" {
		u += "?name=" + url.QueryEscape(name)
	}
	return u
}

// PutRef creates or overwrites the reference rec under rec.Name.
func (c *Client) PutRef(ctx context.Context, rec reference.Reference) error {
	body, err := rec.Encode()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, refsURL(rec.Name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/cbor")
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("put-ref failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// GetRef fetches and decodes the reference stored under name.
func (c *Client) GetRef(ctx context.Context, name string) (reference.Reference, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refsURL(name), nil)
	if err != nil {
		return reference.Reference{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return reference.Reference{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return reference.Reference{}, fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return reference.Reference{}, fmt.Errorf("get-ref failed: %s: %s", resp.Status, msg)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("reading get-ref response: %w", err)
	}
	return reference.Decode(body)
}

// RefInfo mirrors one NDJSON line of the daemon's GET /v1/refs listing.
type RefInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"` // RFC 3339
	Signed    bool   `json:"signed"`
}

// ListRefs returns every reference, in name order.
func (c *Client) ListRefs(ctx context.Context) ([]RefInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refsURL(""), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("list-refs failed: %s: %s", resp.Status, msg)
	}
	var infos []RefInfo
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 64<<10)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var info RefInfo
		if err := json.Unmarshal(line, &info); err != nil {
			return nil, fmt.Errorf("decoding list-refs response: %w", err)
		}
		infos = append(infos, info)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading list-refs response: %w", err)
	}
	return infos, nil
}

// DeleteRef removes the reference stored under name.
func (c *Client) DeleteRef(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, refsURL(name), nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("delete-ref failed: %s: %s", resp.Status, msg)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./daemon/ ./client/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/refs.go daemon/refs_client_test.go
git commit -m "client: add reference CRUD methods"
```

---

### Task 5: user config package and `config-user` command

**Files:**
- Create: `internal/userconfig/userconfig.go`
- Create: `cmd/amber-store/config_user.go`
- Modify: `cmd/amber-store/main.go` (register the command)
- Test: `internal/userconfig/userconfig_test.go`, `cmd/amber-store/config_user_test.go`

- [ ] **Step 1: Write the failing package tests**

`internal/userconfig/userconfig_test.go`:

```go
package userconfig_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/userconfig"
)

func TestLoadMissingIsErrNotConfigured(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if _, err := userconfig.Load(); !errors.Is(err, userconfig.ErrNotConfigured) {
		t.Fatalf("Load = %v, want ErrNotConfigured", err)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "deep", "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := userconfig.Save(userconfig.Config{User: "dragan"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "dragan" {
		t.Fatalf("User = %q, want dragan", cfg.User)
	}
}

func TestLoadEmptyUserIsErrNotConfigured(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := os.WriteFile(p, []byte(`{"user":""}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := userconfig.Load(); !errors.Is(err, userconfig.ErrNotConfigured) {
		t.Fatalf("Load = %v, want ErrNotConfigured", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/userconfig/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement `internal/userconfig/userconfig.go`**

```go
// Package userconfig reads and writes the per-user amber-store configuration:
// a JSON file holding the user identity recorded in references it creates.
// The JSON format leaves room for a future signing key.
package userconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotConfigured means no usable config exists; commands that create
// references refuse to run until `amber-store config-user NAME` is run.
var ErrNotConfigured = errors.New("no user configured — run 'amber-store config-user <name>' first")

// Config is the persisted user configuration.
type Config struct {
	User string `json:"user"`
}

// Path returns the config file location: $AMBER_STORE_CONFIG when set,
// otherwise <os.UserConfigDir>/amber-store/config.json.
func Path() (string, error) {
	if p := os.Getenv("AMBER_STORE_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolving user config dir: %w", err)
	}
	return filepath.Join(dir, "amber-store", "config.json"), nil
}

// Load reads the config; a missing file or empty user is ErrNotConfigured.
func Load() (Config, error) {
	p, err := Path()
	if err != nil {
		return Config{}, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, ErrNotConfigured
	}
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing %s: %w", p, err)
	}
	if cfg.User == "" {
		return Config{}, ErrNotConfigured
	}
	return cfg, nil
}

// Save writes the config, creating parent directories as needed.
func Save(cfg Config) error {
	p, err := Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, append(b, '\n'), 0o644)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/userconfig/`
Expected: PASS.

- [ ] **Step 5: Write the failing command test**

`cmd/amber-store/config_user_test.go`:

```go
package main

import (
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/userconfig"
)

func TestConfigUserCommand(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user", "alice"}); err != nil {
		t.Fatalf("config-user: %v", err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "alice" {
		t.Fatalf("User = %q, want alice", cfg.User)
	}
}

func TestConfigUserRequiresOneArg(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := newApp().Run([]string{"amber-store", "config-user"}); err == nil {
		t.Fatal("expected error without NAME")
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestConfigUser`
Expected: FAIL — command not registered.

- [ ] **Step 7: Implement the command**

`cmd/amber-store/config_user.go`:

```go
package main

import (
	"fmt"

	"github.com/draganm/amber-store/internal/userconfig"
	"github.com/urfave/cli/v2"
)

func configUserCommand() *cli.Command {
	return &cli.Command{
		Name:      "config-user",
		Usage:     "record the user name written into references created by this machine",
		ArgsUsage: "NAME",
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("config-user requires exactly one NAME argument, got %d", c.NArg())
			}
			if err := userconfig.Save(userconfig.Config{User: c.Args().First()}); err != nil {
				return err
			}
			p, err := userconfig.Path()
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "user config written to %s\n", p)
			return nil
		},
	}
}
```

Register it in `cmd/amber-store/main.go` by adding `configUserCommand(),` to the `Commands` slice (after `contentKeysCommand()`).

- [ ] **Step 8: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run TestConfigUser`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/userconfig/ cmd/amber-store/config_user.go cmd/amber-store/config_user_test.go cmd/amber-store/main.go
git commit -m "add user config and config-user command"
```

---

### Task 6: `ref` subcommands (create / ls / show / rm)

**Files:**
- Create: `cmd/amber-store/ref.go`
- Modify: `cmd/amber-store/main.go` (register `refCommand()`)
- Test: `cmd/amber-store/ref_test.go`

- [ ] **Step 1: Write the failing test**

`cmd/amber-store/ref_test.go`. `startDaemon` (from `daemon_test.go`) returns a socket; `configureTestUser` is a new helper this test introduces — place it in this file, it is reused by later tasks:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/client"
)

// configureTestUser points AMBER_STORE_CONFIG at a fresh file and writes a
// config for the given user, so commands that create references can run.
func configureTestUser(t *testing.T, user string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := os.WriteFile(p, []byte(`{"user":"`+user+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ingestTestTree ingests a small tree through the daemon and returns the
// printed root key (hex). It requires configureTestUser to have run.
func ingestTestTree(t *testing.T, sock, name string) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, name, src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return strings.TrimSpace(out.String())
}

func TestRefCreateShowLsRm(t *testing.T) {
	configureTestUser(t, "tester")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "first")

	// create (re-point a second name at the same key)
	if err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "second/name", root}); err != nil {
		t.Fatalf("ref create: %v", err)
	}

	// show
	var showOut bytes.Buffer
	app := newApp()
	app.Writer = &showOut
	if err := app.Run([]string{"amber-store", "ref", "show", "--socket", sock, "second/name"}); err != nil {
		t.Fatalf("ref show: %v", err)
	}
	var shown struct {
		Name      string `json:"name"`
		Key       string `json:"key"`
		User      string `json:"user"`
		CreatedAt string `json:"created_at"`
		Signature string `json:"signature,omitempty"`
	}
	if err := json.Unmarshal(showOut.Bytes(), &shown); err != nil {
		t.Fatalf("ref show output not JSON: %v\n%s", err, showOut.String())
	}
	if shown.Name != "second/name" || shown.Key != root || shown.User != "tester" {
		t.Fatalf("ref show = %+v", shown)
	}

	// ls lists both, name order
	var lsOut bytes.Buffer
	app = newApp()
	app.Writer = &lsOut
	if err := app.Run([]string{"amber-store", "ref", "ls", "--socket", sock}); err != nil {
		t.Fatalf("ref ls: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(lsOut.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("ref ls printed %d lines, want 2:\n%s", len(lines), lsOut.String())
	}
	if !strings.HasPrefix(lines[0], "first") || !strings.HasPrefix(lines[1], "second/name") {
		t.Fatalf("ref ls order/content wrong:\n%s", lsOut.String())
	}

	// rm
	if err := newApp().Run([]string{"amber-store", "ref", "rm", "--socket", sock, "second/name"}); err != nil {
		t.Fatalf("ref rm: %v", err)
	}
	_, err := client.New(sock).GetRef(context.Background(), "second/name")
	if !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("after rm, GetRef = %v, want ErrRefNotFound", err)
	}
}

func TestRefCreateNeedsUserConfig(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	sock := startDaemon(t)
	err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "n", strings.Repeat("0", 64)})
	if err == nil || !strings.Contains(err.Error(), "config-user") {
		t.Fatalf("ref create without config = %v, want config-user hint", err)
	}
}
```

NOTE: `ingestTestTree` depends on Task 7's `ingest NAME DIR` form. Until Task 7 lands, make `TestRefCreateShowLsRm` create its first ref with `ref create` instead — OR implement Tasks 6 and 7 against this final test and only expect green after both. **Simplest path: in this task, write the test exactly as above but with `t.Skip("enabled by ingest NAME DIR task")` as the first line of `TestRefCreateShowLsRm`; remove the skip in Task 7.** `TestRefCreateNeedsUserConfig` runs now.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestRefCreate`
Expected: FAIL — `ref` command not registered.

- [ ] **Step 3: Implement `cmd/amber-store/ref.go`**

```go
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/internal/userconfig"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/urfave/cli/v2"
)

// socketFlag is the shared --socket flag; dst receives the value.
func socketFlag(dst *string) cli.Flag {
	return &cli.StringFlag{
		Name:        "socket",
		Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
		Destination: dst,
	}
}

func refCommand() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "manage references (named pointers to keys)",
		Subcommands: []*cli.Command{
			refCreateCommand(),
			refLsCommand(),
			refShowCommand(),
			refRmCommand(),
		},
	}
}

func refCreateCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "create",
		Usage:     "point NAME at the existing key KEY (creates or overwrites)",
		ArgsUsage: "NAME KEY",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return fmt.Errorf("ref create requires NAME KEY arguments, got %d", c.NArg())
			}
			name := c.Args().Get(0)
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			// Load the config before parsing the key so a missing config is
			// reported with its config-user hint regardless of the KEY value.
			ucfg, err := userconfig.Load()
			if err != nil {
				return err
			}
			k, err := parseHexKey(c.Args().Get(1))
			if err != nil {
				return err
			}
			rec := reference.Reference{
				Name:      name,
				Key:       k[:],
				User:      ucfg.User,
				CreatedAt: time.Now().UnixNano(),
			}
			return client.New(socketpath.Resolve(socket)).PutRef(c.Context, rec)
		},
	}
}

func refLsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:  "ls",
		Usage: "list all references: name, key, user, creation date",
		Flags: []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("ref ls takes no arguments, got %d", c.NArg())
			}
			infos, err := client.New(socketpath.Resolve(socket)).ListRefs(c.Context)
			if err != nil {
				return err
			}
			for _, info := range infos {
				if _, err := fmt.Fprintf(c.App.Writer, "%s %s %s %s\n",
					info.Name, info.Key, info.User, info.CreatedAt); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// refShowOutput is the JSON document `ref show` prints.
type refShowOutput struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signature string `json:"signature,omitempty"` // hex
}

func refShowCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "show",
		Usage:     "print one reference's full record as JSON",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("ref show requires exactly one NAME argument, got %d", c.NArg())
			}
			rec, err := client.New(socketpath.Resolve(socket)).GetRef(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			k, err := key.Parse(rec.Key)
			if err != nil {
				return err
			}
			out := refShowOutput{
				Name:      rec.Name,
				Key:       k.String(),
				User:      rec.User,
				CreatedAt: time.Unix(0, rec.CreatedAt).UTC().Format(time.RFC3339Nano),
				Signature: hex.EncodeToString(rec.Signature),
			}
			enc := json.NewEncoder(c.App.Writer)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		},
	}
}

func refRmCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "rm",
		Usage:     "delete a reference (the pointed-to objects stay in the store)",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("ref rm requires exactly one NAME argument, got %d", c.NArg())
			}
			return client.New(socketpath.Resolve(socket)).DeleteRef(c.Context, c.Args().First())
		},
	}
}
```

Register `refCommand(),` in `main.go`'s `Commands` slice.

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/amber-store/ -run 'TestRefCreate|TestConfigUser'`
Expected: `TestRefCreateNeedsUserConfig` PASS, `TestRefCreateShowLsRm` SKIP, others PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/ref.go cmd/amber-store/ref_test.go cmd/amber-store/main.go
git commit -m "add ref create/ls/show/rm commands"
```

---

### Task 7: ingest NAME DIR — mandatory reference on daemon-mode ingest

**Files:**
- Modify: `cmd/amber-store/ingest.go` (args, config check, PutRef after upload)
- Modify: `cmd/amber-store/main.go` (`dirArg` refactor)
- Modify: existing tests that call `ingest` without a name: run `grep -rn '"ingest"' cmd/amber-store/*_test.go` and update every daemon-mode invocation (offline `--output` calls keep their single DIR arg). At the time of writing: `e2e_test.go`, `daemon_test.go`, `loaddump_test.go`, `ingest_test.go`.
- Modify: `cmd/amber-store/ref_test.go` (remove the `t.Skip` from `TestRefCreateShowLsRm`)
- Test: `cmd/amber-store/ingest_ref_test.go`

- [ ] **Step 1: Write the failing test**

`cmd/amber-store/ingest_ref_test.go`:

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
)

func TestIngestCreatesReference(t *testing.T) {
	configureTestUser(t, "ingester")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "my/backup")

	rec, err := client.New(sock).GetRef(context.Background(), "my/backup")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if k.String() != root {
		t.Fatalf("reference key = %s, want printed root %s", k.String(), root)
	}
	if rec.User != "ingester" {
		t.Fatalf("reference user = %q, want ingester", rec.User)
	}
	if rec.CreatedAt == 0 {
		t.Fatal("reference CreatedAt is zero")
	}
}

func TestIngestRefusesWithoutUserConfig(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	sock := startDaemon(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "name", src})
	if err == nil || !strings.Contains(err.Error(), "config-user") {
		t.Fatalf("ingest without config = %v, want config-user hint", err)
	}
}

func TestIngestRejectsBadName(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)
	src := t.TempDir()
	err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "bad@name", src})
	if err == nil || !strings.Contains(err.Error(), "@") {
		t.Fatalf("ingest with bad name = %v, want name error", err)
	}
}

func TestIngestOfflineTakesNoName(t *testing.T) {
	// Offline mode must NOT require a name or a user config.
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := filepath.Join(t.TempDir(), "t.amberpack")
	if err := newApp().Run([]string{"amber-store", "ingest", "--no-progress", "-o", pack, src}); err != nil {
		t.Fatalf("offline ingest: %v", err)
	}
	if _, err := os.Stat(pack); err != nil {
		t.Fatal(err)
	}
}
```

Also remove the `t.Skip(...)` line from `TestRefCreateShowLsRm` in `ref_test.go`.

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `go test ./cmd/amber-store/ -run 'TestIngest(Creates|Refuses|Rejects|Offline)|TestRefCreateShowLsRm'`
Expected: FAIL — ingest still takes a single DIR argument.

- [ ] **Step 3: Implement the ingest changes**

In `cmd/amber-store/main.go`, split `dirArg` so the directory check is reusable:

```go
// checkDir verifies dir names an existing directory.
func checkDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return nil
}

// dirArg validates that the command received exactly one argument naming an
// existing directory, and returns it. cmd names the command for error messages.
func dirArg(c *cli.Context, cmd string) (string, error) {
	if c.NArg() != 1 {
		return "", fmt.Errorf("%s requires exactly one DIR argument, got %d", cmd, c.NArg())
	}
	dir := c.Args().First()
	if err := checkDir(dir); err != nil {
		return "", err
	}
	return dir, nil
}
```

In `cmd/amber-store/ingest.go`:

1. Change the command metadata:

```go
		Name:      "ingest",
		Usage:     "build the content-addressed tree for DIR, store it via the daemon under reference NAME (or write a pack file with --output, no NAME)",
		ArgsUsage: "NAME DIR  (with --output: DIR)",
```

2. At the top of `runIngest`, replace the `dirArg` call with mode-dependent argument parsing (add imports `"time"` is already present; add `"github.com/draganm/amber-store/internal/userconfig"` and `"github.com/draganm/amber-store/reference"`):

```go
	var refName, dir, user string
	if cfg.output != "" {
		d, err := dirArg(c, "ingest")
		if err != nil {
			return err
		}
		dir = d
	} else {
		if c.NArg() != 2 {
			return fmt.Errorf("ingest requires NAME DIR arguments, got %d", c.NArg())
		}
		refName = c.Args().Get(0)
		dir = c.Args().Get(1)
		if err := reference.ValidateName(refName); err != nil {
			return err
		}
		if err := checkDir(dir); err != nil {
			return err
		}
		ucfg, err := userconfig.Load()
		if err != nil {
			return err
		}
		user = ucfg.User
	}
```

3. In the daemon branch, name the client once (replace the inline `client.New(...)` call):

```go
		cl := client.New(socketpath.Resolve(cfg.socket))
		_, ingErr := cl.Ingest(ctx, pr)
```

4. After the existing `root = res.root` line (still inside the daemon branch), create the reference. Use `c.Context`, not the progress `ctx` (which is cancelled right after):

```go
		rec := reference.Reference{
			Name:      refName,
			Key:       root[:],
			User:      user,
			CreatedAt: time.Now().UnixNano(),
		}
		if err := cl.PutRef(c.Context, rec); err != nil {
			return fmt.Errorf("tree stored (root %s) but creating reference %q failed: %w\nretry with: amber-store ref create %q %s",
				root, refName, err, refName, root)
		}
```

- [ ] **Step 4: Update the existing daemon-mode ingest test invocations**

Run `grep -rn '"ingest"' cmd/amber-store/*_test.go`. For every invocation **without** `--output`/`-o`, insert a name argument before the source dir and ensure `configureTestUser(t, ...)` (from `ref_test.go`) runs first in that test. Example — in `e2e_test.go`:

```go
	configureTestUser(t, "e2e")
	...
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "e2e/tree", src}); err != nil {
```

Apply the same pattern in `daemon_test.go` (cmd package), `loaddump_test.go`, and `ingest_test.go` wherever the daemon path is exercised. Offline (`-o`) invocations stay unchanged.

- [ ] **Step 5: Run the full command-package suite**

Run: `go test ./cmd/amber-store/`
Expected: PASS (including the previously skipped `TestRefCreateShowLsRm`).

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/
git commit -m "ingest: require reference name in daemon mode, create ref after upload"
```

---

### Task 8: `ref:NAME[@PATH]` addressing in read commands

**Files:**
- Create: `cmd/amber-store/spec.go`
- Modify: `cmd/amber-store/ls.go`, `cmd/amber-store/dump.go`, `cmd/amber-store/restore.go`, `cmd/amber-store/content_keys.go`
- Test: `cmd/amber-store/spec_test.go`

- [ ] **Step 1: Write the failing test**

`cmd/amber-store/spec_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestLsViaReference checks that `ls ref:NAME@PATH` equals `ls KEY/PATH`.
func TestLsViaReference(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)

	// Build a tree with a subdirectory to exercise @PATH.
	src := t.TempDir()
	if err := writeTestFile(src, "sub/inner.txt", "content"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	app := newApp()
	app.Writer = &out
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "tree", src}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out.String())

	run := func(spec string) string {
		var b bytes.Buffer
		a := newApp()
		a.Writer = &b
		if err := a.Run([]string{"amber-store", "ls", "--socket", sock, spec}); err != nil {
			t.Fatalf("ls %s: %v", spec, err)
		}
		return b.String()
	}

	if got, want := run("ref:tree"), run(root); got != want {
		t.Fatalf("ls ref:tree = %q, ls KEY = %q", got, want)
	}
	if got, want := run("ref:tree@sub"), run(root+"/sub"); got != want {
		t.Fatalf("ls ref:tree@sub = %q, ls KEY/sub = %q", got, want)
	}
}

func TestLsRefErrors(t *testing.T) {
	configureTestUser(t, "u")
	sock := startDaemon(t)

	err := newApp().Run([]string{"amber-store", "ls", "--socket", sock, "ref:absent"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ls ref:absent = %v, want not-found error", err)
	}
	err = newApp().Run([]string{"amber-store", "ls", "--socket", sock, "ref:@path"})
	if err == nil {
		t.Fatal("ls ref:@path should fail: empty name")
	}
}
```

`writeTestFile` may not exist; if no equivalent helper is present in the test files, add to `spec_test.go`:

```go
import (
	"os"
	"path/filepath"
)

// writeTestFile writes content to dir/rel, creating parent directories.
func writeTestFile(dir, rel, content string) error {
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(content), 0o644)
}
```

(Check `buildSourceTree` in `e2e_test.go` first — if a suitable helper exists, use it instead.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run 'TestLsViaReference|TestLsRefErrors'`
Expected: FAIL — `ref:tree` is parsed as a hex key and rejected.

- [ ] **Step 3: Implement `cmd/amber-store/spec.go`**

```go
package main

import (
	"context"
	"strings"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
)

// resolveSpec parses a content spec: either KEY[/PATH] (lowercase-hex key,
// slash-separated subpath) or ref:NAME[@PATH] (reference name, '@'-separated
// subpath — '@' is banned in names, so the first '@' is unambiguous).
// Reference names resolve through the daemon.
func resolveSpec(ctx context.Context, cl *client.Client, s string) (key.Key, string, error) {
	rest, isRef := strings.CutPrefix(s, "ref:")
	if !isRef {
		return parseKeyPath(s)
	}
	name, path, _ := strings.Cut(rest, "@")
	if err := reference.ValidateName(name); err != nil {
		return key.Key{}, "", err
	}
	rec, err := cl.GetRef(ctx, name)
	if err != nil {
		return key.Key{}, "", err
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, "", err
	}
	return k, path, nil
}
```

- [ ] **Step 4: Wire it into the four read commands**

Each command currently parses first, then builds the client. Reorder: build the client, then resolve. The diffs:

`ls.go` `runLs`:

```go
	cl := client.New(socketpath.Resolve(cfg.socket))
	k, path, err := resolveSpec(c.Context, cl, c.Args().First())
	if err != nil {
		return err
	}
	entries, err := cl.Ls(c.Context, k, path)
```

`dump.go` `runDump`:

```go
	cl := client.New(socketpath.Resolve(cfg.socket))
	k, path, err := resolveSpec(c.Context, cl, c.Args().First())
	if err != nil {
		return err
	}
	body, err := cl.Tar(c.Context, k, path)
```

`content_keys.go` (same pattern as `ls.go` — client first, then `resolveSpec`, then the existing `cl.ContentKeys(c.Context, k, path)` call).

`restore.go` (first arg is the spec, second the destination — keep the rest):

```go
	cl := client.New(socketpath.Resolve(cfg.socket))
	k, path, err := resolveSpec(c.Context, cl, c.Args().Get(0))
	if err != nil {
		return err
	}
```

…and replace that command's later `client.New(...)` use with `cl`. Update each command's `ArgsUsage` from `"KEY[/PATH]"` to `"KEY[/PATH] | ref:NAME[@PATH]"` (restore: `"KEY[/PATH] | ref:NAME[@PATH] DIR"`) and mention it in `Usage`.

- [ ] **Step 5: Run the full suite**

Run: `go test ./cmd/amber-store/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/
git commit -m "accept ref:NAME@PATH specs in ls, dump, restore, content-keys"
```

---

### Task 9: documentation and final verification

**Files:**
- Create: `architecture/references.md`
- Modify: `architecture/daemon.md` (route table, naming paragraph, command table)
- Modify: `README.md` (usage section, architecture table)

- [ ] **Step 1: Write `architecture/references.md`**

Content (full file):

```markdown
# References

A **reference** is a global name pointing at a store key (a file or a
directory), recorded with its creator and creation time, with room for a
signature. References give roots names: daemon-mode `ingest` always creates
one, and any `KEY[/PATH]` argument also accepts `ref:NAME[@PATH]`.

## The record

A reference is a canonical CBOR map (RFC 8949 §4.2 core-deterministic, the
same convention as fstree objects) with integer keys:

| CBOR key | Field | CBOR type | Notes |
| --- | --- | --- | --- |
| 0 | name | text string | global reference name |
| 1 | key | 32-byte byte string | pointed-to key, canonical per [keys.md](keys.md) |
| 2 | user | text string | creator, from the user config (`amber-store config-user`) |
| 3 | created_at | int64 | ns since the Unix epoch |
| 4 | signature | byte string, omitted when absent | opaque bytes; reserved |

The **signature payload** is the deterministic encoding of the record without
key 4 — the canonical bytes of `{0,1,2,3}`. Nothing currently produces or
verifies signatures; the daemon stores the field opaquely.

**Name rules:** 1–1024 bytes of valid UTF-8; no `@` (the ref/path separator)
and no control characters. `/` is allowed (`backups/2026/06`) but has no
structural meaning — names are opaque strings, compared whole.

**Mutability:** references are overwritable; a PUT for an existing name
replaces the record unconditionally. There is no history.

## Storage

The daemon owns a second Pebble DB at `<store-dir>/refs/`, next to the object
store's `db/`: DB key = name bytes, value = the CBOR record verbatim. Write
durability follows the daemon's `--sync` flag. Listing is an iterator scan in
lexicographic name order.

## Wire protocol

The name travels as a `?name=` query parameter (never a path segment — names
may contain `/`, `..`, or empty segments that URL path cleaning would mangle):

| Route | Body / response | Errors |
| --- | --- | --- |
| `PUT /v1/refs?name=N` | CBOR record in → `204` | missing name, bad CBOR, record/query name mismatch, invalid name, non-canonical key → `422`; pointed-to key absent → `404` |
| `GET /v1/refs?name=N` | CBOR record out (`application/cbor`) | absent → `404` |
| `GET /v1/refs` | NDJSON: `name`, `key` (hex), `user`, `created_at` (RFC 3339), `signed`; name order | — |
| `DELETE /v1/refs?name=N` | `204` | missing name → `422`; absent → `404` |

`PUT` requires the pointed-to key to exist in the store — no dangling
references. Resolution of `ref:NAME@PATH` is client-side: one
`GET /v1/refs?name=`, then the ordinary key routes.

## CLI

```sh
amber-store config-user NAME             # required once before creating refs
amber-store ingest NAME DIR              # daemon ingest names its root
amber-store ref create NAME KEY          # name an existing key (e.g. after load)
amber-store ref ls                       # name, key, user, date
amber-store ref show NAME                # full record as JSON
amber-store ref rm NAME                  # delete the name; objects stay
amber-store ls ref:NAME[@PATH]           # any KEY[/PATH] argument accepts this
```
```

- [ ] **Step 2: Update `architecture/daemon.md`**

1. In the route table, append the four rows:

```markdown
| `PUT /v1/refs?name=`               | CBOR reference record in → 204                    |
| `GET /v1/refs?name=`               | CBOR reference record out                         |
| `GET /v1/refs`                     | NDJSON, one reference per line, name order        |
| `DELETE /v1/refs?name=`            | 204                                               |
```

2. In "The pack-write format" section, replace the final sentence

> The root key is the client's output (printed by `ingest`), and naming or persisting roots is the caller's concern.

with:

> The root key is the client's output (printed by `ingest`); naming roots is
> handled by [references](references.md), which daemon-mode `ingest` creates
> after the upload completes.

3. In the "Division of labor" command table, update the `ingest DIR` row to `ingest NAME DIR` and append `; PUT the reference` to its client-side work, and add a row:

```markdown
| `ref create/ls/show/rm` | render output | reference CRUD against the refs DB |
```

- [ ] **Step 3: Update `README.md`**

1. In the usage section, change the ingest example to:

```sh
amber-store config-user alice                  # once: who creates references
amber-store ingest backups/home ./some/dir     # ingest + name the root
```

2. After the `restore` line, add:

```sh
amber-store ref ls                       # list references
amber-store ls ref:backups/home@sub/dir  # ref:NAME[@PATH] works wherever KEY[/PATH] does
```

3. In the architecture table, add:

```markdown
| [`architecture/references.md`](architecture/references.md) | Named pointers to keys: record layout, name rules, storage, routes. |
```

- [ ] **Step 4: Full verification**

```bash
go build ./...
go vet ./...
gofmt -l .          # expect no output
go test ./...
```

Expected: everything passes, gofmt prints nothing.

- [ ] **Step 5: Commit**

```bash
git add architecture/ README.md
git commit -m "document references"
```
