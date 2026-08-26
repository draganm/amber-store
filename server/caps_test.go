package server_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/httpsig"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"github.com/draganm/amber-store/sshsign"
	"golang.org/x/crypto/ssh"
)

func capsSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func keyLine(s ssh.Signer) string {
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(s.PublicKey())))
}

type capsHarness struct {
	srv      *httptest.Server
	identity []byte // server public key, wire format: the request audience
	store    *packstore.Store
	refs     *refstore.Store
}

// newCapsHarness starts a server whose allowlist is built from lines.
func newCapsHarness(t *testing.T, lines ...string) *capsHarness {
	t.Helper()
	dir := t.TempDir()
	store, err := packstore.Open(filepath.Join(dir, "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ib.Close() })
	allow, err := allowlist.Parse([]byte(strings.Join(lines, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	identity := capsSigner(t)
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs, Collector: openTestCollector(t, store, refs),
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return &capsHarness{srv: srv, identity: identity.PublicKey().Marshal(), store: store, refs: refs}
}

// signedDo sends one signed request; grantRaw (may be nil) rides the
// Amber-Grant header.
func signedDo(t *testing.T, h *capsHarness, signer ssh.Signer, grantRaw []byte, method, pathQuery string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+pathQuery, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, signer, h.identity, time.Now().UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	if grantRaw != nil {
		req.Header.Set(grant.Header, base64.StdEncoding.EncodeToString(grantRaw))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// storedBlob puts one blob into the harness store and returns its key.
func storedBlob(t *testing.T, h *capsHarness, payload string) key.Key {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	return o.Key
}

// signedRecord builds a signed reference record for root.
func signedRecord(t *testing.T, signer ssh.Signer, name string, root key.Key) []byte {
	t.Helper()
	rec := reference.Reference{
		Name:      name,
		Key:       root[:],
		User:      "caps-test",
		CreatedAt: time.Now().UnixNano(),
		PublicKey: signer.PublicKey().Marshal(),
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := sshsign.SignWith(signer, payload)
	if err != nil {
		t.Fatal(err)
	}
	rec.Signature = sig
	raw, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPushOnlyKeyCannotWriteRefs(t *testing.T) {
	pushOnly := capsSigner(t)
	h := newCapsHarness(t, "read,push-objects "+keyLine(pushOnly))
	root := storedBlob(t, h, "content")
	raw := signedRecord(t, pushOnly, "steal:1", root)
	resp := signedDo(t, h, pushOnly, nil, http.MethodPut, "/v1/refs?name=steal%3A1", raw)
	if resp.StatusCode != http.StatusForbidden {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("got %d (%s), want 403", resp.StatusCode, b)
	}
}

func TestLegacyKeyKeepsFullAccess(t *testing.T) {
	legacy := capsSigner(t)
	h := newCapsHarness(t, keyLine(legacy)) // no options
	root := storedBlob(t, h, "content")
	raw := signedRecord(t, legacy, "ok:1", root)
	resp := signedDo(t, h, legacy, nil, http.MethodPut, "/v1/refs?name=ok%3A1", raw)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("got %d (%s), want 204", resp.StatusCode, b)
	}
	if resp := signedDo(t, h, legacy, nil, http.MethodGet, "/v1/refs?name=ok%3A1", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("read-back got %d", resp.StatusCode)
	}
	// Legacy keys must not delete (admin only), as today.
	if resp := signedDo(t, h, legacy, nil, http.MethodDelete, "/v1/refs?name=ok%3A1", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("delete got %d, want 403", resp.StatusCode)
	}
}

func TestReadOnlyKeyCannotPushObjects(t *testing.T) {
	readOnly := capsSigner(t)
	h := newCapsHarness(t, "read "+keyLine(readOnly))
	o, err := fstree.EncodeBlob([]byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	resp := signedDo(t, h, readOnly, nil, http.MethodPost,
		"/v1/objects?root="+hex.EncodeToString(o.Key[:]), []byte("pack-bytes-do-not-matter"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("got %d, want 403", resp.StatusCode)
	}
	// But reading works.
	if resp := signedDo(t, h, readOnly, nil, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("list got %d, want 200", resp.StatusCode)
	}
}

func TestGrantAuthorizesUnlistedKey(t *testing.T) {
	engine := capsSigner(t) // allowlisted delegate
	runner := capsSigner(t) // never allowlisted
	h := newCapsHarness(t, "read,push-objects,write-refs,delegate "+keyLine(engine))

	now := time.Now()
	// mint signs a grant expiring expiresIn from now. grant.Sign requires
	// IssuedAt <= ExpiresAt, so a negative expiresIn (used to build an
	// already-expired grant below) backdates IssuedAt along with it —
	// otherwise Sign itself would refuse the (legitimately nonsensical)
	// issued-after-it-expires ordering before Verify ever saw the grant.
	mint := func(caps []string, expiresIn time.Duration) []byte {
		t.Helper()
		issuedAt, expiresAt := now, now.Add(expiresIn)
		if expiresAt.Before(issuedAt) {
			issuedAt = expiresAt.Add(-time.Minute)
		}
		raw, err := grant.Sign(grant.Grant{
			Subject:   runner.PublicKey().Marshal(),
			Caps:      caps,
			IssuedAt:  issuedAt.UnixNano(),
			ExpiresAt: expiresAt.UnixNano(),
		}, engine)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	g := mint([]string{allowlist.CapRead, allowlist.CapPushObjects}, 15*time.Minute)

	// Without a grant: 403.
	if resp := signedDo(t, h, runner, nil, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("no grant: got %d, want 403", resp.StatusCode)
	}
	// With the grant: reads work.
	if resp := signedDo(t, h, runner, g, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("with grant: got %d (%s), want 200", resp.StatusCode, b)
	}
	// The grant never authorizes a ref write.
	root := storedBlob(t, h, "content")
	raw := signedRecord(t, runner, "steal:2", root)
	if resp := signedDo(t, h, runner, g, http.MethodPut, "/v1/refs?name=steal%3A2", raw); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("grant ref write: got %d, want 403", resp.StatusCode)
	}
	// Expired grant: 403.
	expired := mint([]string{allowlist.CapRead}, -time.Hour)
	if resp := signedDo(t, h, runner, expired, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expired grant: got %d, want 403", resp.StatusCode)
	}
	// Grant bound to a different subject: 403.
	other := capsSigner(t)
	otherGrant, err := grant.Sign(grant.Grant{
		Subject:   other.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, engine)
	if err != nil {
		t.Fatal(err)
	}
	if resp := signedDo(t, h, runner, otherGrant, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign grant: got %d, want 403", resp.StatusCode)
	}
}

func TestGrantFromNonDelegateIsRejected(t *testing.T) {
	fullButNotDelegate := capsSigner(t)
	runner := capsSigner(t)
	h := newCapsHarness(t, keyLine(fullButNotDelegate)) // legacy full access, NOT delegate
	now := time.Now()
	g, err := grant.Sign(grant.Grant{
		Subject:   runner.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, fullButNotDelegate)
	if err != nil {
		t.Fatal(err)
	}
	if resp := signedDo(t, h, runner, g, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("got %d, want 403", resp.StatusCode)
	}
}
