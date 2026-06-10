# Remote Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A remote `amber-store serve` server that local daemons push/pull objects and references to/from over signed HTTP(S), per `docs/superpowers/specs/2026-06-10-remote-server-design.md`.

**Architecture:** New `server` package (TCP sibling of the local daemon, same diskstore/refstore), signed requests/responses via new `internal/httpsig` (canonical-CBOR payloads, blake3 body hashes, SSHSIG signatures), client-side sync in new `remoteclient` + `remotesync` packages driven by new local-daemon routes, thin CLI on top.

**Tech Stack:** Go 1.26, existing deps only (`golang.org/x/crypto/ssh`, `hiddeco/sshsig`, `zeebo/blake3`, `fxamacker/cbor/v2`, `urfave/cli/v2`, Pebble via existing diskstore/refstore).

**Conventions for every task:** run tests with `go test ./<pkg>/ -run <Name> -v` from the repo root; run `gofmt -w` on touched files before committing; commit messages follow the repo's `feat:`/`test:`/`refactor:`/`docs:` style and end with the Claude co-author trailer. Tests use `t.TempDir()` stores opened with `diskstore.WithSync(false)`.

**File map (what gets created where):**

| Path | Responsibility |
| --- | --- |
| `internal/sshsign/sshsign.go` | (modify) namespace-parameterized sign/verify |
| `internal/keylist/keylist.go` | flatten/parse concatenated 32-byte key lists |
| `internal/httpsig/httpsig.go` | signed-request/response payloads, headers, verify |
| `internal/allowlist/allowlist.go` | authorized_keys allowlist with `admin` option |
| `internal/nonces/nonces.go` | replay cache (window-bounded nonce set) |
| `fstree/children.go` | `ChildKeys` extraction (refactor of `reachable.go`) |
| `internal/remotes/remotes.go` | persistent NAME → {URL, server key} registry |
| `server/server.go`, `server/sign.go`, `server/objects.go`, `server/refs.go` | the remote server handler |
| `remoteclient/remoteclient.go`, `remoteclient/objects.go`, `remoteclient/refs.go` | signed HTTP client with pinned server key |
| `remotesync/batch.go`, `remotesync/push.go`, `remotesync/pull.go` | byte-balanced batching + push/pull algorithms |
| `daemon/remotes.go`, `daemon/remotesync.go` | local-daemon remote routes |
| `client/remote.go` | local-daemon client methods for the new routes |
| `cmd/amber-store/serve.go`, `cmd/amber-store/remote.go` | `serve` command, `remote` command group |
| `cmd/amber-store/daemon.go` | (modify) `--remote-key` flags, registry wiring |
| `architecture/remote.md` | protocol/auth documentation |

---

### Task 1: sshsign namespace-parameterized sign/verify

The HTTP signatures need their own SSHSIG namespace (`amber-store-http`); today `SignWith`/`Verify` hardcode `amber-store-ref`.

**Files:**
- Modify: `internal/sshsign/sshsign.go`
- Test: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Write the failing test** (append to `internal/sshsign/sshsign_test.go`; it already has `writeKeyFiles` and ed25519 imports)

```go
func TestSignNamespaceRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("payload bytes")
	blob, err := sshsign.SignNamespace(signer, payload, "amber-store-http")
	if err != nil {
		t.Fatal(err)
	}
	pubWire := signer.PublicKey().Marshal()
	if _, err := sshsign.VerifyNamespace(payload, blob, pubWire, "amber-store-http"); err != nil {
		t.Fatalf("verify in same namespace: %v", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, blob, pubWire, "amber-store-ref"); err == nil {
		t.Fatal("verify in a different namespace succeeded, want error")
	}
	if _, err := sshsign.VerifyNamespace([]byte("other"), blob, pubWire, "amber-store-http"); err == nil {
		t.Fatal("verify of different payload succeeded, want error")
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — `go test ./internal/sshsign/ -run TestSignNamespaceRoundTrip -v` fails to compile: `undefined: sshsign.SignNamespace`.

- [ ] **Step 3: Implement** — in `internal/sshsign/sshsign.go`, replace the bodies of `SignWith` and `Verify` with delegations and add the namespace variants:

```go
// SignNamespace signs payload in the given SSHSIG namespace, returning a raw
// (un-armored) SSHSIG blob.
func SignNamespace(signer ssh.Signer, payload []byte, namespace string) ([]byte, error) {
	sig, err := sshsig.Sign(bytes.NewReader(payload), signer, sshsig.HashSHA512, namespace)
	if err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}
	return sig.Marshal(), nil
}

// SignWith signs payload with signer, returning a raw (un-armored) SSHSIG
// blob in the amber-store reference namespace.
func SignWith(signer ssh.Signer, payload []byte) ([]byte, error) {
	return SignNamespace(signer, payload, Namespace)
}

// VerifyNamespace checks that blob is a valid SSHSIG over payload in the
// given namespace, made by the key pubWire (SSH wire format). It returns the
// signer's public key on success. This is an integrity check only — it does
// not establish that the key belongs to anyone in particular.
func VerifyNamespace(payload, blob, pubWire []byte, namespace string) (ssh.PublicKey, error) {
	sig, err := sshsig.ParseSignature(blob)
	if err != nil {
		return nil, fmt.Errorf("parsing signature: %w", err)
	}
	if !bytes.Equal(sig.PublicKey.Marshal(), pubWire) {
		return nil, errors.New("recorded public key differs from the signature's")
	}
	// The blob's declared hash algorithm is part of the signed structure, so
	// honoring it (rather than pinning SHA-512) does not weaken the check.
	if err := sshsig.Verify(bytes.NewReader(payload), sig, sig.PublicKey, sig.HashAlgorithm, namespace); err != nil {
		return nil, err
	}
	return sig.PublicKey, nil
}

// Verify checks blob over payload in the amber-store reference namespace.
func Verify(payload, blob, pubWire []byte) (ssh.PublicKey, error) {
	return VerifyNamespace(payload, blob, pubWire, Namespace)
}
```

(Delete the old `SignWith`/`Verify` bodies these replace; the doc comments on the existing `Verify` move to `VerifyNamespace`.)

- [ ] **Step 4: Run the package tests, expect PASS** — `go test ./internal/sshsign/ -v` (all existing tests must still pass).

- [ ] **Step 5: Commit**

```bash
git add internal/sshsign/
git commit -m "refactor: sshsign sign/verify take an explicit namespace"
```

---

### Task 2: internal/keylist — concatenated key lists

The wire format for "which of these keys…" requests/responses: raw concatenated 32-byte keys.

**Files:**
- Create: `internal/keylist/keylist.go`
- Test: `internal/keylist/keylist_test.go`

- [ ] **Step 1: Write the failing test**

```go
package keylist_test

import (
	"bytes"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
)

func testKeys(t *testing.T) []key.Key {
	t.Helper()
	var out []key.Key
	for _, data := range [][]byte{[]byte("one"), []byte("two two"), []byte("three three three")} {
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o.Key)
	}
	return out
}

func TestFlattenParseRoundTrip(t *testing.T) {
	keys := testKeys(t)
	b := keylist.Flatten(keys)
	if len(b) != len(keys)*key.Size {
		t.Fatalf("flattened length = %d, want %d", len(b), len(keys)*key.Size)
	}
	got, err := keylist.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("parsed %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("key %d = %s, want %s", i, got[i], keys[i])
		}
	}
}

func TestParseRejectsBadLength(t *testing.T) {
	if _, err := keylist.Parse(make([]byte, key.Size+1)); err == nil {
		t.Fatal("want error for length not a multiple of 32")
	}
}

func TestParseRejectsNonCanonicalKey(t *testing.T) {
	bad := make([]byte, key.Size) // header 0x00 with zero length byte: non-canonical
	bad[0] = 0x01                 // length-field size 2, but first length byte is 0x00
	if _, err := keylist.Parse(bad); err == nil {
		t.Fatal("want error for non-canonical key")
	}
}

func TestEmptyList(t *testing.T) {
	if b := keylist.Flatten(nil); len(b) != 0 {
		t.Fatalf("Flatten(nil) = %d bytes, want 0", len(b))
	}
	got, err := keylist.Parse(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("Parse(nil) = %v, %v; want empty, nil", got, err)
	}
	_ = bytes.MinRead // keep bytes import if unused otherwise
}
```

(Drop the `bytes` import and the `_ = bytes.MinRead` line if nothing else uses it.)

- [ ] **Step 2: Run it, expect FAIL** — `go test ./internal/keylist/ -v` fails: package does not exist.

- [ ] **Step 3: Implement** `internal/keylist/keylist.go`:

```go
// Package keylist encodes lists of store keys as raw concatenated 32-byte
// keys — the request/response body format of the remote server's
// have/want negotiation. Dense, order-preserving, zero parsing ambiguity:
// the byte length must be a multiple of key.Size and every key canonical.
package keylist

import (
	"fmt"

	"github.com/draganm/amber-store/key"
)

// Flatten concatenates keys in order.
func Flatten(keys []key.Key) []byte {
	out := make([]byte, 0, len(keys)*key.Size)
	for _, k := range keys {
		out = append(out, k[:]...)
	}
	return out
}

// Parse splits b into canonical keys. It rejects a length that is not a
// multiple of key.Size and any non-canonical key.
func Parse(b []byte) ([]key.Key, error) {
	if len(b)%key.Size != 0 {
		return nil, fmt.Errorf("key list length %d is not a multiple of %d", len(b), key.Size)
	}
	keys := make([]key.Key, 0, len(b)/key.Size)
	for i := 0; i < len(b); i += key.Size {
		k, err := key.Parse(b[i : i+key.Size])
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i/key.Size, err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./internal/keylist/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/keylist/
git commit -m "feat: keylist encodes concatenated 32-byte key lists"
```

---

### Task 3: internal/httpsig — signed requests and responses

The auth core: canonical-CBOR payloads `{method, path+query, timestamp, nonce, blake3(body)}` for requests and `{nonce, status, blake3(body)}` for responses, signed as SSHSIG in namespace `amber-store-http`, carried in `Amber-*` headers.

**Files:**
- Create: `internal/httpsig/httpsig.go`
- Test: `internal/httpsig/httpsig_test.go`

- [ ] **Step 1: Write the failing test**

```go
package httpsig_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/internal/httpsig"
	"golang.org/x/crypto/ssh"
)

func testSigner(t *testing.T) ssh.Signer {
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

// signedRequest builds a client request and the equivalent server-side view.
func signedRequest(t *testing.T, signer ssh.Signer, ts int64, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest("POST", "http://server/v1/objects/missing?x=1", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, signer, ts, []byte("nonce-16-bytes!!"), body); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestRequestRoundTrip(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	req := signedRequest(t, signer, now.UnixNano(), body)
	pub, nonce, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if string(nonce) != "nonce-16-bytes!!" {
		t.Fatalf("nonce = %q", nonce)
	}
	if pub.Type() != signer.PublicKey().Type() {
		t.Fatalf("pub type = %s", pub.Type())
	}
}

func TestRequestRejectsTamperedBody(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	req := signedRequest(t, signer, now.UnixNano(), []byte("the body"))
	if _, _, err := httpsig.VerifyRequest(req, []byte("evil body"), now, httpsig.DefaultWindow); err == nil {
		t.Fatal("tampered body verified")
	}
}

func TestRequestRejectsTamperedPath(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	req := signedRequest(t, signer, now.UnixNano(), body)
	req.URL.Path = "/v1/refs"
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("tampered path verified")
	}
}

func TestRequestRejectsStaleTimestamp(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("b")
	req := signedRequest(t, signer, now.Add(-10*time.Minute).UnixNano(), body)
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("stale timestamp verified")
	}
	// future timestamps beyond the window are rejected too
	req = signedRequest(t, signer, now.Add(10*time.Minute).UnixNano(), body)
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("future timestamp verified")
	}
}

func TestRequestRejectsMissingHeaders(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://server/v1/refs", nil)
	if _, _, err := httpsig.VerifyRequest(req, nil, time.Now(), httpsig.DefaultWindow); err == nil {
		t.Fatal("unsigned request verified")
	}
}

func TestResponseRoundTrip(t *testing.T) {
	signer := testSigner(t)
	body := []byte("response body")
	nonce := []byte("nonce-16-bytes!!")
	sig, err := httpsig.SignResponse(signer, nonce, 200, httpsig.HashBody(body))
	if err != nil {
		t.Fatal(err)
	}
	pubWire := signer.PublicKey().Marshal()
	if err := httpsig.VerifyResponse(pubWire, nonce, 200, httpsig.HashBody(body), sig); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := httpsig.VerifyResponse(pubWire, nonce, 404, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong status verified")
	}
	if err := httpsig.VerifyResponse(pubWire, []byte("other nonce!!!!!"), 200, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong nonce verified")
	}
	if err := httpsig.VerifyResponse(pubWire, nonce, 200, httpsig.HashBody([]byte("evil")), sig); err == nil {
		t.Fatal("wrong body hash verified")
	}
	other := testSigner(t).PublicKey().Marshal()
	if err := httpsig.VerifyResponse(other, nonce, 200, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong key verified")
	}
}

func TestResponseNilNonceEqualsEmpty(t *testing.T) {
	signer := testSigner(t)
	sig, err := httpsig.SignResponse(signer, nil, 200, httpsig.HashBody([]byte("b")))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.VerifyResponse(signer.PublicKey().Marshal(), []byte{}, 200, httpsig.HashBody([]byte("b")), sig); err != nil {
		t.Fatalf("nil-signed nonce did not verify as empty: %v", err)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — `go test ./internal/httpsig/ -v`: package does not exist.

- [ ] **Step 3: Implement** `internal/httpsig/httpsig.go`:

```go
// Package httpsig signs and verifies amber-store remote-protocol HTTP
// messages. A request signature covers a canonical CBOR map of
// {method, path+query, timestamp, nonce, blake3(body)}; a response signature
// covers {request nonce, status, blake3(body)}. Both are SSHSIG blobs in the
// amber-store-http namespace, base64 in Amber-* headers. The expensive part —
// hashing a multi-megabyte body — is blake3; SSHSIG's internal SHA-512 only
// covers the ~100-byte canonical payload.
package httpsig

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/fxamacker/cbor/v2"
	"github.com/zeebo/blake3"
	"golang.org/x/crypto/ssh"
)

// Header names of the four signature components.
const (
	HeaderPublicKey = "Amber-Public-Key" // signer's public key, SSH wire format, base64
	HeaderTimestamp = "Amber-Timestamp"  // ns since the Unix epoch, decimal
	HeaderNonce     = "Amber-Nonce"      // random bytes, base64
	HeaderSignature = "Amber-Signature"  // raw SSHSIG blob, base64
)

// Namespace is the SSHSIG namespace for amber-store HTTP signatures.
const Namespace = "amber-store-http"

// DefaultWindow is the default timestamp validity window (each side of now).
const DefaultWindow = 5 * time.Minute

// encMode is the shared deterministic encoder, mirroring the reference and
// fstree conventions.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("httpsig: building CBOR enc mode: %v", err))
	}
	encMode = m
}

type requestPayload struct {
	Method    string `cbor:"0,keyasint"`
	PathQuery string `cbor:"1,keyasint"`
	Timestamp int64  `cbor:"2,keyasint"`
	Nonce     []byte `cbor:"3,keyasint"`
	BodyHash  []byte `cbor:"4,keyasint"`
}

type responsePayload struct {
	Nonce    []byte `cbor:"0,keyasint"`
	Status   int64  `cbor:"1,keyasint"`
	BodyHash []byte `cbor:"2,keyasint"`
}

// HashBody returns the blake3-256 hash of body.
func HashBody(body []byte) []byte {
	h := blake3.Sum256(body)
	return h[:]
}

// notNil maps nil to an empty slice so a nil and an empty nonce/hash encode
// identically.
func notNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

func requestSigPayload(method, pathQuery string, timestamp int64, nonce, bodyHash []byte) ([]byte, error) {
	return encMode.Marshal(requestPayload{
		Method:    method,
		PathQuery: pathQuery,
		Timestamp: timestamp,
		Nonce:     notNil(nonce),
		BodyHash:  notNil(bodyHash),
	})
}

// SignRequest signs req's method, path+query, timestamp, nonce and body, and
// sets the four Amber-* headers. body must be the exact bytes the request
// will send.
func SignRequest(req *http.Request, signer ssh.Signer, timestamp int64, nonce, body []byte) error {
	payload, err := requestSigPayload(req.Method, req.URL.RequestURI(), timestamp, nonce, HashBody(body))
	if err != nil {
		return fmt.Errorf("encoding request signature payload: %w", err)
	}
	sig, err := sshsign.SignNamespace(signer, payload, Namespace)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderPublicKey, base64.StdEncoding.EncodeToString(signer.PublicKey().Marshal()))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(timestamp, 10))
	req.Header.Set(HeaderNonce, base64.StdEncoding.EncodeToString(nonce))
	req.Header.Set(HeaderSignature, base64.StdEncoding.EncodeToString(sig))
	return nil
}

// VerifyRequest checks r's Amber-* headers against body: the timestamp must
// be within window of now and the signature must verify over the
// reconstructed payload with the claimed public key. It returns the claimed
// key and the nonce; the caller still must check the nonce for replay and
// the key against an allowlist. Every failure is a generic authentication
// error suitable for a 401.
func VerifyRequest(r *http.Request, body []byte, now time.Time, window time.Duration) (ssh.PublicKey, []byte, error) {
	pubB64 := r.Header.Get(HeaderPublicKey)
	tsStr := r.Header.Get(HeaderTimestamp)
	nonceB64 := r.Header.Get(HeaderNonce)
	sigB64 := r.Header.Get(HeaderSignature)
	if pubB64 == "" || tsStr == "" || nonceB64 == "" || sigB64 == "" {
		return nil, nil, errors.New("request is not signed (missing Amber-* headers)")
	}
	pubWire, err := base64.StdEncoding.DecodeString(pubB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderPublicKey, err)
	}
	pub, err := ssh.ParsePublicKey(pubWire)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing public key: %w", err)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing %s: %w", HeaderTimestamp, err)
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderNonce, err)
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderSignature, err)
	}
	d := now.Sub(time.Unix(0, ts))
	if d < -window || d > window {
		return nil, nonce, fmt.Errorf("request timestamp outside the ±%s window", window)
	}
	payload, err := requestSigPayload(r.Method, r.URL.RequestURI(), ts, nonce, HashBody(body))
	if err != nil {
		return nil, nonce, fmt.Errorf("encoding request signature payload: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, sig, pubWire, Namespace); err != nil {
		return nil, nonce, fmt.Errorf("request signature: %w", err)
	}
	return pub, nonce, nil
}

// SignResponse signs {nonce, status, bodyHash} with the server identity,
// returning the base64 header/trailer value. nonce is the request's nonce,
// binding the response to its request.
func SignResponse(signer ssh.Signer, nonce []byte, status int, bodyHash []byte) (string, error) {
	payload, err := encMode.Marshal(responsePayload{
		Nonce:    notNil(nonce),
		Status:   int64(status),
		BodyHash: notNil(bodyHash),
	})
	if err != nil {
		return "", fmt.Errorf("encoding response signature payload: %w", err)
	}
	sig, err := sshsign.SignNamespace(signer, payload, Namespace)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// VerifyResponse checks a response signature against the pinned server key
// (SSH wire format), the request nonce, the response status and body hash.
func VerifyResponse(serverPubWire, nonce []byte, status int, bodyHash []byte, sigB64 string) error {
	if sigB64 == "" {
		return errors.New("response is not signed (missing Amber-Signature)")
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("decoding response signature: %w", err)
	}
	payload, err := encMode.Marshal(responsePayload{
		Nonce:    notNil(nonce),
		Status:   int64(status),
		BodyHash: notNil(bodyHash),
	})
	if err != nil {
		return fmt.Errorf("encoding response signature payload: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, sig, serverPubWire, Namespace); err != nil {
		return fmt.Errorf("response signature: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./internal/httpsig/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/httpsig/
git commit -m "feat: httpsig signs and verifies remote-protocol requests and responses"
```

---

### Task 4: internal/allowlist — authorized_keys with admin option

**Files:**
- Create: `internal/allowlist/allowlist.go`
- Test: `internal/allowlist/allowlist_test.go`

- [ ] **Step 1: Write the failing test**

```go
package allowlist_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/allowlist"
	"golang.org/x/crypto/ssh"
)

func testPub(t *testing.T) ssh.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

func TestParseLookupAndAdmin(t *testing.T) {
	plain, admin, absent := testPub(t), testPub(t), testPub(t)
	content := "# fleet keys\n\n" +
		string(ssh.MarshalAuthorizedKey(plain)) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin))
	l, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := l.Lookup(plain.Marshal()); !ok || e.Admin {
		t.Fatalf("plain key: ok=%v admin=%v, want ok, non-admin", ok, e.Admin)
	}
	if e, ok := l.Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key: ok=%v admin=%v, want ok, admin", ok, e.Admin)
	}
	if _, ok := l.Lookup(absent.Marshal()); ok {
		t.Fatal("absent key found")
	}
}

func TestParseRejectsGarbageLine(t *testing.T) {
	if _, err := allowlist.Parse([]byte("not a key\n")); err == nil {
		t.Fatal("want parse error")
	}
}

func TestLoad(t *testing.T) {
	pub := testPub(t)
	p := filepath.Join(t.TempDir(), "allowed")
	if err := os.WriteFile(p, ssh.MarshalAuthorizedKey(pub), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := allowlist.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.Lookup(pub.Marshal()); !ok {
		t.Fatal("loaded key not found")
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — package does not exist.

- [ ] **Step 3: Implement** `internal/allowlist/allowlist.go`:

```go
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
		return nil, err
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
```

- [ ] **Step 4: Run, expect PASS** — `go test ./internal/allowlist/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/allowlist/
git commit -m "feat: allowlist parses allowed-keys files with the admin option"
```

---

### Task 5: internal/nonces — replay cache

**Files:**
- Create: `internal/nonces/nonces.go`
- Test: `internal/nonces/nonces_test.go`

- [ ] **Step 1: Write the failing test**

```go
package nonces_test

import (
	"testing"
	"time"

	"github.com/draganm/amber-store/internal/nonces"
)

func TestReplayDetection(t *testing.T) {
	c := nonces.New(time.Minute)
	now := time.Now()
	if c.SeenBefore("fp1", []byte("n1"), now) {
		t.Fatal("fresh nonce reported as seen")
	}
	if !c.SeenBefore("fp1", []byte("n1"), now) {
		t.Fatal("replayed nonce not detected")
	}
	if c.SeenBefore("fp2", []byte("n1"), now) {
		t.Fatal("same nonce from a different key reported as seen")
	}
}

func TestExpiry(t *testing.T) {
	c := nonces.New(time.Minute)
	now := time.Now()
	c.SeenBefore("fp", []byte("n"), now)
	// After more than 2× the window the entry is expired (timestamps that old
	// are rejected before the nonce check, so forgetting them is safe).
	later := now.Add(3 * time.Minute)
	if c.SeenBefore("fp", []byte("n"), later) {
		t.Fatal("expired nonce still reported as seen")
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — package does not exist.

- [ ] **Step 3: Implement** `internal/nonces/nonces.go`:

```go
// Package nonces is the remote server's replay cache: a set of recently seen
// (key fingerprint, nonce) pairs. Entries are kept for twice the timestamp
// window — requests older than the window are rejected before the nonce
// check, so an expired entry can never be replayed successfully.
package nonces

import (
	"sync"
	"time"
)

// Cache remembers nonces for 2× the timestamp window. Safe for concurrent use.
type Cache struct {
	mu     sync.Mutex
	window time.Duration
	seen   map[string]time.Time // (id + "\x00" + nonce) → insertion time
}

// New returns a Cache for the given timestamp window.
func New(window time.Duration) *Cache {
	return &Cache{window: window, seen: map[string]time.Time{}}
}

// SeenBefore reports whether (id, nonce) was already recorded within the
// retention period, recording it if not. id should identify the signing key
// (e.g. its fingerprint).
func (c *Cache) SeenBefore(id string, nonce []byte, now time.Time) bool {
	k := id + "\x00" + string(nonce)
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := now.Add(-2 * c.window)
	for ek, et := range c.seen {
		if et.Before(cutoff) {
			delete(c.seen, ek)
		}
	}
	if t, ok := c.seen[k]; ok && !t.Before(cutoff) {
		return true
	}
	c.seen[k] = now
	return false
}
```

(The full sweep on every call is O(entries); entries are bounded by request rate × window, and the remote protocol's request rate is batch-granular — fine for v1.)

- [ ] **Step 4: Run, expect PASS** — `go test ./internal/nonces/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/nonces/
git commit -m "feat: nonces replay cache for the remote-protocol auth"
```

---

### Task 6: fstree.ChildKeys — child extraction, ReachableKeys refactor

Pull needs to parse a fetched object's children without walking a store. Extract the type-switch from `ReachableKeys` into an exported helper and reuse it.

**Files:**
- Create: `fstree/children.go`
- Modify: `fstree/reachable.go`
- Test: `fstree/children_test.go`

- [ ] **Step 1: Write the failing test**

```go
package fstree_test

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

func TestChildKeysBlobHasNone(t *testing.T) {
	o, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(o.Key, o.Bytes)
	if err != nil || len(kids) != 0 {
		t.Fatalf("blob children = %v, %v; want none", kids, err)
	}
}

func TestChildKeysFileNode(t *testing.T) {
	b1, err := fstree.EncodeBlob([]byte("chunk one"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fstree.EncodeBlob([]byte("chunk two"))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key})
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(fn.Key, fn.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 2 || kids[0] != b1.Key || kids[1] != b2.Key {
		t.Fatalf("FileNode children = %v", kids)
	}
}

func TestChildKeysDirLeaf(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("file content"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: blob.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(leaf.Key, leaf.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0] != blob.Key {
		t.Fatalf("DirLeaf children = %v", kids)
	}
}
```

(If `fstree.Entry` field names differ — check `fstree/encode.go` `EncodeDirLeaf` and the `Entry` struct in the package before writing — adjust the literal accordingly.)

- [ ] **Step 2: Run it, expect FAIL** — `undefined: fstree.ChildKeys`.

- [ ] **Step 3: Implement** `fstree/children.go`:

```go
package fstree

import (
	"fmt"

	"github.com/draganm/amber-store/key"
)

// ChildKeys returns the keys directly referenced by the object with key k
// and serialized bytes data, in encounter order. Blob and XattrSet objects
// are leaves and have no children.
func ChildKeys(k key.Key, data []byte) ([]key.Key, error) {
	switch k.Type() {
	case key.Blob, key.XattrSet:
		return nil, nil
	case key.FileNode:
		children, err := DecodeFileNode(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding FileNode %s: %w", k, err)
		}
		return children, nil
	case key.DirNode:
		pairs, err := DecodeDirNode(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
		out := make([]key.Key, 0, len(pairs))
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			out = append(out, ck)
		}
		return out, nil
	case key.DirLeaf:
		entries, err := DecodeDirLeaf(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
		var out []key.Key
		for _, ent := range entries {
			if len(ent.ContentKey) > 0 {
				ck, err := key.Parse(ent.ContentKey)
				if err != nil {
					return nil, fmt.Errorf("fstree: %q: content key: %w", ent.Name, err)
				}
				out = append(out, ck)
			}
			if len(ent.XattrsKey) > 0 {
				xk, err := key.Parse(ent.XattrsKey)
				if err != nil {
					return nil, fmt.Errorf("fstree: %q: xattrs key: %w", ent.Name, err)
				}
				out = append(out, xk)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("fstree: unknown object type %s", k.Type())
	}
}
```

- [ ] **Step 4: Refactor `fstree/reachable.go`** to use it — replace the body of the inner `walk` func:

```go
	walk = func(k key.Key) error {
		if seen[k] {
			return nil
		}
		seen[k] = true
		out = append(out, k)
		if k.Type() == key.Blob || k.Type() == key.XattrSet {
			return nil
		}
		data, err := get(k)
		if err != nil {
			return fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		children, err := ChildKeys(k, data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := walk(ck); err != nil {
				return err
			}
		}
		return nil
	}
```

- [ ] **Step 5: Run the whole package, expect PASS** — `go test ./fstree/ -v` (existing reachable tests guard the refactor).

- [ ] **Step 6: Commit**

```bash
git add fstree/
git commit -m "refactor: extract fstree.ChildKeys; ReachableKeys uses it"
```

---

### Task 7: internal/remotes — persistent remote registry

`NAME → {URL, server public key}` persisted as JSON at `<store-dir>/remotes` (tmp+rename writes, like userconfig's style).

**Files:**
- Create: `internal/remotes/remotes.go`
- Test: `internal/remotes/remotes_test.go`

- [ ] **Step 1: Write the failing test**

```go
package remotes_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/remotes"
)

func open(t *testing.T, path string) *remotes.Registry {
	t.Helper()
	r, err := remotes.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAddGetPersistRemove(t *testing.T) {
	p := filepath.Join(t.TempDir(), "remotes")
	r := open(t, p)
	rem := remotes.Remote{URL: "https://amber.example.com", ServerKey: []byte("wire-key-bytes")}
	if err := r.Add("origin", rem); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("origin", rem); !errors.Is(err, remotes.ErrExists) {
		t.Fatalf("second add = %v, want ErrExists", err)
	}
	// a fresh Registry sees the persisted state
	r2 := open(t, p)
	got, ok := r2.Get("origin")
	if !ok || got.URL != rem.URL || string(got.ServerKey) != string(rem.ServerKey) {
		t.Fatalf("reloaded remote = %+v, ok=%v", got, ok)
	}
	if err := r2.Remove("origin"); err != nil {
		t.Fatal(err)
	}
	if err := r2.Remove("origin"); !errors.Is(err, remotes.ErrNotFound) {
		t.Fatalf("second remove = %v, want ErrNotFound", err)
	}
	if _, ok := open(t, p).Get("origin"); ok {
		t.Fatal("removed remote survived reload")
	}
}

func TestResolve(t *testing.T) {
	r := open(t, filepath.Join(t.TempDir(), "remotes"))
	if _, _, err := r.Resolve(""); err == nil {
		t.Fatal("resolve with no remotes should fail")
	}
	if err := r.Add("a", remotes.Remote{URL: "http://a", ServerKey: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	name, rem, err := r.Resolve("")
	if err != nil || name != "a" || rem.URL != "http://a" {
		t.Fatalf("sole resolve = %q, %+v, %v", name, rem, err)
	}
	if err := r.Add("b", remotes.Remote{URL: "http://b", ServerKey: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve(""); err == nil {
		t.Fatal("ambiguous resolve should fail")
	}
	if _, _, err := r.Resolve("missing"); err == nil {
		t.Fatal("unknown name should fail")
	}
	if name, _, err := r.Resolve("b"); err != nil || name != "b" {
		t.Fatalf("named resolve = %q, %v", name, err)
	}
}

func TestAddValidates(t *testing.T) {
	r := open(t, filepath.Join(t.TempDir(), "remotes"))
	bad := []struct {
		name string
		rem  remotes.Remote
	}{
		{"", remotes.Remote{URL: "http://x", ServerKey: []byte("k")}},
		{"has/slash", remotes.Remote{URL: "http://x", ServerKey: []byte("k")}},
		{"ok", remotes.Remote{URL: "ftp://x", ServerKey: []byte("k")}},
		{"ok", remotes.Remote{URL: "http://x", ServerKey: nil}},
	}
	for _, tc := range bad {
		if err := r.Add(tc.name, tc.rem); err == nil {
			t.Fatalf("Add(%q, %+v) succeeded, want error", tc.name, tc.rem)
		}
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — package does not exist.

- [ ] **Step 3: Implement** `internal/remotes/remotes.go`:

```go
// Package remotes persists the daemon's registered remotes: a name → {URL,
// pinned server public key} map stored as JSON at <store-dir>/remotes. The
// pinned key is recorded once at `remote add` (after the user confirms its
// fingerprint) and enforced on every response from that remote.
package remotes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
)

// ErrExists reports an Add for a name that is already registered.
var ErrExists = errors.New("remote already exists")

// ErrNotFound reports an operation on an unregistered name.
var ErrNotFound = errors.New("remote not found")

// Remote is one registered remote server.
type Remote struct {
	URL       string `json:"url"`
	ServerKey []byte `json:"server_key"` // pinned public key, SSH wire format
}

// Named pairs a remote with its name, for listings.
type Named struct {
	Name string
	Remote
}

// Registry is the persistent name → Remote map. Safe for concurrent use.
type Registry struct {
	path    string
	mu      sync.Mutex
	remotes map[string]Remote
}

// Open loads the registry at path; a missing file is an empty registry.
func Open(path string) (*Registry, error) {
	r := &Registry{path: path, remotes: map[string]Remote{}}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading remotes file: %w", err)
	}
	if err := json.Unmarshal(b, &r.remotes); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return r, nil
}

// validate checks a remote name and its fields before persisting.
func validate(name string, rem Remote) error {
	if name == "" || len(name) > 64 {
		return errors.New("remote name must be 1-64 bytes")
	}
	for _, c := range name {
		if c == '/' || c < 0x21 || c == 0x7f {
			return fmt.Errorf("remote name %q contains an invalid character", name)
		}
	}
	u, err := url.Parse(rem.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("remote URL %q must be http(s)://host[:port][/path]", rem.URL)
	}
	if len(rem.ServerKey) == 0 {
		return errors.New("remote has no pinned server key")
	}
	return nil
}

// save writes the map atomically (tmp + rename). Caller holds mu.
func (r *Registry) save() error {
	b, err := json.MarshalIndent(r.remotes, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// Add registers a new remote; ErrExists if the name is taken.
func (r *Registry) Add(name string, rem Remote) error {
	if err := validate(name, rem); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.remotes[name]; ok {
		return fmt.Errorf("%w: %s", ErrExists, name)
	}
	r.remotes[name] = rem
	if err := r.save(); err != nil {
		delete(r.remotes, name)
		return err
	}
	return nil
}

// Remove unregisters name; ErrNotFound if absent.
func (r *Registry) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rem, ok := r.remotes[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	delete(r.remotes, name)
	if err := r.save(); err != nil {
		r.remotes[name] = rem
		return err
	}
	return nil
}

// Get returns the remote registered under name.
func (r *Registry) Get(name string) (Remote, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rem, ok := r.remotes[name]
	return rem, ok
}

// All returns every remote in name order.
func (r *Registry) All() []Named {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Named, 0, len(r.remotes))
	for n, rem := range r.remotes {
		out = append(out, Named{Name: n, Remote: rem})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Resolve returns the remote for name; an empty name selects the sole
// registered remote and errors when there are none or several.
func (r *Registry) Resolve(name string) (string, Remote, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name != "" {
		rem, ok := r.remotes[name]
		if !ok {
			return "", Remote{}, fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		return name, rem, nil
	}
	switch len(r.remotes) {
	case 0:
		return "", Remote{}, errors.New("no remotes registered — run 'amber-store remote add NAME URL' first")
	case 1:
		for n, rem := range r.remotes {
			return n, rem, nil
		}
	}
	return "", Remote{}, errors.New("several remotes registered — name one explicitly")
}
```

(The final `return` after the `switch` is unreachable for case 1 but required by the compiler; keep the structure as written.)

- [ ] **Step 4: Run, expect PASS** — `go test ./internal/remotes/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/remotes/
git commit -m "feat: remotes registry persists pinned server keys per remote"
```

---

### Task 8: server package — skeleton, auth middleware, signed responses, identity route

**Files:**
- Create: `server/server.go` (handler, routes, auth middleware)
- Create: `server/sign.go` (signed-response helpers)
- Test: `server/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
package server_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func testSigner(t *testing.T) ssh.Signer {
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

// testServer is the shared harness: a server over fresh stores with one
// allowed client key (and optionally an admin key), returning everything a
// test needs to issue signed requests.
type testServer struct {
	srv      *httptest.Server
	store    *diskstore.Store
	refs     *refstore.Store
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testServer{srv: srv, store: store, refs: refs, identity: identity, client: client, admin: admin}
}

// signedDo sends one signed request and returns status + body, verifying the
// response signature against the server identity along the way.
func (ts *testServer) signedDo(t *testing.T, signer ssh.Signer, method, pathQuery string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ts.srv.URL+pathQuery, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, signer, time.Now().UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	sig := resp.Header.Get(httpsig.HeaderSignature)
	if sig == "" {
		sig = resp.Trailer.Get(httpsig.HeaderSignature)
	}
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nonce, resp.StatusCode,
		httpsig.HashBody(respBody), sig); err != nil {
		t.Fatalf("response signature: %v", err)
	}
	return resp.StatusCode, respBody
}

func TestIdentityIsServedAndSelfSigned(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.srv.URL + "/v1/identity")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !bytes.Equal(body, ts.identity.PublicKey().Marshal()) {
		t.Fatal("identity body is not the server public key")
	}
	// self-signed: the body itself is the verification key, nonce empty
	if err := httpsig.VerifyResponse(body, nil, 200, httpsig.HashBody(body),
		resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		t.Fatalf("identity self-signature: %v", err)
	}
}

func TestAuthRejections(t *testing.T) {
	ts := newTestServer(t)

	// unsigned request → 401
	resp, err := http.Post(ts.srv.URL+"/v1/objects/missing", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("unsigned status = %d, want 401", resp.StatusCode)
	}

	// valid signature, unlisted key → 403
	stranger := testSigner(t)
	if code, _ := ts.signedDo(t, stranger, "POST", "/v1/objects/missing", nil); code != 403 {
		t.Fatalf("unlisted key status = %d, want 403", code)
	}

	// allowed key → 200
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", nil); code != 200 {
		t.Fatalf("allowed key status = %d, want 200", code)
	}
}

func TestReplayedNonceRejected(t *testing.T) {
	ts := newTestServer(t)
	req, err := http.NewRequest("POST", ts.srv.URL+"/v1/objects/missing", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), []byte("fixed-nonce-0123"), nil); err != nil {
		t.Fatal(err)
	}
	send := func() int {
		r2 := req.Clone(req.Context())
		r2.Body = io.NopCloser(bytes.NewReader(nil))
		resp, err := http.DefaultClient.Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := send(); code != 200 {
		t.Fatalf("first send = %d, want 200", code)
	}
	if code := send(); code != 401 {
		t.Fatalf("replay = %d, want 401", code)
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	dirTS := newTestServer(t) // default cap is 64 MiB; use a tiny test server instead
	_ = dirTS
}
```

For the body-cap test, instead of allocating 64 MiB, extend `server.Config` with `MaxBody` and use it:

```go
func TestBodyOverCapRejected(t *testing.T) {
	ts := newTestServer(t) // newTestServer sets no MaxBody → default
	// build a second server with a 1 KiB cap
	small := server.New(server.Config{
		Store:    ts.store,
		Refs:     ts.refs,
		Allow:    func() *allowlist.List { l, _ := allowlist.Parse(ssh.MarshalAuthorizedKey(ts.client.PublicKey())); return l },
		Identity: ts.identity,
		MaxBody:  1024,
	})
	srv := httptest.NewServer(small)
	defer srv.Close()
	body := make([]byte, 2048)
	req, _ := http.NewRequest("POST", srv.URL+"/v1/objects/missing", bytes.NewReader(body))
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), []byte("nonce-16-bytes!!"), body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}
```

(Delete the placeholder `TestOversizedBodyRejected` stub; keep only `TestBodyOverCapRejected`.)

- [ ] **Step 2: Run it, expect FAIL** — `go test ./server/ -v`: package does not exist.

- [ ] **Step 3: Implement** `server/sign.go`:

```go
package server

import (
	"net/http"

	"github.com/draganm/amber-store/internal/httpsig"
)

// signAndWrite signs {nonce, status, blake3(body)} with the server identity
// and writes the response. Every non-streaming response goes through here so
// clients can verify the pinned server key on everything they receive.
func (h *handler) signAndWrite(w http.ResponseWriter, nonce []byte, status int, contentType string, body []byte) {
	sig, err := httpsig.SignResponse(h.identity, nonce, status, httpsig.HashBody(body))
	if err != nil {
		h.log.Error("signing response failed", "error", err)
		http.Error(w, "signing response failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set(httpsig.HeaderSignature, sig)
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

// signError is the signed analogue of http.Error.
func (h *handler) signError(w http.ResponseWriter, nonce []byte, status int, msg string) {
	h.signAndWrite(w, nonce, status, "text/plain; charset=utf-8", []byte(msg+"\n"))
}
```

`server/server.go`:

```go
// Package server implements the amber-store remote server: a TCP HTTP(S)
// sibling of the local daemon that other amber daemons push objects and
// references to and pull them from. Every request must carry a valid
// signature by an allowed SSH key (internal/httpsig); every response is
// signed with the server's identity key so clients can enforce their pinned
// key. See architecture/remote.md.
package server

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/nonces"
	"github.com/draganm/amber-store/refstore"
	"golang.org/x/crypto/ssh"
)

// DefaultMaxBody caps a request body; batches are sized well below it.
const DefaultMaxBody = 64 << 20 // 64 MiB

// Config assembles a server handler.
type Config struct {
	Store    *diskstore.Store
	Refs     *refstore.Store
	Allow    func() *allowlist.List // called per request, enabling hot reload
	Identity ssh.Signer
	Log      *slog.Logger  // nil discards
	Window   time.Duration // timestamp validity window; 0 = httpsig.DefaultWindow
	MaxBody  int64         // request body cap; 0 = DefaultMaxBody
}

type handler struct {
	store    *diskstore.Store
	refs     *refstore.Store
	allow    func() *allowlist.List
	identity ssh.Signer
	log      *slog.Logger
	window   time.Duration
	maxBody  int64
	nonces   *nonces.Cache
}

// New returns the remote server's http.Handler.
func New(cfg Config) http.Handler {
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	window := cfg.Window
	if window == 0 {
		window = httpsig.DefaultWindow
	}
	maxBody := cfg.MaxBody
	if maxBody == 0 {
		maxBody = DefaultMaxBody
	}
	h := &handler{
		store:    cfg.Store,
		refs:     cfg.Refs,
		allow:    cfg.Allow,
		identity: cfg.Identity,
		log:      log,
		window:   window,
		maxBody:  maxBody,
		nonces:   nonces.New(window),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", h.getIdentity)
	mux.HandleFunc("POST /v1/objects/missing", h.auth(h.postMissing))
	mux.HandleFunc("POST /v1/objects", h.auth(h.postObjects))
	mux.HandleFunc("POST /v1/objects/get", h.auth(h.postObjectsGet))
	mux.HandleFunc("PUT /v1/refs", h.auth(h.putRef))
	mux.HandleFunc("GET /v1/refs", h.auth(h.getRefs))
	mux.HandleFunc("DELETE /v1/refs", h.auth(h.deleteRef))
	return logRequests(log, mux)
}

// statusWriter and logRequests mirror the daemon's request logging.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func logRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start),
		)
	})
}

// authedRequest is what the middleware hands an authenticated handler.
type authedRequest struct {
	pubWire []byte // the client's key, SSH wire format
	admin   bool
	nonce   []byte // the request nonce; responses sign over it
	body    []byte // the fully-read request body
}

type authedHandler func(w http.ResponseWriter, r *http.Request, a *authedRequest)

// auth reads the (size-capped) body, verifies the request signature, checks
// the nonce for replay and the key against the allowlist — all before the
// wrapped handler can cause any side effect. Bad signature/timestamp/replay
// are 401; a valid signature by an unlisted key is 403.
func (h *handler) auth(next authedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBody+1))
		if err != nil {
			h.signError(w, nil, http.StatusInternalServerError, err.Error())
			return
		}
		if int64(len(body)) > h.maxBody {
			h.signError(w, nil, http.StatusRequestEntityTooLarge, "request body exceeds the server limit")
			return
		}
		now := time.Now()
		pub, nonce, err := httpsig.VerifyRequest(r, body, now, h.window)
		if err != nil {
			h.log.Warn("request authentication failed", "error", err)
			h.signError(w, nonce, http.StatusUnauthorized, err.Error())
			return
		}
		// Replay check after signature verification so unauthenticated junk
		// cannot grow the nonce cache.
		if h.nonces.SeenBefore(ssh.FingerprintSHA256(pub), nonce, now) {
			h.log.Warn("replayed nonce", "key", ssh.FingerprintSHA256(pub))
			h.signError(w, nonce, http.StatusUnauthorized, "replayed nonce")
			return
		}
		ent, ok := h.allow().Lookup(pub.Marshal())
		if !ok {
			h.log.Warn("key not allowed", "key", ssh.FingerprintSHA256(pub))
			h.signError(w, nonce, http.StatusForbidden, "public key is not in the server allowlist")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r, &authedRequest{pubWire: pub.Marshal(), admin: ent.Admin, nonce: nonce, body: body})
	}
}

// getIdentity serves the server's public key (SSH wire format),
// self-signed: trust comes from the user confirming the fingerprint at
// `remote add`, not from this signature.
func (h *handler) getIdentity(w http.ResponseWriter, r *http.Request) {
	h.signAndWrite(w, nil, http.StatusOK, "application/octet-stream", h.identity.PublicKey().Marshal())
}
```

Add a temporary stub for the routes Tasks 9-12 implement, in `server/server.go` (they will move to their own files and gain real bodies; the stubs keep this task compiling):

```go
// Stubs replaced in later tasks.
func (h *handler) postMissing(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", nil)
}
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
func (h *handler) postObjectsGet(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
func (h *handler) putRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
func (h *handler) getRefs(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
func (h *handler) deleteRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./server/ -v`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: remote server skeleton with signed-request auth and signed responses"
```

---

### Task 9: server — POST /v1/objects/missing

**Files:**
- Create: `server/objects.go` (move the three object-route stubs here from `server/server.go`)
- Test: `server/objects_test.go`

- [ ] **Step 1: Write the failing test** (`server/objects_test.go`; uses the Task 8 harness)

```go
package server_test

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
)

// storeBlobs writes blobs into ts.store and returns their objects.
func storeBlobs(t *testing.T, ts *testServer, contents ...string) []fstree.Object {
	t.Helper()
	var out []fstree.Object
	for _, c := range contents {
		o, err := fstree.EncodeBlob([]byte(c))
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

func TestMissingReturnsAbsentSubset(t *testing.T) {
	ts := newTestServer(t)
	present := storeBlobs(t, ts, "present one", "present two")
	absent, err := fstree.EncodeBlob([]byte("absent"))
	if err != nil {
		t.Fatal(err)
	}
	req := keylist.Flatten([]key.Key{present[0].Key, absent.Key, present[1].Key})
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", req)
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	got, err := keylist.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != absent.Key {
		t.Fatalf("missing = %v, want [%s]", got, absent.Key)
	}
}

func TestMissingRejectsBadList(t *testing.T) {
	ts := newTestServer(t)
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", make([]byte, 33)); code != 422 {
		t.Fatalf("status = %d, want 422", code)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — the stub returns an empty list for everything, so `TestMissingReturnsAbsentSubset` fails.

- [ ] **Step 3: Implement** — create `server/objects.go`, move the three object stubs out of `server/server.go`, and replace `postMissing`:

```go
package server

import (
	"net/http"

	"github.com/draganm/amber-store/internal/keylist"
)

// postMissing answers the have/want negotiation: of the keys in the request
// body, which does the server not have. Request and response bodies are raw
// concatenated 32-byte keys.
func (h *handler) postMissing(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	keys, err := keylist.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	var missing []byte
	for _, k := range keys {
		has, err := h.store.Has(k)
		if err != nil {
			h.log.Error("missing-check lookup failed", "key", k, "error", err)
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		if !has {
			missing = append(missing, k[:]...)
		}
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", missing)
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./server/ -v`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: server objects/missing answers the have/want negotiation"
```

---

### Task 10: server — POST /v1/objects (verified pack upload)

**Files:**
- Modify: `server/objects.go`
- Test: `server/objects_test.go`

- [ ] **Step 1: Write the failing test** (append)

```go
// packOf serializes objects as one amberpack stream.
func packOf(t *testing.T, objs ...fstree.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestObjectUploadStoresAndDedupes(t *testing.T) {
	ts := newTestServer(t)
	o1, err := fstree.EncodeBlob([]byte("upload one"))
	if err != nil {
		t.Fatal(err)
	}
	o2, err := fstree.EncodeBlob([]byte("upload two"))
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, o1, o2))
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	var stats struct {
		Stored  int `json:"objects_stored"`
		Deduped int `json:"objects_deduped"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 2 {
		t.Fatalf("stored = %d, want 2", stats.Stored)
	}
	if has, _ := ts.store.Has(o1.Key); !has {
		t.Fatal("o1 not in store after upload")
	}
	// re-upload dedupes
	code, body = ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, o1))
	if code != 200 {
		t.Fatalf("re-upload status = %d", code)
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 0 || stats.Deduped != 1 {
		t.Fatalf("re-upload stored/deduped = %d/%d, want 0/1", stats.Stored, stats.Deduped)
	}
}

func TestObjectUploadRejectsHashMismatch(t *testing.T) {
	ts := newTestServer(t)
	good, err := fstree.EncodeBlob([]byte("good payload"))
	if err != nil {
		t.Fatal(err)
	}
	evil := fstree.Object{Key: good.Key, Bytes: []byte("evil payload")}
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, evil)); code != 422 {
		t.Fatalf("status = %d, want 422", code)
	}
	if has, _ := ts.store.Has(good.Key); has {
		t.Fatal("mismatching object was stored")
	}
}
```

(Add `bytes`, `encoding/json` and `github.com/draganm/amber-store/amberpack` to the test imports.)

- [ ] **Step 2: Run it, expect FAIL** — stub returns 501.

- [ ] **Step 3: Implement** — replace the `postObjects` stub in `server/objects.go` (mirrors the daemon's handler, reading from the buffered body):

```go
// uploadResponse mirrors the local daemon's ingest stats shape.
type uploadResponse struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// postObjects decodes an amberpack stream, verifies each object's payload
// against its key, and stores it — the same trust boundary as the local
// daemon: nothing unverified is ever persisted.
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	rd := amberpack.NewReader(bytes.NewReader(a.body))
	seq := func(yield func(diskstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(diskstore.Object{}, err)
				return
			}
			if !yield(diskstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	stats, err := h.store.WriteParallel(seq, diskstore.WriteOpts{Verify: true})
	if err != nil {
		if errors.Is(err, amberpack.ErrMalformed) || errors.Is(err, diskstore.ErrVerify) {
			h.log.Warn("upload rejected", "error", err)
			h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.log.Error("upload failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	body, err := json.Marshal(uploadResponse{
		ObjectsStored:  stats.Stored,
		ObjectsDeduped: stats.Deduped,
		BytesStored:    stats.BytesStored,
	})
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", body)
}
```

(Add `bytes`, `encoding/json`, `errors`, `github.com/draganm/amber-store/amberpack`, `github.com/draganm/amber-store/diskstore` to `server/objects.go` imports.)

- [ ] **Step 4: Run, expect PASS** — `go test ./server/ -v`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: server accepts verified amberpack uploads"
```

---

### Task 11: server — POST /v1/objects/get (trailer-signed stream)

**Files:**
- Modify: `server/objects.go`
- Test: `server/objects_test.go`

- [ ] **Step 1: Write the failing test** (append; this one talks HTTP directly because it must check the trailer)

```go
func TestObjectsGetStreamsPackWithSignedTrailer(t *testing.T) {
	ts := newTestServer(t)
	objs := storeBlobs(t, ts, "stream one", "stream two", "stream three")
	want := []key.Key{objs[0].Key, objs[2].Key}
	body := keylist.Flatten(want)

	req, err := http.NewRequest("POST", ts.srv.URL+"/v1/objects/get", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("nonce-16-bytes!!")
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// trailer signature covers the exact body bytes
	sig := resp.Trailer.Get(httpsig.HeaderSignature)
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nonce, 200,
		httpsig.HashBody(respBody), sig); err != nil {
		t.Fatalf("trailer signature: %v", err)
	}
	// the body is an amberpack of exactly the requested objects
	got := map[key.Key][]byte{}
	for o, err := range amberpack.NewReader(bytes.NewReader(respBody)).All() {
		if err != nil {
			t.Fatal(err)
		}
		got[o.Key] = o.Bytes
	}
	if len(got) != 2 || string(got[objs[0].Key]) != "stream one" || string(got[objs[2].Key]) != "stream three" {
		t.Fatalf("got objects %v", got)
	}
}

func TestObjectsGetAbsentKeyIs404BeforeStreaming(t *testing.T) {
	ts := newTestServer(t)
	absent, err := fstree.EncodeBlob([]byte("never stored"))
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects/get", keylist.Flatten([]key.Key{absent.Key}))
	if code != 404 {
		t.Fatalf("status = %d, want 404", code)
	}
	if !strings.Contains(string(body), absent.Key.String()) {
		t.Fatalf("404 body does not name the missing key: %s", body)
	}
}
```

(Add `strings`, `time`, `io`, `net/http`, `github.com/draganm/amber-store/internal/httpsig` to the test imports as needed.)

- [ ] **Step 2: Run it, expect FAIL** — stub returns 501.

- [ ] **Step 3: Implement** — replace the `postObjectsGet` stub in `server/objects.go`:

```go
// postObjectsGet streams an amberpack of the requested keys. Existence is
// checked for every key before the first body byte (the project-wide
// do-the-work-before-streaming convention), so an absent key is a clean 404
// naming the missing keys. The response signature travels in an HTTP trailer
// because the body hash is only known at the end; a mid-stream failure cuts
// the stream, which clients surface as a missing/invalid trailer signature.
func (h *handler) postObjectsGet(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	keys, err := keylist.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	var missing []string
	for _, k := range keys {
		has, err := h.store.Has(k)
		if err != nil {
			h.log.Error("objects/get lookup failed", "key", k, "error", err)
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		if !has {
			missing = append(missing, k.String())
		}
	}
	if len(missing) > 0 {
		h.signError(w, a.nonce, http.StatusNotFound, "objects not found:\n"+strings.Join(missing, "\n"))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	hasher := blake3.New()
	pw := amberpack.NewWriter(io.MultiWriter(w, hasher))
	for _, k := range keys {
		data, err := h.store.Get(k)
		if err != nil {
			// Bytes are already in flight; cut the stream without a trailer.
			h.log.Error("objects/get stream aborted", "key", k, "error", err)
			return
		}
		if err := pw.Add(fstree.Object{Key: k, Bytes: data}); err != nil {
			h.log.Error("objects/get stream aborted", "key", k, "error", err)
			return
		}
	}
	if err := pw.Close(); err != nil {
		h.log.Error("objects/get stream aborted", "error", err)
		return
	}
	sig, err := httpsig.SignResponse(h.identity, a.nonce, http.StatusOK, hasher.Sum(nil))
	if err != nil {
		h.log.Error("signing objects/get trailer failed", "error", err)
		return
	}
	w.Header().Set(http.TrailerPrefix+httpsig.HeaderSignature, sig)
}
```

(Add `strings`, `io`, `github.com/draganm/amber-store/fstree`, `github.com/draganm/amber-store/internal/httpsig`, `github.com/zeebo/blake3` to imports. `http.TrailerPrefix` lets the handler set the trailer after the body without pre-declaring it.)

- [ ] **Step 4: Run, expect PASS** — `go test ./server/ -v`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: server objects/get streams packs with a trailer signature"
```

---

### Task 12: server — reference routes with ownership and admin

**Files:**
- Create: `server/refs.go` (move the three ref stubs out of `server/server.go`)
- Test: `server/refs_test.go`

- [ ] **Step 1: Write the failing test**

```go
package server_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/reference"
	"golang.org/x/crypto/ssh"
)

// signedRef builds a signed reference record pointing at k, signed by signer.
func signedRef(t *testing.T, name string, k []byte, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name:      name,
		Key:       k,
		User:      "tester@example.com",
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
	b, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRefPutGetRoundTrip(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "ref target")[0]
	owner := testSigner(t)
	rec := signedRef(t, "backups/main", target.Key[:], owner)
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=backups%2Fmain", rec)
	if code != 204 {
		t.Fatalf("put status = %d: %s", code, body)
	}
	code, got := ts.signedDo(t, ts.client, "GET", "/v1/refs?name=backups%2Fmain", nil)
	if code != 200 {
		t.Fatalf("get status = %d", code)
	}
	if string(got) != string(rec) {
		t.Fatal("stored record differs from uploaded record")
	}
}

func TestRefPutRejectsUnsigned(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "unsigned target")[0]
	rec, err := (reference.Reference{
		Name: "unsigned", Key: target.Key[:], User: "u", CreatedAt: time.Now().UnixNano(),
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=unsigned", rec)
	if code != 422 || !strings.Contains(string(body), "signed") {
		t.Fatalf("status = %d body = %s, want 422 mentioning signing", code, body)
	}
}

func TestRefPutRequiresPointedToKey(t *testing.T) {
	ts := newTestServer(t)
	absent, err := fstree.EncodeBlob([]byte("not uploaded"))
	if err != nil {
		t.Fatal(err)
	}
	rec := signedRef(t, "dangling", absent.Key[:], testSigner(t))
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=dangling", rec); code != 404 {
		t.Fatalf("status = %d, want 404", code)
	}
}

func TestRefOwnership(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "owned target")[0]
	owner, intruder := testSigner(t), testSigner(t)
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], owner)); code != 204 {
		t.Fatal("initial put failed")
	}
	// a different signer cannot overwrite, even over an allowed transport key
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], intruder)); code != 403 {
		t.Fatal("intruder overwrite was not rejected with 403")
	}
	// the same signer can update from any transport key
	if code, _ := ts.signedDo(t, ts.admin, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], owner)); code != 204 {
		t.Fatal("owner update over another transport key failed")
	}
	// an admin transport key bypasses ownership
	if code, _ := ts.signedDo(t, ts.admin, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], intruder)); code != 204 {
		t.Fatal("admin override failed")
	}
}

func TestRefDeleteIsAdminOnly(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "delete target")[0]
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=doomed", signedRef(t, "doomed", target.Key[:], testSigner(t))); code != 204 {
		t.Fatal("put failed")
	}
	if code, _ := ts.signedDo(t, ts.client, "DELETE", "/v1/refs?name=doomed", nil); code != 403 {
		t.Fatal("non-admin delete was not rejected")
	}
	if code, _ := ts.signedDo(t, ts.admin, "DELETE", "/v1/refs?name=doomed", nil); code != 204 {
		t.Fatal("admin delete failed")
	}
	if code, _ := ts.signedDo(t, ts.client, "GET", "/v1/refs?name=doomed", nil); code != 404 {
		t.Fatal("deleted ref still present")
	}
}

func TestRefListing(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "list target")[0]
	for _, n := range []string{"b-ref", "a-ref"} {
		if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name="+n, signedRef(t, n, target.Key[:], testSigner(t))); code != 204 {
			t.Fatalf("put %s failed", n)
		}
	}
	code, body := ts.signedDo(t, ts.client, "GET", "/v1/refs", nil)
	if code != 200 {
		t.Fatalf("list status = %d", code)
	}
	var names []string
	dec := json.NewDecoder(strings.NewReader(string(body)))
	for dec.More() {
		var line struct {
			Name   string `json:"name"`
			Signed bool   `json:"signed"`
		}
		if err := dec.Decode(&line); err != nil {
			t.Fatal(err)
		}
		if !line.Signed {
			t.Fatalf("ref %s listed as unsigned", line.Name)
		}
		names = append(names, line.Name)
	}
	if len(names) != 2 || names[0] != "a-ref" || names[1] != "b-ref" {
		t.Fatalf("listing order = %v", names)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — stubs return 501.

- [ ] **Step 3: Implement** `server/refs.go` (move the three stubs here and replace them; the validation order is the spec's §4):

```go
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// refName extracts and validates the ?name= query parameter.
func refName(r *http.Request) (string, error) {
	name := r.URL.Query().Get("name")
	if name == "" {
		return "", errors.New("missing name query parameter")
	}
	if err := reference.ValidateName(name); err != nil {
		return "", err
	}
	return name, nil
}

// putRef stores a reference record after the full validation chain:
// canonical record matching the query name; a verifying signature (the
// record MUST be signed — the signer key owns the name); ownership — an
// existing name may only be overwritten by the same signer key, unless the
// transport key is an admin; and the pointed-to key must exist (push
// objects before the ref).
func (h *handler) putRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rec, err := reference.Decode(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if rec.Name != name {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "record name does not match query name")
		return
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "reference record is not signed; the server only accepts signed references")
		return
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := sshsign.Verify(payload, rec.Signature, rec.PublicKey); err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "reference signature does not verify: "+err.Error())
		return
	}
	existing, err := h.refs.Get(name)
	switch {
	case err == nil:
		old, derr := reference.Decode(existing)
		if derr != nil {
			h.log.Error("stored reference is malformed", "name", name, "error", derr)
			h.signError(w, a.nonce, http.StatusInternalServerError, derr.Error())
			return
		}
		if !bytes.Equal(old.PublicKey, rec.PublicKey) && !a.admin {
			h.signError(w, a.nonce, http.StatusForbidden, "reference is owned by a different signer key")
			return
		}
	case errors.Is(err, refstore.ErrNotFound):
		// fresh name
	default:
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if !has {
		h.signError(w, a.nonce, http.StatusNotFound, "referenced key not found on the server — push objects before the ref")
		return
	}
	if err := h.refs.Put(name, a.body); err != nil {
		h.log.Error("ref put failed", "name", name, "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("reference stored", "name", name, "key", k)
	h.signAndWrite(w, a.nonce, http.StatusNoContent, "", nil)
}

// refLine mirrors the local daemon's listing shape.
type refLine struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signed    bool   `json:"signed"`
}

// getRefs serves a single record (?name=) or the NDJSON listing.
func (h *handler) getRefs(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if r.URL.Query().Get("name") == "" {
		h.listRefs(w, a)
		return
	}
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	data, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		h.signError(w, a.nonce, http.StatusNotFound, "reference not found")
		return
	}
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/cbor", data)
}

// listRefs builds the full NDJSON listing in memory (records are small and
// the body must be hashed for the response signature anyway).
func (h *handler) listRefs(w http.ResponseWriter, a *authedRequest) {
	recs, err := h.refs.All()
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			h.log.Error("stored reference is malformed", "name", rec.Name, "error", err)
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		if err := enc.Encode(refLine{
			Name:      ref.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Signed:    len(ref.Signature) > 0,
		}); err != nil {
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/x-ndjson", buf.Bytes())
}

// deleteRef removes a reference. Ownership lives in the ref signature, but a
// DELETE carries no record to sign, so deletion is restricted to admin
// transport keys (spec §4).
func (h *handler) deleteRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if !a.admin {
		h.signError(w, a.nonce, http.StatusForbidden, "reference deletion requires an admin key")
		return
	}
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	err = h.refs.Delete(name)
	if errors.Is(err, refstore.ErrNotFound) {
		h.signError(w, a.nonce, http.StatusNotFound, "reference not found")
		return
	}
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("reference deleted", "name", name)
	h.signAndWrite(w, a.nonce, http.StatusNoContent, "", nil)
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./server/ -v`.

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: server ref routes enforce signatures, signer ownership and admin delete"
```

---

### Task 13: remoteclient — signed HTTP client core + FetchIdentity

**Files:**
- Create: `remoteclient/remoteclient.go`
- Test: `remoteclient/remoteclient_test.go`

The tests use the real `server` package over `httptest` — full protocol fidelity, no mocks. Build the same `testSigner` helper as in Task 8's `server_test.go` (package-local copy; test helpers are not exported across packages in this repo).

- [ ] **Step 1: Write the failing test**

```go
package remoteclient_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func testSigner(t *testing.T) ssh.Signer {
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

type harness struct {
	srv      *httptest.Server
	store    *diskstore.Store
	refs     *refstore.Store
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, refs: refs, identity: identity, client: client, admin: admin}
}

// rc returns a remoteclient pinned to the harness server's real identity.
func (h *harness) rc(t *testing.T) *remoteclient.Client {
	t.Helper()
	c, err := remoteclient.New(h.srv.URL, h.client, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFetchIdentity(t *testing.T) {
	h := newHarness(t)
	pubWire, err := remoteclient.FetchIdentity(context.Background(), h.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if string(pubWire) != string(h.identity.PublicKey().Marshal()) {
		t.Fatal("fetched identity differs from the server key")
	}
}

func TestPinnedKeyMismatchAborts(t *testing.T) {
	h := newHarness(t)
	wrongPin := testSigner(t).PublicKey().Marshal()
	c, err := remoteclient.New(h.srv.URL, h.client, wrongPin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Missing(context.Background(), nil); err == nil {
		t.Fatal("request against a mismatched pinned key succeeded")
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	if _, err := remoteclient.New("ftp://nope", testSigner(t), []byte("k")); err == nil {
		t.Fatal("want error for non-http(s) URL")
	}
}

func TestStatusErrorsCarryServerMessage(t *testing.T) {
	h := newHarness(t)
	stranger := testSigner(t)
	c, err := remoteclient.New(h.srv.URL, stranger, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Missing(context.Background(), nil)
	var se *remoteclient.StatusError
	if !errorsAs(err, &se) || se.Code != http.StatusForbidden {
		t.Fatalf("err = %v, want StatusError 403", err)
	}
}

func errorsAs(err error, target any) bool {
	type asser interface{ As(any) bool }
	_ = asser(nil)
	return errors.As(err, target)
}
```

(Replace the `errorsAs` indirection with a direct `errors.As` call and an `errors` import — written out here only to flag the import.)

- [ ] **Step 2: Run it, expect FAIL** — package does not exist.

- [ ] **Step 3: Implement** `remoteclient/remoteclient.go`:

```go
// Package remoteclient is the signed HTTP client the local daemon uses to
// talk to a remote amber-store server: every request is signed with the
// daemon's SSH identity (internal/httpsig) and every response must verify
// against the pinned server key recorded at `remote add` — a mismatch aborts
// the operation.
package remoteclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/draganm/amber-store/internal/httpsig"
	"golang.org/x/crypto/ssh"
)

// maxResponse bounds a buffered (non-streaming) response body.
const maxResponse = 256 << 20 // 256 MiB

// StatusError is a non-2xx server response: the HTTP status plus the
// server's (signed) message body.
type StatusError struct {
	Code int
	Msg  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server responded %d: %s", e.Code, strings.TrimSpace(e.Msg))
}

// Client talks to one remote server with one client identity and one pinned
// server key. Safe for concurrent use.
type Client struct {
	hc            *http.Client
	base          string
	signer        ssh.Signer
	serverPubWire []byte
}

// New validates the base URL (http or https) and returns a Client. The
// pinned server key is the SSH wire-format public key confirmed at
// `remote add`.
func New(baseURL string, signer ssh.Signer, serverPubWire []byte) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("remote URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("remote URL %q must be http(s)://host[:port][/path]", baseURL)
	}
	if len(serverPubWire) == 0 {
		return nil, fmt.Errorf("no pinned server key for %s", baseURL)
	}
	return &Client{
		hc:            &http.Client{},
		base:          strings.TrimRight(baseURL, "/"),
		signer:        signer,
		serverPubWire: serverPubWire,
	}, nil
}

func newNonce() ([]byte, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}
	return b, nil
}

// signedRequest builds and signs a request; the returned nonce is what the
// response signature must cover.
func (c *Client) signedRequest(ctx context.Context, method, pathQuery, contentType string, body []byte) (*http.Request, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+pathQuery, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	nonce, err := newNonce()
	if err != nil {
		return nil, nil, err
	}
	if err := httpsig.SignRequest(req, c.signer, time.Now().UnixNano(), nonce, body); err != nil {
		return nil, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nonce, nil
}

// do sends one signed request with fully-buffered request and response
// bodies, verifies the response signature against the pinned key, and maps
// non-2xx statuses to StatusError.
func (c *Client) do(ctx context.Context, method, pathQuery, contentType string, body []byte) (int, []byte, error) {
	req, nonce, err := c.signedRequest(ctx, method, pathQuery, contentType, body)
	if err != nil {
		return 0, nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("contacting remote %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return 0, nil, fmt.Errorf("reading response: %w", err)
	}
	sig := resp.Header.Get(httpsig.HeaderSignature)
	if sig == "" {
		sig = resp.Trailer.Get(httpsig.HeaderSignature)
	}
	if err := httpsig.VerifyResponse(c.serverPubWire, nonce, resp.StatusCode,
		httpsig.HashBody(respBody), sig); err != nil {
		return 0, nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, respBody, &StatusError{Code: resp.StatusCode, Msg: string(respBody)}
	}
	return resp.StatusCode, respBody, nil
}

// FetchIdentity fetches the server's public key (SSH wire format) from the
// unauthenticated identity endpoint. The response is self-signed — the
// returned key verifies its own signature — so trust must come from the user
// confirming the fingerprint.
func FetchIdentity(ctx context.Context, baseURL string) ([]byte, error) {
	base := strings.TrimRight(baseURL, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/identity", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting remote %s: %w", base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading identity: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{Code: resp.StatusCode, Msg: string(body)}
	}
	if _, err := ssh.ParsePublicKey(body); err != nil {
		return nil, fmt.Errorf("server identity is not a valid SSH public key: %w", err)
	}
	if err := httpsig.VerifyResponse(body, nil, resp.StatusCode, httpsig.HashBody(body),
		resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		return nil, fmt.Errorf("server identity self-signature: %w", err)
	}
	return body, nil
}
```

Also add a minimal `Missing` so the tests compile (Task 14 keeps it):

```go
// Missing returns the subset of keys the server does not have.
func (c *Client) Missing(ctx context.Context, keys []key.Key) ([]key.Key, error) {
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects/missing", "application/octet-stream", keylist.Flatten(keys))
	if err != nil {
		return nil, err
	}
	return keylist.Parse(body)
}
```

(Imports: `github.com/draganm/amber-store/internal/keylist`, `github.com/draganm/amber-store/key`.)

- [ ] **Step 4: Run, expect PASS** — `go test ./remoteclient/ -v`.

- [ ] **Step 5: Commit**

```bash
git add remoteclient/
git commit -m "feat: remoteclient signs requests and enforces the pinned server key"
```

---

### Task 14: remoteclient — object and ref methods

**Files:**
- Create: `remoteclient/objects.go`, `remoteclient/refs.go` (move `Missing` to `objects.go`)
- Test: `remoteclient/objects_test.go`, `remoteclient/refs_test.go`

- [ ] **Step 1: Write the failing tests**

`remoteclient/objects_test.go`:

```go
package remoteclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
)

func blobs(t *testing.T, contents ...string) []fstree.Object {
	t.Helper()
	var out []fstree.Object
	for _, c := range contents {
		o, err := fstree.EncodeBlob([]byte(c))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

func TestPushPackAndMissing(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "po one", "po two")
	keys := []key.Key{objs[0].Key, objs[1].Key}

	missing, err := c.Missing(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 2 {
		t.Fatalf("missing before push = %d, want 2", len(missing))
	}
	stats, err := c.PushPack(ctx, objs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 2 {
		t.Fatalf("stored = %d, want 2", stats.Stored)
	}
	missing, err = c.Missing(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing after push = %d, want 0", len(missing))
	}
}

func TestFetchObjects(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "fo one", "fo two")
	if _, err := c.PushPack(ctx, objs); err != nil {
		t.Fatal(err)
	}
	got, err := c.FetchObjects(ctx, []key.Key{objs[0].Key, objs[1].Key})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("fetched %d objects, want 2", len(got))
	}
	byKey := map[key.Key][]byte{}
	for _, o := range got {
		byKey[o.Key] = o.Bytes
	}
	if string(byKey[objs[0].Key]) != "fo one" || string(byKey[objs[1].Key]) != "fo two" {
		t.Fatal("fetched payloads differ")
	}
}

func TestFetchObjectsAbsentIsStatusError(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	absent := blobs(t, "absent")[0]
	_, err := c.FetchObjects(context.Background(), []key.Key{absent.Key})
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("err = %v, want StatusError 404", err)
	}
}
```

`remoteclient/refs_test.go`:

```go
package remoteclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

func signedRecord(t *testing.T, name string, k []byte, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name: name, Key: k, User: "u@example.com",
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
	b, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRefRoundTripAndListing(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	obj := blobs(t, "ref target")[0]
	if _, err := c.PushPack(ctx, []fstree.Object{obj}); err != nil {
		t.Fatal(err)
	}
	rec := signedRecord(t, "main", obj.Key[:], testSigner(t))
	if err := c.PutRef(ctx, "main", rec); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetRef(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(rec) {
		t.Fatal("round-tripped record differs")
	}
	infos, err := c.ListRefs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Name != "main" || !infos[0].Signed {
		t.Fatalf("listing = %+v", infos)
	}
}

func TestGetRefAbsent(t *testing.T) {
	h := newHarness(t)
	_, err := h.rc(t).GetRef(context.Background(), "nope")
	if !errors.Is(err, remoteclient.ErrRefNotFound) {
		t.Fatalf("err = %v, want ErrRefNotFound", err)
	}
}
```

(Add the `fstree` import to `refs_test.go`.)

- [ ] **Step 2: Run them, expect FAIL** — `undefined: PushPack` etc.

- [ ] **Step 3: Implement** `remoteclient/objects.go` (move `Missing` here):

```go
package remoteclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// Missing returns the subset of keys the server does not have.
func (c *Client) Missing(ctx context.Context, keys []key.Key) ([]key.Key, error) {
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects/missing", "application/octet-stream", keylist.Flatten(keys))
	if err != nil {
		return nil, err
	}
	return keylist.Parse(body)
}

// Stats mirrors the server's upload response.
type Stats struct {
	Stored      int   `json:"objects_stored"`
	Deduped     int   `json:"objects_deduped"`
	BytesStored int64 `json:"bytes_stored"`
}

// PushPack uploads objs as one amberpack stream; the server verifies every
// payload against its key before storing.
func (c *Client) PushPack(ctx context.Context, objs []fstree.Object) (Stats, error) {
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			return Stats{}, err
		}
	}
	if err := pw.Close(); err != nil {
		return Stats{}, err
	}
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects", "application/octet-stream", buf.Bytes())
	if err != nil {
		return Stats{}, err
	}
	var s Stats
	if err := json.Unmarshal(body, &s); err != nil {
		return Stats{}, fmt.Errorf("decoding upload response: %w", err)
	}
	return s, nil
}

// FetchObjects downloads the requested objects as a streamed amberpack,
// verifying the trailer signature (over the exact body bytes) against the
// pinned server key before returning. Error responses are buffered and
// header-signed like every other response.
func (c *Client) FetchObjects(ctx context.Context, keys []key.Key) ([]fstree.Object, error) {
	body := keylist.Flatten(keys)
	req, nonce, err := c.signedRequest(ctx, http.MethodPost, "/v1/objects/get", "application/octet-stream", body)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting remote %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if rerr != nil {
			return nil, rerr
		}
		if err := httpsig.VerifyResponse(c.serverPubWire, nonce, resp.StatusCode,
			httpsig.HashBody(respBody), resp.Header.Get(httpsig.HeaderSignature)); err != nil {
			return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
		}
		return nil, &StatusError{Code: resp.StatusCode, Msg: string(respBody)}
	}
	hasher := blake3.New()
	tee := io.TeeReader(resp.Body, hasher)
	var objs []fstree.Object
	for o, err := range amberpack.NewReader(tee).All() {
		if err != nil {
			return nil, fmt.Errorf("reading object stream: %w", err)
		}
		objs = append(objs, o)
	}
	// Drain to EOF so the hash covers the whole body and the trailer arrives.
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return nil, err
	}
	if err := httpsig.VerifyResponse(c.serverPubWire, nonce, http.StatusOK,
		hasher.Sum(nil), resp.Trailer.Get(httpsig.HeaderSignature)); err != nil {
		return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
	}
	return objs, nil
}
```

`remoteclient/refs.go`:

```go
package remoteclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ErrRefNotFound reports an absent reference name on the server.
var ErrRefNotFound = errors.New("reference not found on the remote")

func refsPath(name string) string {
	p := "/v1/refs"
	if name != "" {
		p += "?name=" + url.QueryEscape(name)
	}
	return p
}

// PutRef uploads a canonical (signed) reference record verbatim.
func (c *Client) PutRef(ctx context.Context, name string, record []byte) error {
	_, _, err := c.do(ctx, http.MethodPut, refsPath(name), "application/cbor", record)
	return err
}

// GetRef fetches the raw canonical record stored under name.
func (c *Client) GetRef(ctx context.Context, name string) ([]byte, error) {
	_, body, err := c.do(ctx, http.MethodGet, refsPath(name), "", nil)
	var se *StatusError
	if errors.As(err, &se) && se.Code == http.StatusNotFound {
		return nil, fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if err != nil {
		return nil, err
	}
	return body, nil
}

// RefInfo mirrors one NDJSON line of the server's listing.
type RefInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signed    bool   `json:"signed"`
}

// ListRefs returns every reference on the server, in name order.
func (c *Client) ListRefs(ctx context.Context) ([]RefInfo, error) {
	_, body, err := c.do(ctx, http.MethodGet, refsPath(""), "", nil)
	if err != nil {
		return nil, err
	}
	var infos []RefInfo
	dec := json.NewDecoder(strings.NewReader(string(body)))
	for {
		var info RefInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding ref listing: %w", err)
		}
		infos = append(infos, info)
	}
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./remoteclient/ -v`.

- [ ] **Step 5: Commit**

```bash
git add remoteclient/
git commit -m "feat: remoteclient object and reference methods"
```

---

### Task 15: remotesync — byte-balanced batching

**Files:**
- Create: `remotesync/batch.go`
- Test: `remotesync/batch_test.go`

- [ ] **Step 1: Write the failing test**

```go
package remotesync_test

import (
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remotesync"
)

func TestBatchesBalanceByBytes(t *testing.T) {
	// three 100-byte blobs with a 150-byte target → [2, 1] batches
	var keys []key.Key
	for _, c := range []string{"a", "b", "c"} {
		o, err := fstree.EncodeBlob([]byte(c + string(make([]byte, 99))))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, o.Key)
	}
	batches := remotesync.Batches(keys, 150, remotesync.PullSizer())
	if len(batches) != 3 {
		// each blob is 100 bytes; adding a second to a batch would exceed 150
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	batches = remotesync.Batches(keys, 250, remotesync.PullSizer())
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("got %v-shaped batches, want [2 1]", batches)
	}
}

func TestBatchesOversizedSingleItemGetsOwnBatch(t *testing.T) {
	big, err := fstree.EncodeBlob(make([]byte, 1000))
	if err != nil {
		t.Fatal(err)
	}
	small, err := fstree.EncodeBlob([]byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	batches := remotesync.Batches([]key.Key{big.Key, small.Key}, 100, remotesync.PullSizer())
	if len(batches) != 2 || len(batches[0]) != 1 || len(batches[1]) != 1 {
		t.Fatalf("batches = %v, want one key each", batches)
	}
}

func TestPushSizerUsesActualSizeForNodes(t *testing.T) {
	store, err := diskstore.Open(filepath.Join(t.TempDir(), "s"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blob, err := fstree.EncodeBlob(make([]byte, 4096))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(fn.Key, fn.Bytes); err != nil {
		t.Fatal(err)
	}
	size := remotesync.PushSizer(store)
	if got := size(blob.Key); got != 4096 {
		t.Fatalf("blob size = %d, want 4096 (from the key)", got)
	}
	// FileNode keys encode the logical file length (4096), not the node's
	// encoded size; the push sizer must use the actual stored size.
	if got := size(fn.Key); got != uint64(len(fn.Bytes)) {
		t.Fatalf("node size = %d, want %d (actual encoded size)", got, len(fn.Bytes))
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — package does not exist.

- [ ] **Step 3: Implement** `remotesync/batch.go`:

```go
// Package remotesync implements the push/pull algorithms between a local
// diskstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a two-round-trip have/want push, and a
// round-based BFS pull. See architecture/remote.md.
package remotesync

import (
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
)

// DefaultBatchBytes is the default per-batch payload target.
const DefaultBatchBytes = 8 << 20 // 8 MiB

// maxBatchKeys bounds a batch's key count so pathological trees of tiny
// objects cannot produce arbitrarily large key-list bodies.
const maxBatchKeys = 8192

// nominalNodeSize is the pull-side estimate for tree/file-node objects whose
// encoded size is unknown before fetching (their key lengths are logical,
// not encoded, sizes). It only affects batch balance, never correctness.
const nominalNodeSize = 4096

// SizeOf estimates the transfer size of the object behind a key.
type SizeOf func(k key.Key) uint64

// PushSizer sizes objects for pushing: a Blob's exact payload length comes
// from its key; everything else (FileNode/DirLeaf/DirNode/XattrSet key
// lengths are logical or cumulative) is measured from the local store, with
// a nominal fallback if the read fails (the push itself will surface the
// real error).
func PushSizer(store *diskstore.Store) SizeOf {
	return func(k key.Key) uint64 {
		if k.Type() == key.Blob {
			return k.Length()
		}
		data, err := store.Get(k)
		if err != nil {
			return nominalNodeSize
		}
		return uint64(len(data))
	}
}

// PullSizer sizes objects for pulling, where only the key is known: a Blob's
// exact length from the key, a nominal estimate for everything else.
func PullSizer() SizeOf {
	return func(k key.Key) uint64 {
		if k.Type() == key.Blob {
			return k.Length()
		}
		return nominalNodeSize
	}
}

// Batches bins keys, in order, into batches whose estimated payload sizes
// approach target without exceeding it (a single object larger than target
// gets its own batch).
func Batches(keys []key.Key, target uint64, size SizeOf) [][]key.Key {
	var out [][]key.Key
	var cur []key.Key
	var curBytes uint64
	for _, k := range keys {
		s := size(k)
		if len(cur) > 0 && (curBytes+s > target || len(cur) >= maxBatchKeys) {
			out = append(out, cur)
			cur, curBytes = nil, 0
		}
		cur = append(cur, k)
		curBytes += s
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./remotesync/ -v`.

- [ ] **Step 5: Commit**

```bash
git add remotesync/
git commit -m "feat: remotesync byte-balanced batching from key-encoded sizes"
```

---

### Task 16: remotesync — Push

**Files:**
- Create: `remotesync/push.go`
- Test: `remotesync/push_test.go`

The test harness mirrors Task 13's (`testSigner`, real `server` over `httptest`); create `remotesync/harness_test.go` with the same `testSigner`/`newHarness`/`rc` helpers as `remoteclient/remoteclient_test.go` (copy them; they are package-local test code), plus a tree builder:

```go
// buildTree stores a small two-level tree in store and returns its root:
// DirLeaf{ file "f" → FileNode → [blob1, blob2] }.
func buildTree(t *testing.T, store *diskstore.Store) key.Key {
	t.Helper()
	b1, err := fstree.EncodeBlob([]byte("blob one payload"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fstree.EncodeBlob([]byte("blob two payload"))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: fn.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{b1, b2, fn, leaf} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return leaf.Key
}
```

(Adjust the `fstree.Entry` literal to the actual field names in `fstree` — check `fstree/encode.go` before writing.)

- [ ] **Step 1: Write the failing test** (`remotesync/push_test.go`)

```go
package remotesync_test

import (
	"context"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/remotesync"
)

func TestPushTransfersAllReachableObjects(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t) // diskstore.Open in a fresh TempDir, like the harness store
	root := buildTree(t, local)

	stats, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsTotal != 4 || stats.ObjectsPushed != 4 {
		t.Fatalf("stats = %+v, want 4/4", stats)
	}
	// every reachable object is now on the server
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		t.Fatalf("server-side walk failed: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("server has %d reachable objects, want 4", len(keys))
	}
}

func TestPushIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 0 || stats.BytesPushed != 0 {
		t.Fatalf("re-push transferred %+v, want nothing", stats)
	}
}

func TestPushReportsProgress(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	var last, total int
	_, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{
		Progress: func(done, t int) { last, total = done, t },
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 || total != 4 {
		t.Fatalf("final progress = %d/%d, want 4/4", last, total)
	}
}
```

Add to `remotesync/harness_test.go`:

```go
func newLocalStore(t *testing.T) *diskstore.Store {
	t.Helper()
	s, err := diskstore.Open(filepath.Join(t.TempDir(), "local"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
```

- [ ] **Step 2: Run it, expect FAIL** — `undefined: remotesync.Push`.

- [ ] **Step 3: Implement** `remotesync/push.go`:

```go
package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// DefaultJobs is the default number of parallel transfer workers.
const DefaultJobs = 4

// Opts configures Push and Pull.
type Opts struct {
	BatchBytes uint64 // per-batch payload target; 0 = DefaultBatchBytes
	Jobs       int    // parallel workers; <= 0 = DefaultJobs
	// Progress, when non-nil, is called as work completes. For Push, done
	// counts negotiated keys out of total reachable keys; for Pull, done
	// counts fetched objects and total is 0 (unknown up front).
	Progress func(done, total int)
}

func (o Opts) batchBytes() uint64 {
	if o.BatchBytes == 0 {
		return DefaultBatchBytes
	}
	return o.BatchBytes
}

func (o Opts) jobs() int {
	if o.Jobs <= 0 {
		return DefaultJobs
	}
	return o.Jobs
}

// PushStats summarizes one Push.
type PushStats struct {
	ObjectsTotal  int   // reachable objects under the root
	ObjectsPushed int   // objects the server was missing and received
	BytesPushed   int64 // payload bytes of pushed objects
}

// Push uploads every object reachable from root that the server is missing:
// walk the local reachable set, bin it into byte-balanced batches, and per
// batch (N workers in parallel) negotiate the missing subset and upload an
// amberpack of exactly those objects. Idempotent: a re-run pushes nothing.
func Push(ctx context.Context, store *diskstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PushStats, error) {
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return PushStats{}, fmt.Errorf("walking reachable objects: %w", err)
	}
	batches := Batches(keys, opts.batchBytes(), PushSizer(store))

	var mu sync.Mutex
	stats := PushStats{ObjectsTotal: len(keys)}
	done := 0

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.jobs())
	for _, batch := range batches {
		g.Go(func() error {
			missing, err := rc.Missing(gctx, batch)
			if err != nil {
				return err
			}
			var pushedBytes int64
			if len(missing) > 0 {
				objs := make([]fstree.Object, 0, len(missing))
				for _, k := range missing {
					data, err := store.Get(k)
					if err != nil {
						return fmt.Errorf("reading %s: %w", k, err)
					}
					objs = append(objs, fstree.Object{Key: k, Bytes: data})
					pushedBytes += int64(len(data))
				}
				if _, err := rc.PushPack(gctx, objs); err != nil {
					return err
				}
			}
			mu.Lock()
			stats.ObjectsPushed += len(missing)
			stats.BytesPushed += pushedBytes
			done += len(batch)
			if opts.Progress != nil {
				opts.Progress(done, stats.ObjectsTotal)
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return stats, err
	}
	return stats, nil
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./remotesync/ -v`.

- [ ] **Step 5: Commit**

```bash
git add remotesync/
git commit -m "feat: remotesync.Push negotiates and uploads only missing objects"
```

---

### Task 17: remotesync — Pull

**Files:**
- Create: `remotesync/pull.go`
- Test: `remotesync/pull_test.go`

- [ ] **Step 1: Write the failing test**

```go
package remotesync_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/remotesync"
)

func TestPullFetchesWholeTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store) // tree lives on the SERVER
	local := newLocalStore(t)

	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 4 {
		t.Fatalf("fetched %d objects, want 4", stats.ObjectsFetched)
	}
	// the local store can now serve the full tree
	keys, err := fstree.ReachableKeys(root, local.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("local reachable = %d, want 4", len(keys))
	}
	// payloads survived intact
	for _, k := range keys {
		want, err := h.store.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		got, err := local.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("payload of %s differs", k)
		}
	}
}

func TestPullCompletesPartialLocalTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)

	// pre-seed the local store with the root object only: present-but-
	// incomplete roots must still be descended (spec §3).
	rootData, err := h.store.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(root, rootData); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 3 {
		t.Fatalf("fetched %d objects, want 3 (root was local)", stats.ObjectsFetched)
	}
	if keys, err := fstree.ReachableKeys(root, local.Get); err != nil || len(keys) != 4 {
		t.Fatalf("local tree incomplete: %d keys, err %v", len(keys), err)
	}
}

func TestPullIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 0 {
		t.Fatalf("re-pull fetched %d objects, want 0", stats.ObjectsFetched)
	}
}

func TestPullAbsentRootFails(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	absent, err := fstree.EncodeBlob([]byte("never on server"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remotesync.Pull(context.Background(), local, h.rc(t), absent.Key, remotesync.Opts{}); err == nil {
		t.Fatal("pull of an absent root succeeded")
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — `undefined: remotesync.Pull`.

- [ ] **Step 3: Implement** `remotesync/pull.go`:

```go
package remotesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/sync/errgroup"
)

// PullStats summarizes one Pull.
type PullStats struct {
	ObjectsFetched int   // objects downloaded from the server
	BytesFetched   int64 // their payload bytes
}

// Pull completes the local store under root: a round-based BFS where each
// round classifies the frontier (locally-present keys contribute their
// children without a fetch — so a partial local tree completes correctly —
// and missing keys are binned into byte-balanced batches fetched by N
// parallel workers), stores each fetched object with hash verification, and
// feeds the children of fetched tree/file nodes into the next round.
func Pull(ctx context.Context, store *diskstore.Store, rc *remoteclient.Client, root key.Key, opts Opts) (PullStats, error) {
	seen := map[key.Key]bool{}
	frontier := []key.Key{root}
	var stats PullStats

	for len(frontier) > 0 {
		var want []key.Key
		var next []key.Key
		for _, k := range frontier {
			if seen[k] {
				continue
			}
			seen[k] = true
			has, err := store.Has(k)
			if err != nil {
				return stats, err
			}
			if has {
				if k.Type() == key.Blob || k.Type() == key.XattrSet {
					continue
				}
				data, err := store.Get(k)
				if err != nil {
					return stats, err
				}
				kids, err := fstree.ChildKeys(k, data)
				if err != nil {
					return stats, err
				}
				next = append(next, kids...)
				continue
			}
			want = append(want, k)
		}

		var mu sync.Mutex
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(opts.jobs())
		for _, batch := range Batches(want, opts.batchBytes(), PullSizer()) {
			g.Go(func() error {
				objs, err := rc.FetchObjects(gctx, batch)
				if err != nil {
					return err
				}
				seq := func(yield func(diskstore.Object, error) bool) {
					for _, o := range objs {
						if !yield(diskstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
							return
						}
					}
				}
				// Verify: the store re-hashes every object against its key, so a
				// hostile or corrupted stream can never poison the local store.
				if _, err := store.WriteParallel(seq, diskstore.WriteOpts{Verify: true}); err != nil {
					return fmt.Errorf("storing fetched objects: %w", err)
				}
				var kids []key.Key
				var fetchedBytes int64
				for _, o := range objs {
					fetchedBytes += int64(len(o.Bytes))
					ck, err := fstree.ChildKeys(o.Key, o.Bytes)
					if err != nil {
						return err
					}
					kids = append(kids, ck...)
				}
				mu.Lock()
				next = append(next, kids...)
				stats.ObjectsFetched += len(objs)
				stats.BytesFetched += fetchedBytes
				if opts.Progress != nil {
					opts.Progress(stats.ObjectsFetched, 0)
				}
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return stats, err
		}
		frontier = next
	}
	return stats, nil
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./remotesync/ -v`.

- [ ] **Step 5: Commit**

```bash
git add remotesync/
git commit -m "feat: remotesync.Pull completes local trees via batched BFS"
```

---

### Task 18: daemon — RemoteConfig and remote-registry routes

**Files:**
- Create: `daemon/remotes.go`
- Modify: `daemon/daemon.go` (constructor + route registration)
- Test: `daemon/remotes_test.go`

- [ ] **Step 1: Write the failing test**

```go
package daemon_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func testSignerD(t *testing.T) ssh.Signer {
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

// remoteHarness wires a local daemon (with remotes support) and a remote
// server together.
type remoteHarness struct {
	daemonSrv  *httptest.Server
	serverSrv  *httptest.Server
	store      *diskstore.Store // local daemon's store
	refs       *refstore.Store  // local daemon's refs
	srvStore   *diskstore.Store // remote server's store
	srvRefs    *refstore.Store
	identity   ssh.Signer
	registry   *remotes.Registry
}

func newRemoteHarness(t *testing.T) *remoteHarness {
	t.Helper()
	dir := t.TempDir()
	open := func(name string) (*diskstore.Store, *refstore.Store) {
		s, err := diskstore.Open(filepath.Join(dir, name, "store"), diskstore.WithSync(false))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		r, err := refstore.Open(filepath.Join(dir, name, "refs"), false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		return s, r
	}
	store, refs := open("local")
	srvStore, srvRefs := open("remote")

	identity, client := testSignerD(t), testSignerD(t)
	allow, err := allowlist.Parse(ssh.MarshalAuthorizedKey(client.PublicKey()))
	if err != nil {
		t.Fatal(err)
	}
	serverSrv := httptest.NewServer(server.New(server.Config{
		Store: srvStore, Refs: srvRefs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(serverSrv.Close)

	registry, err := remotes.Open(filepath.Join(dir, "local", "remotes"))
	if err != nil {
		t.Fatal(err)
	}
	h := daemon.NewWithRemotes(store, refs, slog.New(slog.DiscardHandler), &daemon.RemoteConfig{
		Registry:      registry,
		DefaultSigner: client,
	})
	daemonSrv := httptest.NewServer(h)
	t.Cleanup(daemonSrv.Close)
	return &remoteHarness{
		daemonSrv: daemonSrv, serverSrv: serverSrv,
		store: store, refs: refs, srvStore: srvStore, srvRefs: srvRefs,
		identity: identity, registry: registry,
	}
}

func (h *remoteHarness) doReq(t *testing.T, method, pathQuery string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, h.daemonSrv.URL+pathQuery, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// addRemote registers the harness server under name via the daemon routes.
func (h *remoteHarness) addRemote(t *testing.T, name string) {
	t.Helper()
	code, body := h.doReq(t, "POST", "/v1/remotes/preflight?url="+url.QueryEscape(h.serverSrv.URL), nil)
	if code != 200 {
		t.Fatalf("preflight = %d: %s", code, body)
	}
	var pf struct {
		PublicKey   string `json:"public_key"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.Unmarshal(body, &pf); err != nil {
		t.Fatal(err)
	}
	if pf.Fingerprint != ssh.FingerprintSHA256(h.identity.PublicKey()) {
		t.Fatalf("fingerprint = %s", pf.Fingerprint)
	}
	addBody, _ := json.Marshal(map[string]string{"public_key": pf.PublicKey})
	code, body = h.doReq(t, "PUT",
		"/v1/remotes?name="+name+"&url="+url.QueryEscape(h.serverSrv.URL), addBody)
	if code != 204 {
		t.Fatalf("add = %d: %s", code, body)
	}
}

func TestRemoteAddListRemove(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")

	// duplicate add → 409
	pub := base64.StdEncoding.EncodeToString(h.identity.PublicKey().Marshal())
	addBody, _ := json.Marshal(map[string]string{"public_key": pub})
	if code, _ := h.doReq(t, "PUT", "/v1/remotes?name=origin&url="+url.QueryEscape(h.serverSrv.URL), addBody); code != 409 {
		t.Fatalf("duplicate add = %d, want 409", code)
	}

	code, body := h.doReq(t, "GET", "/v1/remotes", nil)
	if code != 200 || !strings.Contains(string(body), `"origin"`) {
		t.Fatalf("list = %d: %s", code, body)
	}

	if code, _ := h.doReq(t, "DELETE", "/v1/remotes?name=origin", nil); code != 204 {
		t.Fatal("remove failed")
	}
	if code, _ := h.doReq(t, "DELETE", "/v1/remotes?name=origin", nil); code != 404 {
		t.Fatal("second remove should 404")
	}
}

func TestPreflightBadURL(t *testing.T) {
	h := newRemoteHarness(t)
	if code, _ := h.doReq(t, "POST", "/v1/remotes/preflight?url="+url.QueryEscape("http://127.0.0.1:1"), nil); code != 502 {
		t.Fatalf("unreachable preflight = %d, want 502", code)
	}
	if code, _ := h.doReq(t, "POST", "/v1/remotes/preflight", nil); code != 422 {
		t.Fatal("missing url should 422")
	}
}

func TestRemoteRoutesAbsentWithoutConfig(t *testing.T) {
	// daemon.New (no RemoteConfig) must not register the remote routes
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer refs.Close()
	srv := httptest.NewServer(daemon.New(store, refs, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/remotes")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — `undefined: daemon.NewWithRemotes`.

- [ ] **Step 3: Implement** — in `daemon/daemon.go`, extend the handler and constructor:

```go
// RemoteConfig wires the daemon's remote-sync support: the persistent remote
// registry and the SSH identities (held in memory, resolved at daemon start)
// used to sign requests to remote servers.
type RemoteConfig struct {
	Registry      *remotes.Registry
	DefaultSigner ssh.Signer            // used for remotes without an override; may be nil
	Signers       map[string]ssh.Signer // per-remote overrides by remote name
}

// signerFor picks the identity for a remote: the per-remote override, else
// the default, else an error telling the operator to configure one.
func (rc *RemoteConfig) signerFor(name string) (ssh.Signer, error) {
	if s, ok := rc.Signers[name]; ok {
		return s, nil
	}
	if rc.DefaultSigner != nil {
		return rc.DefaultSigner, nil
	}
	return nil, errors.New("no remote signing key configured — start the daemon with --remote-key")
}
```

Change the `handler` struct and constructors:

```go
type handler struct {
	store   *diskstore.Store
	refs    *refstore.Store
	log     *slog.Logger
	remotes *RemoteConfig // nil when the daemon runs without remote support
}

// New returns an http.Handler serving the store without remote-sync routes.
func New(store *diskstore.Store, refs *refstore.Store, logger *slog.Logger) http.Handler {
	return NewWithRemotes(store, refs, logger, nil)
}

// NewWithRemotes additionally registers the /v1/remotes and /v1/remote/*
// routes backed by rc.
func NewWithRemotes(store *diskstore.Store, refs *refstore.Store, logger *slog.Logger, rc *RemoteConfig) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{store: store, refs: refs, log: logger, remotes: rc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	mux.HandleFunc("GET /v1/ls/{key}", h.getLs)
	mux.HandleFunc("GET /v1/content-keys/{key}", h.getContentKeys)
	mux.HandleFunc("PUT /v1/refs", h.putRef)
	mux.HandleFunc("GET /v1/refs", h.getRefs)
	mux.HandleFunc("DELETE /v1/refs", h.deleteRef)
	if rc != nil {
		mux.HandleFunc("POST /v1/remotes/preflight", h.remotePreflight)
		mux.HandleFunc("PUT /v1/remotes", h.putRemote)
		mux.HandleFunc("GET /v1/remotes", h.listRemotes)
		mux.HandleFunc("DELETE /v1/remotes", h.deleteRemote)
		mux.HandleFunc("POST /v1/remote/push-objects", h.remotePushObjects)
		mux.HandleFunc("POST /v1/remote/pull-objects", h.remotePullObjects)
		mux.HandleFunc("POST /v1/remote/push-ref", h.remotePushRef)
		mux.HandleFunc("POST /v1/remote/pull-ref", h.remotePullRef)
		mux.HandleFunc("GET /v1/remote/refs", h.remoteLsRefs)
	}
	return logRequests(logger, mux)
}
```

(The four sync routes get stubs returning 501 in this task; Task 19 fills them. Add imports `errors`, `github.com/draganm/amber-store/internal/remotes`, `golang.org/x/crypto/ssh` where needed.)

Create `daemon/remotes.go`:

```go
package daemon

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

// preflightResponse is what `remote add` shows the user for confirmation.
type preflightResponse struct {
	PublicKey   string `json:"public_key"` // SSH wire format, base64
	Fingerprint string `json:"fingerprint"`
	KeyType     string `json:"key_type"`
}

// remotePreflight fetches and self-verifies a prospective remote's identity
// so the CLI can show its fingerprint for trust-on-first-use confirmation.
// Upstream failures are 502: the daemon is fine, the remote is not.
func (h *handler) remotePreflight(w http.ResponseWriter, r *http.Request) {
	u := r.URL.Query().Get("url")
	if u == "" {
		http.Error(w, "missing url query parameter", http.StatusUnprocessableEntity)
		return
	}
	pubWire, err := remoteclient.FetchIdentity(r.Context(), u)
	if err != nil {
		h.log.Warn("remote preflight failed", "url", u, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	pub, err := ssh.ParsePublicKey(pubWire)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(preflightResponse{
		PublicKey:   base64.StdEncoding.EncodeToString(pubWire),
		Fingerprint: ssh.FingerprintSHA256(pub),
		KeyType:     pub.Type(),
	})
}

// putRemote persists a confirmed remote: name and URL from the query, the
// user-confirmed public key in the JSON body.
func (h *handler) putRemote(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	u := r.URL.Query().Get("url")
	if name == "" || u == "" {
		http.Error(w, "missing name or url query parameter", http.StatusUnprocessableEntity)
		return
	}
	var body struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	pubWire, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if _, err := ssh.ParsePublicKey(pubWire); err != nil {
		http.Error(w, "public_key is not a valid SSH key: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	err = h.remotes.Registry.Add(name, remotes.Remote{URL: u, ServerKey: pubWire})
	switch {
	case errors.Is(err, remotes.ErrExists):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	h.log.Info("remote added", "name", name, "url", u)
	w.WriteHeader(http.StatusNoContent)
}

// remoteLine is one NDJSON line of the GET /v1/remotes listing.
type remoteLine struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
}

func (h *handler) listRemotes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, n := range h.remotes.Registry.All() {
		fp := "(invalid key)"
		if pub, err := ssh.ParsePublicKey(n.ServerKey); err == nil {
			fp = ssh.FingerprintSHA256(pub)
		}
		if err := enc.Encode(remoteLine{Name: n.Name, URL: n.URL, Fingerprint: fp}); err != nil {
			h.log.Error("remote list stream aborted", "error", err)
			return
		}
	}
}

func (h *handler) deleteRemote(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return
	}
	err := h.remotes.Registry.Remove(name)
	switch {
	case errors.Is(err, remotes.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		h.log.Info("remote removed", "name", name)
		w.WriteHeader(http.StatusNoContent)
	}
}

// Sync-route stubs, filled in by the next task.
func (h *handler) remotePushObjects(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePullObjects(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePushRef(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePullRef(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remoteLsRefs(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./daemon/ -v` (existing daemon tests must still pass — `New` is unchanged in behavior).

- [ ] **Step 5: Commit**

```bash
git add daemon/
git commit -m "feat: daemon manages registered remotes with TOFU key pinning"
```

---

### Task 19: daemon — sync routes (push/pull objects, push/pull ref, ls-refs)

**Files:**
- Create: `daemon/remotesync.go` (replace the five stubs in `daemon/remotes.go`)
- Test: `daemon/remotesync_test.go`

- [ ] **Step 1: Write the failing test** (uses Task 18's `remoteHarness`; add these helpers to the harness file)

```go
// putLocalRef stores a signed ref in the local daemon's refstore, pointing
// at root, signed by signer. Returns the canonical record bytes.
func (h *remoteHarness) putLocalRef(t *testing.T, name string, root key.Key, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name: name, Key: root[:], User: "u@example.com",
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
	b, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.refs.Put(name, b); err != nil {
		t.Fatal(err)
	}
	return b
}

// buildTree is the same helper as remotesync's (DirLeaf → FileNode → blobs);
// copy it into this package's harness file targeting an arbitrary store.
```

```go
package daemon_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"golang.org/x/crypto/ssh"
)

// lastEvent decodes the final NDJSON line of a sync response.
func lastEvent(t *testing.T, body []byte) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	var ev map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil {
		t.Fatalf("decoding final event %q: %v", lines[len(lines)-1], err)
	}
	return ev
}

func TestPushPullRoundTripThroughDaemon(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "backup", root, signer)

	// push objects
	code, body := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("push-objects = %d: %s", code, body)
	}
	ev := lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("push-objects error: %v", ev["error"])
	}
	if ps, ok := ev["push_stats"].(map[string]any); !ok || ps["objects_pushed"].(float64) != 4 {
		t.Fatalf("final event = %v, want 4 objects pushed", ev)
	}

	// push ref
	if code, body := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=backup", nil); code != 204 {
		t.Fatalf("push-ref = %d: %s", code, body)
	}
	if _, err := h.srvRefs.Get("backup"); err != nil {
		t.Fatalf("ref not on server: %v", err)
	}

	// a second daemon-less store pulls it back: wipe nothing, use a second
	// harness sharing the same remote server
	h2 := newRemoteHarnessWithServer(t, h)
	h2.addRemote(t, "origin")

	code, body = h2.doReq(t, "POST", "/v1/remote/pull-objects?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("pull-objects = %d: %s", code, body)
	}
	ev = lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("pull-objects error: %v", ev["error"])
	}
	if code, body := h2.doReq(t, "POST", "/v1/remote/pull-ref?remote=origin&name=backup", nil); code != 204 {
		t.Fatalf("pull-ref = %d: %s", code, body)
	}
	rec, err := h2.refs.Get("backup")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := reference.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if string(ref.Key) != string(root[:]) {
		t.Fatal("pulled ref points elsewhere")
	}
	// the tree is fully present locally
	if keys, err := fstree.ReachableKeys(root, h2.store.Get); err != nil || len(keys) != 4 {
		t.Fatalf("pulled tree incomplete: %d keys, %v", len(keys), err)
	}
}

func TestPushRefRejectsUnsigned(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	root := buildTree(t, h.store)
	rec, err := (reference.Reference{
		Name: "plain", Key: root[:], User: "u", CreatedAt: time.Now().UnixNano(),
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.refs.Put("plain", rec); err != nil {
		t.Fatal(err)
	}
	code, body := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=plain", nil)
	if code != 422 || !strings.Contains(string(body), "signed") {
		t.Fatalf("push-ref unsigned = %d: %s, want 422 mentioning signing", code, body)
	}
}

func TestPullRefBeforeObjectsConflicts(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "early", root, signer)
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=early", nil); code != 200 {
		t.Fatal("push-objects failed")
	}
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=early", nil); code != 204 {
		t.Fatal("push-ref failed")
	}
	h2 := newRemoteHarnessWithServer(t, h)
	h2.addRemote(t, "origin")
	// pull-ref without pull-objects → 409 with a hint
	code, body := h2.doReq(t, "POST", "/v1/remote/pull-ref?remote=origin&name=early", nil)
	if code != 409 || !strings.Contains(string(body), "pull-objects") {
		t.Fatalf("pull-ref before objects = %d: %s, want 409 hinting pull-objects", code, body)
	}
}

func TestRemoteLsRefs(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "listme", root, signer)
	h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=listme", nil)
	h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=listme", nil)
	code, body := h.doReq(t, "GET", "/v1/remote/refs?remote=origin", nil)
	if code != 200 || !strings.Contains(string(body), `"listme"`) {
		t.Fatalf("ls-refs = %d: %s", code, body)
	}
}

func TestUnknownRemoteAndRef(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=nope&name=x", nil); code != 404 {
		t.Fatal("unknown remote should 404")
	}
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=nope", nil); code != 404 {
		t.Fatal("unknown local ref should 404")
	}
}
```

`newRemoteHarnessWithServer` (add to the harness file): identical to `newRemoteHarness` but reuses `h.serverSrv`, `h.identity` and the same allowlist instead of creating its own server — factor `newRemoteHarness` accordingly (build the server once, then a `newDaemonFor(t, serverSrv, identity, client) *remoteHarness` used by both).

- [ ] **Step 2: Run it, expect FAIL** — stubs return 501.

- [ ] **Step 3: Implement** `daemon/remotesync.go` (replace the five stubs in `daemon/remotes.go`):

```go
package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/remotesync"
)

// remoteFor resolves the ?remote= query (empty selects the sole remote) into
// a connected client. The error string/status pair is written by the caller.
func (h *handler) remoteFor(r *http.Request) (*remoteclient.Client, int, error) {
	name, rem, err := h.remotes.Registry.Resolve(r.URL.Query().Get("remote"))
	if err != nil {
		status := http.StatusUnprocessableEntity
		if errors.Is(err, remotes.ErrNotFound) {
			status = http.StatusNotFound
		}
		return nil, status, err
	}
	signer, err := h.remotes.signerFor(name)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}
	rc, err := remoteclient.New(rem.URL, signer, rem.ServerKey)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}
	return rc, 0, nil
}

// syncOpts reads optional ?jobs= and ?batch_bytes= query parameters.
func syncOpts(r *http.Request) (remotesync.Opts, error) {
	var opts remotesync.Opts
	if v := r.URL.Query().Get("jobs"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return opts, fmt.Errorf("invalid jobs %q", v)
		}
		opts.Jobs = n
	}
	if v := r.URL.Query().Get("batch_bytes"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return opts, fmt.Errorf("invalid batch_bytes %q", v)
		}
		opts.BatchBytes = n
	}
	return opts, nil
}

// syncEvent is one NDJSON line of a push/pull-objects response. Progress
// lines carry done/total; the final line carries stats or an error. The 200
// status is committed before the work runs, so failures surface here.
type syncEvent struct {
	Done      int                   `json:"done,omitempty"`
	Total     int                   `json:"total,omitempty"`
	PushStats *pushStatsLine        `json:"push_stats,omitempty"`
	PullStats *pullStatsLine        `json:"pull_stats,omitempty"`
	Key       string                `json:"key,omitempty"` // pull: the resolved root
	Error     string                `json:"error,omitempty"`
}

type pushStatsLine struct {
	ObjectsTotal  int   `json:"objects_total"`
	ObjectsPushed int   `json:"objects_pushed"`
	BytesPushed   int64 `json:"bytes_pushed"`
}

type pullStatsLine struct {
	ObjectsFetched int   `json:"objects_fetched"`
	BytesFetched   int64 `json:"bytes_fetched"`
}

// eventStream serializes NDJSON events and flushes each one so the CLI can
// render live progress. Safe for concurrent Progress callbacks.
type eventStream struct {
	mu  sync.Mutex
	enc *json.Encoder
	rc  *http.ResponseController
}

func newEventStream(w http.ResponseWriter) *eventStream {
	w.Header().Set("Content-Type", "application/x-ndjson")
	return &eventStream{enc: json.NewEncoder(w), rc: http.NewResponseController(w)}
}

func (s *eventStream) send(ev syncEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(ev)
	_ = s.rc.Flush()
}

// localRefKey resolves a name in the local refstore to its key.
func (h *handler) localRefKey(name string) (key.Key, error) {
	data, err := h.refs.Get(name)
	if err != nil {
		return key.Key{}, err
	}
	rec, err := reference.Decode(data)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(rec.Key)
}

// remotePushObjects pushes everything reachable from the local ref ?name= to
// the remote, streaming NDJSON progress.
func (h *handler) remotePushObjects(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	root, err := h.localRefKey(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Push(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("push-objects failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("push-objects complete", "name", name, "pushed", stats.ObjectsPushed)
	stream.send(syncEvent{PushStats: &pushStatsLine{
		ObjectsTotal:  stats.ObjectsTotal,
		ObjectsPushed: stats.ObjectsPushed,
		BytesPushed:   stats.BytesPushed,
	}})
}

// fetchAndVerifyRemoteRef gets ?name= from the remote and fully validates
// the record: canonical decode, present signature, verifying signature.
func fetchAndVerifyRemoteRef(r *http.Request, rc *remoteclient.Client, name string) ([]byte, reference.Reference, int, error) {
	if name == "" {
		return nil, reference.Reference{}, http.StatusUnprocessableEntity, errors.New("missing name query parameter")
	}
	raw, err := rc.GetRef(r.Context(), name)
	if errors.Is(err, remoteclient.ErrRefNotFound) {
		return nil, reference.Reference{}, http.StatusNotFound, err
	}
	if err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, err
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, fmt.Errorf("remote returned an invalid reference: %w", err)
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		return nil, reference.Reference{}, http.StatusBadGateway, errors.New("remote returned an unsigned reference")
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		return nil, reference.Reference{}, http.StatusInternalServerError, err
	}
	if _, err := sshsign.Verify(payload, rec.Signature, rec.PublicKey); err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, fmt.Errorf("remote reference signature does not verify: %w", err)
	}
	return raw, rec, 0, nil
}

// remotePullObjects resolves ?name= on the REMOTE (so it works before any
// local ref exists) and pulls everything reachable from its key.
func (h *handler) remotePullObjects(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	_, rec, status, err := fetchAndVerifyRemoteRef(r, rc, name)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Pull(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("pull-objects failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("pull-objects complete", "name", name, "fetched", stats.ObjectsFetched)
	stream.send(syncEvent{Key: root.String(), PullStats: &pullStatsLine{
		ObjectsFetched: stats.ObjectsFetched,
		BytesFetched:   stats.BytesFetched,
	}})
}

// remotePushRef uploads the local ref record verbatim. It must be signed —
// the server would reject it anyway, but failing here gives a clearer error
// before any network traffic.
func (h *handler) remotePushRef(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return
	}
	raw, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		http.Error(w, "reference is not signed — configure a signing key (config-user --signing-key) and re-create it", http.StatusUnprocessableEntity)
		return
	}
	if err := rc.PutRef(r.Context(), name, raw); err != nil {
		var se *remoteclient.StatusError
		if errors.As(err, &se) {
			http.Error(w, se.Msg, se.Code)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	h.log.Info("push-ref complete", "name", name)
	w.WriteHeader(http.StatusNoContent)
}

// remotePullRef fetches and verifies the remote record, requires its objects
// to already be local (the same no-dangling rule as the local PUT — run
// pull-objects first), and stores it.
func (h *handler) remotePullRef(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	name := r.URL.Query().Get("name")
	raw, rec, status, err := fetchAndVerifyRemoteRef(r, rc, name)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "referenced objects are not local yet — run pull-objects first", http.StatusConflict)
		return
	}
	if err := h.refs.Put(name, raw); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("pull-ref complete", "name", name, "key", k)
	w.WriteHeader(http.StatusNoContent)
}

// remoteLsRefs proxies the remote's reference listing as NDJSON.
func (h *handler) remoteLsRefs(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	infos, err := rc.ListRefs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, info := range infos {
		if err := enc.Encode(info); err != nil {
			h.log.Error("remote ls-refs stream aborted", "error", err)
			return
		}
	}
}
```

(Delete the five stubs from `daemon/remotes.go`.)

- [ ] **Step 4: Run, expect PASS** — `go test ./daemon/ -v`.

- [ ] **Step 5: Commit**

```bash
git add daemon/
git commit -m "feat: daemon sync routes push/pull objects and refs against remotes"
```

---

### Task 20: client — local-daemon methods for the remote routes

**Files:**
- Create: `client/remote.go`
- Test: covered by the daemon tests above plus the CLI tests below; add compile-level round-trip coverage in `daemon/refs_client_test.go` style if the package has one — otherwise rely on Task 23's CLI tests, which exercise every method end-to-end.

- [ ] **Step 1: Implement** `client/remote.go` (this task is plumbing over already-tested routes; the CLI tests in Task 23 are its failing tests — write this after reading those if executing strictly TDD):

```go
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// PreflightInfo is the daemon's report on a prospective remote's identity.
type PreflightInfo struct {
	PublicKey   []byte // SSH wire format
	Fingerprint string
	KeyType     string
}

// RemotePreflight asks the daemon to fetch a server's identity for
// fingerprint confirmation.
func (c *Client) RemotePreflight(ctx context.Context, remoteURL string) (PreflightInfo, error) {
	u := baseURL + "/v1/remotes/preflight?url=" + url.QueryEscape(remoteURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return PreflightInfo{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return PreflightInfo{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return PreflightInfo{}, fmt.Errorf("preflight failed: %s: %s", resp.Status, msg)
	}
	var body struct {
		PublicKey   string `json:"public_key"`
		Fingerprint string `json:"fingerprint"`
		KeyType     string `json:"key_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return PreflightInfo{}, fmt.Errorf("decoding preflight response: %w", err)
	}
	pubWire, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil {
		return PreflightInfo{}, err
	}
	return PreflightInfo{PublicKey: pubWire, Fingerprint: body.Fingerprint, KeyType: body.KeyType}, nil
}

// RemoteAdd persists a confirmed remote on the daemon.
func (c *Client) RemoteAdd(ctx context.Context, name, remoteURL string, publicKey []byte) error {
	u := baseURL + "/v1/remotes?name=" + url.QueryEscape(name) + "&url=" + url.QueryEscape(remoteURL)
	body, err := json.Marshal(map[string]string{
		"public_key": base64.StdEncoding.EncodeToString(publicKey),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("remote add failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// RemoteRemove unregisters a remote.
func (c *Client) RemoteRemove(ctx context.Context, name string) error {
	u := baseURL + "/v1/remotes?name=" + url.QueryEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("remote rm failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// RemoteInfo is one registered remote.
type RemoteInfo struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
}

// RemoteList returns the registered remotes in name order.
func (c *Client) RemoteList(ctx context.Context) ([]RemoteInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/remotes", nil)
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
		return nil, fmt.Errorf("remote ls failed: %s: %s", resp.Status, msg)
	}
	var infos []RemoteInfo
	dec := json.NewDecoder(resp.Body)
	for {
		var info RemoteInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding remote listing: %w", err)
		}
		infos = append(infos, info)
	}
}

// PushStats / PullStats mirror the daemon's final sync events.
type PushStats struct {
	ObjectsTotal  int   `json:"objects_total"`
	ObjectsPushed int   `json:"objects_pushed"`
	BytesPushed   int64 `json:"bytes_pushed"`
}

type PullStats struct {
	ObjectsFetched int   `json:"objects_fetched"`
	BytesFetched   int64 `json:"bytes_fetched"`
}

// syncEvent mirrors the daemon's NDJSON sync event lines.
type syncEvent struct {
	Done      int        `json:"done"`
	Total     int        `json:"total"`
	PushStats *PushStats `json:"push_stats"`
	PullStats *PullStats `json:"pull_stats"`
	Key       string     `json:"key"`
	Error     string     `json:"error"`
}

// runSync POSTs a sync route and consumes its NDJSON event stream, invoking
// onProgress per progress line and returning the final event.
func (c *Client) runSync(ctx context.Context, pathQuery string, onProgress func(done, total int)) (*syncEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+pathQuery, nil)
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
		return nil, fmt.Errorf("sync failed: %s: %s", resp.Status, msg)
	}
	var last *syncEvent
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var ev syncEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			return nil, fmt.Errorf("decoding sync event: %w", err)
		}
		if ev.Error != "" {
			return nil, fmt.Errorf("sync failed: %s", ev.Error)
		}
		if ev.PushStats != nil || ev.PullStats != nil {
			e := ev
			last = &e
			continue
		}
		if onProgress != nil {
			onProgress(ev.Done, ev.Total)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if last == nil {
		return nil, fmt.Errorf("sync stream ended without a final event (connection cut?)")
	}
	return last, nil
}

// syncQuery builds the common query string for sync routes.
func syncQuery(remote, name string, jobs int, batchBytes uint64) string {
	q := url.Values{}
	if remote != "" {
		q.Set("remote", remote)
	}
	q.Set("name", name)
	if jobs > 0 {
		q.Set("jobs", fmt.Sprint(jobs))
	}
	if batchBytes > 0 {
		q.Set("batch_bytes", fmt.Sprint(batchBytes))
	}
	return "?" + q.Encode()
}

// RemotePushObjects pushes the objects reachable from local ref name.
func (c *Client) RemotePushObjects(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PushStats, error) {
	ev, err := c.runSync(ctx, "/v1/remote/push-objects"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PushStats{}, err
	}
	if ev.PushStats == nil {
		return PushStats{}, fmt.Errorf("sync stream ended without push stats")
	}
	return *ev.PushStats, nil
}

// RemotePullObjects pulls the objects reachable from the SERVER's ref name;
// it returns the resolved root key (hex) alongside the stats.
func (c *Client) RemotePullObjects(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PullStats, string, error) {
	ev, err := c.runSync(ctx, "/v1/remote/pull-objects"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PullStats{}, "", err
	}
	if ev.PullStats == nil {
		return PullStats{}, "", fmt.Errorf("sync stream ended without pull stats")
	}
	return *ev.PullStats, ev.Key, nil
}

// remoteRefAction POSTs push-ref/pull-ref style routes expecting 204.
func (c *Client) remoteRefAction(ctx context.Context, route, remote, name string) error {
	q := url.Values{}
	if remote != "" {
		q.Set("remote", remote)
	}
	q.Set("name", name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+route+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("%s failed: %s: %s", route, resp.Status, msg)
	}
	return nil
}

// RemotePushRef uploads the local ref record to the remote.
func (c *Client) RemotePushRef(ctx context.Context, remote, name string) error {
	return c.remoteRefAction(ctx, "/v1/remote/push-ref", remote, name)
}

// RemotePullRef fetches the remote ref record into the local refstore.
func (c *Client) RemotePullRef(ctx context.Context, remote, name string) error {
	return c.remoteRefAction(ctx, "/v1/remote/pull-ref", remote, name)
}

// RemoteRefInfo mirrors the remote listing lines (same shape as RefInfo).
type RemoteRefInfo = RefInfo

// RemoteLsRefs lists the references on the remote.
func (c *Client) RemoteLsRefs(ctx context.Context, remote string) ([]RemoteRefInfo, error) {
	q := url.Values{}
	if remote != "" {
		q.Set("remote", remote)
	}
	u := baseURL + "/v1/remote/refs"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
		return nil, fmt.Errorf("remote ls-refs failed: %s: %s", resp.Status, msg)
	}
	var infos []RemoteRefInfo
	dec := json.NewDecoder(resp.Body)
	for {
		var info RemoteRefInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding remote ref listing: %w", err)
		}
		infos = append(infos, info)
	}
}
```

- [ ] **Step 2: Build** — `go build ./client/` (its tests come with the CLI tasks).

- [ ] **Step 3: Commit**

```bash
git add client/
git commit -m "feat: client methods for the daemon's remote routes"
```

---

### Task 21: cmd — `serve` command

**Files:**
- Create: `cmd/amber-store/serve.go`
- Modify: `cmd/amber-store/main.go` (register the command)
- Test: `cmd/amber-store/serve_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

// writeServeFixtures writes an identity key and an allowed-keys file.
func writeServeFixtures(t *testing.T) (identityPath, allowedPath string) {
	t.Helper()
	dir := t.TempDir()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	identityPath = filepath.Join(dir, "identity")
	if err := os.WriteFile(identityPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	allowedPath = filepath.Join(dir, "allowed")
	if err := os.WriteFile(allowedPath, ssh.MarshalAuthorizedKey(sshPub), 0o644); err != nil {
		t.Fatal(err)
	}
	return identityPath, allowedPath
}

func TestServeRequiresFlags(t *testing.T) {
	identity, allowed := writeServeFixtures(t)
	cases := [][]string{
		{"amber-store", "serve", "--store", t.TempDir(), "--allowed-keys", allowed},  // no identity
		{"amber-store", "serve", "--store", t.TempDir(), "--identity", identity},     // no allowed-keys
		{"amber-store", "serve", "--identity", identity, "--allowed-keys", allowed},  // no store
	}
	for _, args := range cases {
		if err := newApp().Run(args); err == nil {
			t.Fatalf("serve %v succeeded, want missing-flag error", args[2:])
		}
	}
}

func TestServeRejectsTLSHalfConfig(t *testing.T) {
	identity, allowed := writeServeFixtures(t)
	err := newApp().Run([]string{
		"amber-store", "serve", "--store", t.TempDir(),
		"--identity", identity, "--allowed-keys", allowed,
		"--tls-cert", "/nonexistent/cert.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("err = %v, want tls flag pairing error", err)
	}
}
```

(Runtime behavior — listening, signed responses, SIGHUP reload — is covered by the package-level server tests and the integration task; these CLI tests pin flag validation. A full serve-then-request test would need a probe for the bound port; defer that to Task 24's note.)

- [ ] **Step 2: Run it, expect FAIL** — unknown command `serve`.

- [ ] **Step 3: Implement** `cmd/amber-store/serve.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh"
)

type serveConfig struct {
	store       string
	listen      string
	allowedKeys string
	identity    string
	tlsCert     string
	tlsKey      string
	authWindow  time.Duration
	sync        bool
	logLevel    string
	logFormat   string
}

func serveCommand() *cli.Command {
	cfg := &serveConfig{}
	return &cli.Command{
		Name:  "serve",
		Usage: "run the remote server other amber daemons push to and pull from",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "store",
				Aliases:     []string{"s"},
				Usage:       "diskstore directory (created if missing)",
				Required:    true,
				Destination: &cfg.store,
			},
			&cli.StringFlag{
				Name:        "listen",
				Value:       ":8590",
				Usage:       "TCP listen address",
				Destination: &cfg.listen,
			},
			&cli.StringFlag{
				Name:        "allowed-keys",
				Usage:       "authorized_keys-format file of allowed client keys ('admin' option marks ops keys); reloaded on SIGHUP",
				Required:    true,
				Destination: &cfg.allowedKeys,
			},
			&cli.StringFlag{
				Name:        "identity",
				Usage:       "server SSH identity: a private-key file, or a .pub resolved through the ssh-agent",
				Required:    true,
				Destination: &cfg.identity,
			},
			&cli.StringFlag{
				Name:        "tls-cert",
				Usage:       "TLS certificate file (omit both tls flags to serve plain HTTP behind a TLS-terminating proxy)",
				Destination: &cfg.tlsCert,
			},
			&cli.StringFlag{
				Name:        "tls-key",
				Usage:       "TLS private-key file",
				Destination: &cfg.tlsKey,
			},
			&cli.DurationFlag{
				Name:        "auth-window",
				Value:       5 * time.Minute,
				Usage:       "request-timestamp validity window (each side of now)",
				Destination: &cfg.authWindow,
			},
			&cli.BoolFlag{
				Name:        "sync",
				Value:       true,
				Usage:       "fsync writes for crash durability",
				Destination: &cfg.sync,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Value:       "info",
				Usage:       "log level: debug, info, warn or error",
				Destination: &cfg.logLevel,
			},
			&cli.StringFlag{
				Name:        "log-format",
				Value:       "text",
				Usage:       "log format: text or json",
				Destination: &cfg.logFormat,
			},
		},
		Action: func(c *cli.Context) error { return runServe(c, cfg) },
	}
}

// remoteIdentitySigner loads an SSH identity under the remote-protocol key
// rule: unencrypted private-key files load directly, .pub files resolve via
// the ssh-agent, and passphrase-protected files are rejected — only agent
// signing is allowed for protected keys, because nothing may block at
// request time.
func remoteIdentitySigner(path string) (ssh.Signer, func(), error) {
	return sshsign.Signer(path, func(p string) ([]byte, error) {
		return nil, fmt.Errorf("key %s is passphrase-protected; load it into the ssh-agent and configure the .pub path instead", p)
	})
}

func runServe(c *cli.Context, cfg *serveConfig) error {
	if (cfg.tlsCert == "") != (cfg.tlsKey == "") {
		return errors.New("--tls-cert and --tls-key must be set together")
	}
	logger, err := buildLogger(cfg.logLevel, cfg.logFormat)
	if err != nil {
		return err
	}
	identity, closeIdentity, err := remoteIdentitySigner(cfg.identity)
	if err != nil {
		return err
	}
	defer closeIdentity()

	initial, err := allowlist.Load(cfg.allowedKeys)
	if err != nil {
		return err
	}
	var allow atomic.Pointer[allowlist.List]
	allow.Store(initial)
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for range hup {
			l, err := allowlist.Load(cfg.allowedKeys)
			if err != nil {
				logger.Error("allowlist reload failed; keeping the previous list", "error", err)
				continue
			}
			allow.Store(l)
			logger.Info("allowlist reloaded", "path", cfg.allowedKeys)
		}
	}()

	store, err := diskstore.Open(cfg.store, diskstore.WithSync(cfg.sync))
	if err != nil {
		return err
	}
	defer store.Close()
	refs, err := refstore.Open(filepath.Join(cfg.store, "refs"), cfg.sync)
	if err != nil {
		return err
	}
	defer refs.Close()

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.listen, err)
	}

	srv := &http.Server{
		Handler: server.New(server.Config{
			Store:    store,
			Refs:     refs,
			Allow:    func() *allowlist.List { return allow.Load() },
			Identity: identity,
			Log:      logger,
			Window:   cfg.authWindow,
		}),
	}

	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = srv.Shutdown(context.Background())
		close(shutdownDone)
	}()

	logger.Info("serve listening",
		"addr", ln.Addr().String(),
		"store", cfg.store,
		"tls", cfg.tlsCert != "",
		"identity", ssh.FingerprintSHA256(identity.PublicKey()),
	)
	if cfg.tlsCert != "" {
		err = srv.ServeTLS(ln, cfg.tlsCert, cfg.tlsKey)
	} else {
		err = srv.Serve(ln)
	}
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
		return nil
	}
	return err
}
```

Register in `cmd/amber-store/main.go`'s command list: add `serveCommand(),` after `daemonCommand(),`.

- [ ] **Step 4: Run, expect PASS** — `go test ./cmd/amber-store/ -run TestServe -v`.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/
git commit -m "feat: serve command runs the remote server with TLS and SIGHUP allowlist reload"
```

---

### Task 22: cmd — daemon `--remote-key` flags and registry wiring

**Files:**
- Modify: `cmd/amber-store/daemon.go`
- Test: `cmd/amber-store/daemon_remote_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestParseRemoteKeyFlags(t *testing.T) {
	keyPath, _ := writeSigningKey(t) // existing helper in ref_test.go
	def, overrides, err := parseRemoteKeys([]string{keyPath, "origin=" + keyPath})
	if err != nil {
		t.Fatal(err)
	}
	if def == nil {
		t.Fatal("default signer not loaded")
	}
	if _, ok := overrides["origin"]; !ok {
		t.Fatal("origin override not loaded")
	}
	var _ ssh.Signer = def
}

func TestParseRemoteKeysRejectsTwoDefaults(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	_, _, err := parseRemoteKeys([]string{keyPath, keyPath})
	if err == nil || !strings.Contains(err.Error(), "default") {
		t.Fatalf("err = %v, want duplicate-default error", err)
	}
}

func TestParseRemoteKeysRejectsBadPath(t *testing.T) {
	if _, _, err := parseRemoteKeys([]string{"/nonexistent/key"}); err == nil {
		t.Fatal("want error for unreadable key")
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** — `undefined: parseRemoteKeys`.

- [ ] **Step 3: Implement** — in `cmd/amber-store/daemon.go`:

Add to `daemonConfig`:

```go
	remoteKeys cli.StringSlice
```

Add to the daemon command's `Flags`:

```go
			&cli.StringSliceFlag{
				Name: "remote-key",
				Usage: "SSH identity for remote sync: PATH (default for all remotes) " +
					"or NAME=PATH (per-remote override); repeatable. Passphrase-protected " +
					"keys must be used via the ssh-agent (.pub path).",
				Destination: &cfg.remoteKeys,
			},
```

Add the parser (new function in `daemon.go`; `remoteIdentitySigner` comes from Task 21's `serve.go`, same package):

```go
// parseRemoteKeys resolves --remote-key flags into signers held for the
// daemon's lifetime. A bare PATH is the default identity (at most one);
// NAME=PATH overrides per remote. Closers are tied to the process — agent
// connections stay open as long as the daemon runs.
func parseRemoteKeys(entries []string) (ssh.Signer, map[string]ssh.Signer, error) {
	var def ssh.Signer
	overrides := map[string]ssh.Signer{}
	for _, e := range entries {
		name, path, isOverride := strings.Cut(e, "=")
		if !isOverride {
			path, name = e, ""
		}
		signer, _, err := remoteIdentitySigner(path)
		if err != nil {
			return nil, nil, fmt.Errorf("--remote-key %s: %w", e, err)
		}
		if name == "" {
			if def != nil {
				return nil, nil, errors.New("--remote-key: only one default (NAME-less) key is allowed")
			}
			def = signer
			continue
		}
		if _, dup := overrides[name]; dup {
			return nil, nil, fmt.Errorf("--remote-key: duplicate override for remote %q", name)
		}
		overrides[name] = signer
	}
	return def, overrides, nil
}
```

Wire into `runDaemon`, replacing the `daemon.New(...)` call:

```go
	defSigner, overrides, err := parseRemoteKeys(cfg.remoteKeys.Value())
	if err != nil {
		return err
	}
	registry, err := remotes.Open(filepath.Join(cfg.store, "remotes"))
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: daemon.NewWithRemotes(store, refs, logger, &daemon.RemoteConfig{
			Registry:      registry,
			DefaultSigner: defSigner,
			Signers:       overrides,
		}),
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
```

(Add imports: `strings`, `github.com/draganm/amber-store/internal/remotes`, `golang.org/x/crypto/ssh`. The remotes registry is always opened — `remote add` must work even before any `--remote-key` is configured; sync commands fail later with the no-key message.)

- [ ] **Step 4: Run, expect PASS** — `go test ./cmd/amber-store/ -run TestParseRemoteKeys -v`, then the whole package: `go test ./cmd/amber-store/`.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/
git commit -m "feat: daemon --remote-key flags configure remote-sync identities"
```

---

### Task 23: cmd — `remote` command group

**Files:**
- Create: `cmd/amber-store/remote.go`
- Modify: `cmd/amber-store/main.go` (register `remoteCommand()`)
- Test: `cmd/amber-store/remote_test.go`

- [ ] **Step 1: Write the failing test**

The harness: `startDaemon(t)` (existing helper) starts the local daemon; a remote server runs in-process via `server.New` over `httptest` with the daemon's default `--remote-key` identity allowed. Check how `startDaemon` passes flags — it must be extended (or a sibling `startDaemonWithArgs(t, extra ...string)` added) so tests can pass `--remote-key`.

```go
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

// startRemoteServer runs an in-process remote server allowing clientPub.
func startRemoteServer(t *testing.T, clientPub ssh.PublicKey) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	allow, err := allowlist.Parse(ssh.MarshalAuthorizedKey(clientPub))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return srv
}

// runApp runs the CLI with stdin/stdout wired, returning stdout.
func runApp(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	app := newApp()
	var out bytes.Buffer
	app.Reader = strings.NewReader(stdin)
	app.Writer = &out
	err := app.Run(append([]string{"amber-store"}, args...))
	return out.String(), err
}

func TestRemoteAddConfirmAndLs(t *testing.T) {
	keyPath, pub := writeSigningKey(t)
	sock := startDaemonWithArgs(t, "--remote-key", keyPath)
	srv := startRemoteServer(t, pub)

	// declining the fingerprint aborts
	if _, err := runApp(t, "n\n", "remote", "add", "--socket", sock, "origin", srv.URL); err == nil {
		t.Fatal("declined add succeeded")
	}
	out, err := runApp(t, "", "remote", "ls", "--socket", sock)
	if err != nil || strings.Contains(out, "origin") {
		t.Fatalf("remote listed after declined add: %q, %v", out, err)
	}

	// confirming records it
	out, err = runApp(t, "y\n", "remote", "add", "--socket", sock, "origin", srv.URL)
	if err != nil {
		t.Fatalf("remote add: %v", err)
	}
	if !strings.Contains(out, "SHA256:") {
		t.Fatalf("add output shows no fingerprint: %q", out)
	}
	out, err = runApp(t, "", "remote", "ls", "--socket", sock)
	if err != nil || !strings.Contains(out, "origin") || !strings.Contains(out, srv.URL) {
		t.Fatalf("remote ls = %q, %v", out, err)
	}

	// --yes skips the prompt
	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "--yes", "second", srv.URL); err != nil {
		t.Fatalf("add --yes: %v", err)
	}

	// rm removes
	if _, err := runApp(t, "", "remote", "rm", "--socket", sock, "second"); err != nil {
		t.Fatalf("remote rm: %v", err)
	}
}

func TestRemotePushPullCycle(t *testing.T) {
	keyPath, pub := writeSigningKey(t)
	configureTestUserWithKey(t, keyPath) // existing helper: user config + signing key
	sock := startDaemonWithArgs(t, "--remote-key", keyPath)
	srv := startRemoteServer(t, pub)
	if _, err := runApp(t, "y\n", "remote", "add", "--socket", sock, "origin", srv.URL); err != nil {
		t.Fatal(err)
	}

	// ingest a directory and push it
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("push me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, "", "ingest", "--socket", sock, "snap", dir); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	out, err := runApp(t, "", "remote", "push-objects", "--socket", sock, "origin", "snap")
	if err != nil {
		t.Fatalf("push-objects: %v", err)
	}
	if !strings.Contains(out, "pushed") {
		t.Fatalf("push-objects output: %q", out)
	}
	if _, err := runApp(t, "", "remote", "push-ref", "--socket", sock, "origin", "snap"); err != nil {
		t.Fatalf("push-ref: %v", err)
	}
	out, err = runApp(t, "", "remote", "ls-refs", "--socket", sock, "origin")
	if err != nil || !strings.Contains(out, "snap") {
		t.Fatalf("ls-refs = %q, %v", out, err)
	}

	// pull into a second daemon
	sock2 := startDaemonWithArgs(t, "--remote-key", keyPath)
	if _, err := runApp(t, "y\n", "remote", "add", "--socket", sock2, "origin", srv.URL); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, "", "remote", "pull-objects", "--socket", sock2, "origin", "snap"); err != nil {
		t.Fatalf("pull-objects: %v", err)
	}
	if _, err := runApp(t, "", "remote", "pull-ref", "--socket", sock2, "origin", "snap"); err != nil {
		t.Fatalf("pull-ref: %v", err)
	}
	// the content round-trips
	restoreDir := t.TempDir()
	if _, err := runApp(t, "", "restore", "--socket", sock2, "ref:snap", restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restoreDir, "f.txt"))
	if err != nil || string(got) != "push me" {
		t.Fatalf("restored content = %q, %v", got, err)
	}
}

func TestRemoteArgParsing(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	sock := startDaemonWithArgs(t, "--remote-key", keyPath)
	if _, err := runApp(t, "", "remote", "push-objects", "--socket", sock); err == nil {
		t.Fatal("no-arg push-objects succeeded")
	}
	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "only-name"); err == nil {
		t.Fatal("add without URL succeeded")
	}
}
```

(Adapt the `ingest`/`restore` invocations and `configureTestUserWithKey` to the existing test helpers' exact signatures in `cmd/amber-store/*_test.go` — read `e2e_test.go`, `ingest_test.go` and `ref_test.go` first; the daemon start helper may already accept extra args, in which case `startDaemonWithArgs` is just `startDaemon`.)

- [ ] **Step 2: Run it, expect FAIL** — unknown command `remote`.

- [ ] **Step 3: Implement** `cmd/amber-store/remote.go`:

```go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

// socketFlag is the shared --socket flag every remote subcommand takes.
func socketFlag(dest *string) cli.Flag {
	return &cli.StringFlag{
		Name:        "socket",
		Usage:       "unix socket path (default: $AMBER_STORE_SOCKET or a per-user path)",
		Destination: dest,
	}
}

func remoteCommand() *cli.Command {
	return &cli.Command{
		Name:  "remote",
		Usage: "manage remote servers and push/pull objects and references",
		Subcommands: []*cli.Command{
			remoteAddCommand(),
			remoteRmCommand(),
			remoteLsCommand(),
			remoteSyncCommand("push-objects", "push objects reachable from local ref NAME to the remote"),
			remoteSyncCommand("pull-objects", "pull objects reachable from the remote's ref NAME"),
			remoteRefCommand("push-ref", "upload the local reference record NAME to the remote"),
			remoteRefCommand("pull-ref", "fetch the remote reference record NAME into the local store"),
			remoteLsRefsCommand(),
		},
	}
}

// remoteAndName parses the [REMOTE] NAME argument pair; one argument means
// "the sole registered remote".
func remoteAndName(c *cli.Context, cmd string) (remote, name string, err error) {
	switch c.NArg() {
	case 1:
		return "", c.Args().Get(0), nil
	case 2:
		return c.Args().Get(0), c.Args().Get(1), nil
	default:
		return "", "", fmt.Errorf("%s requires [REMOTE] NAME arguments, got %d", cmd, c.NArg())
	}
}

func remoteAddCommand() *cli.Command {
	var socket string
	var yes bool
	return &cli.Command{
		Name:      "add",
		Usage:     "register a remote: fetch its key, confirm the fingerprint, persist",
		ArgsUsage: "NAME URL",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.BoolFlag{
				Name:        "yes",
				Usage:       "trust the server key without prompting (scripting)",
				Destination: &yes,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return fmt.Errorf("remote add requires NAME URL arguments, got %d", c.NArg())
			}
			name, url := c.Args().Get(0), c.Args().Get(1)
			cl := client.New(socketpath.Resolve(socket))
			info, err := cl.RemotePreflight(c.Context, url)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%s key fingerprint: %s (%s)\n", url, info.Fingerprint, info.KeyType)
			if !yes {
				fmt.Fprint(c.App.Writer, "Trust this key and register the remote? [y/N] ")
				reader := c.App.Reader
				if reader == nil {
					reader = os.Stdin
				}
				answer, err := bufio.NewReader(reader).ReadString('\n')
				if err != nil {
					return fmt.Errorf("reading confirmation: %w", err)
				}
				if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
					return fmt.Errorf("remote add aborted: server key not confirmed")
				}
			}
			if err := cl.RemoteAdd(c.Context, name, url, info.PublicKey); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "remote %s added (%s)\n", name, url)
			return nil
		},
	}
}

func remoteRmCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "rm",
		Usage:     "unregister a remote",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("remote rm requires exactly one NAME argument, got %d", c.NArg())
			}
			return client.New(socketpath.Resolve(socket)).RemoteRemove(c.Context, c.Args().First())
		},
	}
}

func remoteLsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:  "ls",
		Usage: "list registered remotes",
		Flags: []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			infos, err := client.New(socketpath.Resolve(socket)).RemoteList(c.Context)
			if err != nil {
				return err
			}
			for _, info := range infos {
				fmt.Fprintf(c.App.Writer, "%s\t%s\t%s\n", info.Name, info.URL, info.Fingerprint)
			}
			return nil
		},
	}
}

// remoteSyncCommand builds push-objects / pull-objects (same flags and arg
// shape; the route differs).
func remoteSyncCommand(name, usage string) *cli.Command {
	var socket string
	var jobs int
	var batchBytes uint64
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "[REMOTE] NAME",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.IntFlag{
				Name:        "jobs",
				Aliases:     []string{"j"},
				Value:       4,
				Usage:       "parallel transfer workers",
				Destination: &jobs,
			},
			&cli.Uint64Flag{
				Name:        "batch-bytes",
				Value:       8 << 20,
				Usage:       "per-batch payload target in bytes",
				Destination: &batchBytes,
			},
		},
		Action: func(c *cli.Context) error {
			remote, refName, err := remoteAndName(c, "remote "+name)
			if err != nil {
				return err
			}
			cl := client.New(socketpath.Resolve(socket))
			progress := func(done, total int) {
				if total > 0 {
					fmt.Fprintf(os.Stderr, "\r%s: %d/%d objects", name, done, total)
				} else {
					fmt.Fprintf(os.Stderr, "\r%s: %d objects", name, done)
				}
			}
			defer fmt.Fprintln(os.Stderr)
			if name == "push-objects" {
				stats, err := cl.RemotePushObjects(c.Context, remote, refName, jobs, batchBytes, progress)
				if err != nil {
					return err
				}
				fmt.Fprintf(c.App.Writer, "pushed %d objects (%d bytes), %d already present\n",
					stats.ObjectsPushed, stats.BytesPushed, stats.ObjectsTotal-stats.ObjectsPushed)
				return nil
			}
			stats, rootKey, err := cl.RemotePullObjects(c.Context, remote, refName, jobs, batchBytes, progress)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "pulled %d objects (%d bytes), root %s\n",
				stats.ObjectsFetched, stats.BytesFetched, rootKey)
			return nil
		},
	}
}

// remoteRefCommand builds push-ref / pull-ref.
func remoteRefCommand(name, usage string) *cli.Command {
	var socket string
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "[REMOTE] NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			remote, refName, err := remoteAndName(c, "remote "+name)
			if err != nil {
				return err
			}
			cl := client.New(socketpath.Resolve(socket))
			if name == "push-ref" {
				err = cl.RemotePushRef(c.Context, remote, refName)
			} else {
				err = cl.RemotePullRef(c.Context, remote, refName)
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%s %s: ok\n", name, refName)
			return nil
		},
	}
}

func remoteLsRefsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "ls-refs",
		Usage:     "list references on the remote",
		ArgsUsage: "[REMOTE]",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() > 1 {
				return fmt.Errorf("remote ls-refs takes at most one REMOTE argument, got %d", c.NArg())
			}
			infos, err := client.New(socketpath.Resolve(socket)).RemoteLsRefs(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			for _, info := range infos {
				fmt.Fprintf(c.App.Writer, "%s\t%s\t%s\t%s\n", info.Name, info.Key, info.User, info.CreatedAt)
			}
			return nil
		},
	}
}
```

Register in `main.go`: add `remoteCommand(),` to the command list. Check how the existing commands declare `--socket` — if there is a shared helper already, use it instead of `socketFlag`.

- [ ] **Step 4: Run, expect PASS** — `go test ./cmd/amber-store/ -run TestRemote -v`, then `go test ./cmd/amber-store/`.

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/
git commit -m "feat: remote CLI group: add/rm/ls, push/pull objects and refs, ls-refs"
```

---

### Task 24: full verification pass

The end-to-end behavior (two daemons + one server, CLI-level push/pull, restore round-trip) is already covered by Task 23's `TestRemotePushPullCycle`. This task is the whole-tree gate.

- [ ] **Step 1: Run everything**

```bash
go build ./... && go vet ./... && go test ./...
```

Expected: all packages pass. Fix anything that surfaces (most likely import drift between tasks or a test-helper name mismatch) before proceeding.

- [ ] **Step 2: Race check the concurrent paths**

```bash
go test -race ./remotesync/ ./server/ ./remoteclient/ ./daemon/
```

Expected: PASS with no race reports (`remotesync` workers and the daemon's `eventStream` are the concurrency hot spots).

- [ ] **Step 3: Remove any leftover built binaries** — `git status` must show no `amber-store` binary or other build artifacts; delete them if present (`rm -f amber-store`).

- [ ] **Step 4: Commit** (only if fixes were needed)

```bash
git add -A
git commit -m "test: fix integration fallout from the remote-server feature"
```

---

### Task 25: documentation

**Files:**
- Create: `architecture/remote.md`
- Modify: `architecture/daemon.md` (new local routes + commands in the tables)
- Modify: `architecture/references.md` (server-side ref semantics pointer)
- Modify: `README.md` (if it lists commands — check; add `serve` and `remote`)

- [ ] **Step 1: Write `architecture/remote.md`** — cover, in this order (follow the existing docs' voice: short sections, tables for routes, the *why* next to the *what*):

1. **The shape** — `amber-store serve` as a TCP/HTTP(S) sibling of the local daemon owning its own diskstore + refs DB; local daemons are its only clients; per-single-reference push/pull composed as objects-then-ref.
2. **Identity & trust** — both sides hold SSH keys; client keys must appear in the server's `--allowed-keys` (authorized_keys format, `admin` option, SIGHUP reload); the server's key is pinned client-side at `remote add` after fingerprint confirmation (TOFU); passphrase-protected keys only via ssh-agent.
3. **Request/response signing** — the four `Amber-*` headers; canonical-CBOR payloads `{method, path+query, timestamp, nonce, blake3(body)}` and `{nonce, status, blake3(body)}`; SSHSIG namespace `amber-store-http`; blake3 carries the bulk so signing cost is constant; ±5 min timestamp window plus nonce replay cache; trailer signatures on streamed responses; TLS optional and orthogonal.
4. **Wire protocol** — the seven-route table from the spec (§3), key-list encoding, the 64 MiB body cap.
5. **Sync algorithms** — byte-balanced batching from key-encoded sizes (blob exact, node actual-on-push/nominal-on-pull), two-round-trip push batches, round-based BFS pull that completes partial local trees, idempotent re-runs.
6. **Reference semantics** — the five-step PUT validation, signer-key ownership, admin override, admin-only DELETE, no-dangling rule on both ends and the resulting command order.
7. **Status mapping** — the error table from the spec (§4).

Source the content from `docs/superpowers/specs/2026-06-10-remote-server-design.md` — the spec is the contract; the architecture doc is its polished, present-tense description.

- [ ] **Step 2: Update `architecture/daemon.md`** — add to the command table:

```markdown
| `serve`                 | —                                                               | remote server: owns its own store, signed HTTP(S) |
| `remote add/rm/ls`      | fingerprint confirmation, render listing                        | fetch+pin server identity, registry CRUD   |
| `remote push-objects`   | render progress + stats                                         | walk, negotiate, upload missing batches    |
| `remote pull-objects`   | render progress + stats                                         | resolve remote ref, batched BFS download   |
| `remote push/pull-ref`  | render outcome                                                  | validate + transfer the reference record   |
| `remote ls-refs`        | render listing                                                  | proxy the remote's reference listing       |
```

and to the route table:

```markdown
| `POST /v1/remotes/preflight?url=`  | → server identity + fingerprint JSON              |
| `PUT /v1/remotes?name=&url=`       | confirmed public key JSON in → 204                |
| `GET /v1/remotes`                  | NDJSON, one remote per line, name order           |
| `DELETE /v1/remotes?name=`         | 204                                               |
| `POST /v1/remote/push-objects`     | `?remote=&name=` → NDJSON progress + final stats  |
| `POST /v1/remote/pull-objects`     | `?remote=&name=` → NDJSON progress + final stats  |
| `POST /v1/remote/push-ref`         | `?remote=&name=` → 204                            |
| `POST /v1/remote/pull-ref`         | `?remote=&name=` → 204                            |
| `GET /v1/remote/refs?remote=`      | NDJSON, the remote's reference listing            |
```

with a sentence noting these routes exist only when the daemon was started with remote support, and a pointer to `remote.md`.

- [ ] **Step 3: Update `architecture/references.md`** — append one short section "References on a remote server" stating: the server only accepts **signed** records; the signer key (CBOR key 5) owns the name — same-signer overwrites only, `admin` transport keys override; deletion is admin-only; the pointed-to key must exist server-side, so the order is push-objects → push-ref (and pull-objects → pull-ref locally). Link to `remote.md`.

- [ ] **Step 4: Check `README.md`** for a command list; if present, add `serve` and `remote` one-liners in its style.

- [ ] **Step 5: Commit**

```bash
git add architecture/ README.md
git commit -m "docs: architecture/remote.md documents the remote server and sync protocol"
```

---

## Plan self-review notes (already applied)

- **Spec coverage:** §1 architecture → Tasks 7, 18, 21-23; §2 auth → Tasks 1, 3-5, 8; §3 protocol/sync → Tasks 2, 6, 9-17, 19; §4 ref semantics/errors/testing → Tasks 12, 19, 24; docs → Task 25.
- **Known intentional deviation:** the server verifies the request signature *before* the nonce-replay check (spec lists timestamp → nonce → signature). Recording nonces only for validly-signed requests prevents unauthenticated junk from growing the replay cache; security is equivalent. Task 8's middleware comment records this.
- **Type-consistency anchors:** `httpsig.SignRequest/VerifyRequest/SignResponse/VerifyResponse/HashBody`, `keylist.Flatten/Parse`, `allowlist.List.Lookup`, `nonces.Cache.SeenBefore`, `remotes.Registry.Add/Remove/Get/All/Resolve`, `remoteclient.Client.Missing/PushPack/FetchObjects/PutRef/GetRef/ListRefs`, `remotesync.Push/Pull/Batches/PushSizer/PullSizer/Opts`, `daemon.NewWithRemotes/RemoteConfig`, `client.Remote*` — later tasks use exactly these names; if an executor renames one, the change must propagate forward.
- **Places the executor must verify against the live code** (flagged inline): `fstree.Entry` field names (Tasks 6, 16), the `cmd/amber-store` test-helper signatures (`startDaemon`, `configureTestUserWithKey`, ingest/restore CLI argument shapes — Tasks 22, 23), shared `--socket` flag helpers (Task 23).







