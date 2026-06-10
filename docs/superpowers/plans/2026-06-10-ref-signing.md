# Reference Signing with SSH Keys — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sign references at creation time (ingest, ref create) with the user's SSH key — private-key files (incl. passphrase-protected) or ssh-agent-backed keys (Secretive, yubikey-agent, …) — storing a raw SSHSIG blob in the record's reserved signature field.

**Architecture:** A new `internal/sshsign` package resolves the configured key path (public key → agent signing via `$SSH_AUTH_SOCK`; private key → in-process signing with an injectable passphrase prompt) and produces SSHSIG v1 signatures (namespace `amber-store-ref`, SHA-512) over `reference.SignaturePayload()`. The per-user config gains a `signing_key` path; the two ref-creation sites sign when it is set and fail closed when signing fails. Daemon, wire protocol, and record encoding are untouched.

**Tech Stack:** Go, `github.com/hiddeco/sshsig` v0.2.0, `golang.org/x/crypto/ssh` (+ `ssh/agent`), `golang.org/x/term`.

**Spec:** `docs/superpowers/specs/2026-06-10-ref-signing-design.md`

## File structure

| File | Responsibility |
| --- | --- |
| `internal/sshsign/sshsign.go` (create) | Key resolution (file vs `.pub`→agent), passphrase prompting, sk-* rejection, SSHSIG production, `CheckKeyFile` |
| `internal/sshsign/sshsign_test.go` (create) | Unit tests: file keys, encrypted keys, agent, sk-*, OpenSSH interop |
| `internal/userconfig/userconfig.go` (modify) | `SigningKey` config field |
| `internal/userconfig/userconfig_test.go` (create) | Round-trip of the new field |
| `cmd/amber-store/config_user.go` (modify) | `--signing-key` flag: validate, set, preserve, clear |
| `cmd/amber-store/config_user_test.go` (modify) | Flag behavior tests |
| `cmd/amber-store/ref.go` (modify) | Sign in `ref create` |
| `cmd/amber-store/ref_test.go` (modify) | Shared signing test helpers + signed-create tests |
| `cmd/amber-store/ingest.go` (modify) | Sign in daemon-mode ingest |
| `cmd/amber-store/ingest_ref_test.go` (modify) | Signed-ingest test |
| `architecture/references.md` (modify) | Concrete signature format |

API reference for `hiddeco/sshsig` v0.2.0 (verified):
`Sign(m io.Reader, signer ssh.Signer, h HashAlgorithm, namespace string) (*Signature, error)`, `(*Signature).Marshal() []byte` (raw blob), `ParseSignature([]byte) (*Signature, error)`, `Verify(m io.Reader, sig *Signature, pub ssh.PublicKey, h HashAlgorithm, namespace string) error`, `Armor(*Signature) []byte`, constants `HashSHA512`, `HashSHA256`.

---

### Task 1: `internal/sshsign` — signing with plain private-key files

**Files:**
- Create: `internal/sshsign/sshsign.go`
- Create: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Add dependencies**

```bash
cd /Users/dragan/draganm/amber-store
go get github.com/hiddeco/sshsig@v0.2.0 golang.org/x/crypto@latest golang.org/x/term@latest
```

Expected: `go.mod` gains the three requires (x/term may come in as an indirect of x/crypto; `go get` pins it directly).

- [ ] **Step 2: Write the failing test**

Create `internal/sshsign/sshsign_test.go`:

```go
package sshsign_test

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
)

// noPrompt fails the test if the passphrase prompt is invoked.
func noPrompt(t *testing.T) sshsign.PassphrasePrompt {
	return func(path string) ([]byte, error) {
		t.Fatalf("passphrase prompt invoked for %s", path)
		return nil, nil
	}
}

// writeKeyFiles writes an OpenSSH private key (encrypted when passphrase is
// non-empty) plus its .pub next to it, returning both paths and the public key.
func writeKeyFiles(t *testing.T, priv crypto.PrivateKey, passphrase string) (privPath, pubPath string, pub ssh.PublicKey) {
	t.Helper()
	var block *pem.Block
	var err error
	if passphrase == "" {
		block, err = ssh.MarshalPrivateKey(priv, "")
	} else {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte(passphrase))
	}
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	pub = signer.PublicKey()
	dir := t.TempDir()
	privPath = filepath.Join(dir, "key")
	pubPath = filepath.Join(dir, "key.pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubPath, ssh.MarshalAuthorizedKey(pub), 0o644); err != nil {
		t.Fatal(err)
	}
	return privPath, pubPath, pub
}

// mustVerify checks blob is a valid SSHSIG over payload by pub in our namespace.
func mustVerify(t *testing.T, blob, payload []byte, pub ssh.PublicKey) {
	t.Helper()
	sig, err := sshsig.ParseSignature(blob)
	if err != nil {
		t.Fatalf("parsing produced signature: %v", err)
	}
	if err := sshsig.Verify(bytes.NewReader(payload), sig, pub, sshsig.HashSHA512, sshsign.Namespace); err != nil {
		t.Fatalf("verifying produced signature: %v", err)
	}
}

func TestSignWithEd25519File(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(privPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignWithRSAFile(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(privPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignMissingKeyFile(t *testing.T) {
	_, err := sshsign.Sign(filepath.Join(t.TempDir(), "absent"), []byte("p"), noPrompt(t))
	if err == nil {
		t.Fatal("expected error for missing key file")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/sshsign/ -v`
Expected: FAIL — package `sshsign` does not exist / `Sign` undefined.

- [ ] **Step 4: Write the implementation**

Create `internal/sshsign/sshsign.go`:

```go
// Package sshsign produces SSHSIG signatures (the ssh-keygen -Y / git SSH
// signing format) over reference signature payloads, using either a private
// key file or a key held by the ssh-agent (selected by a .pub file).
package sshsign

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
)

// Namespace is the SSHSIG namespace for amber-store reference signatures;
// namespaces prevent a signature from being replayed in another protocol.
const Namespace = "amber-store-ref"

// PassphrasePrompt obtains the passphrase for the encrypted key at path.
type PassphrasePrompt func(path string) ([]byte, error)

// Sign signs payload with the key at keyPath and returns a raw (un-armored)
// SSHSIG blob. A public-key file selects the matching ssh-agent key; a
// private-key file is parsed directly, calling prompt at most once if it is
// encrypted.
func Sign(keyPath string, payload []byte, prompt PassphrasePrompt) ([]byte, error) {
	b, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("reading signing key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		var miss *ssh.PassphraseMissingError
		if !errors.As(err, &miss) {
			return nil, fmt.Errorf("parsing signing key %s: %w", keyPath, err)
		}
		pass, perr := prompt(keyPath)
		if perr != nil {
			return nil, fmt.Errorf("reading passphrase for %s: %w", keyPath, perr)
		}
		signer, err = ssh.ParsePrivateKeyWithPassphrase(b, pass)
		if err != nil {
			return nil, fmt.Errorf("decrypting signing key %s: %w", keyPath, err)
		}
	}
	return rawSign(signer, payload)
}

// rawSign wraps a one-shot SSHSIG signing, returning the binary blob.
func rawSign(signer ssh.Signer, payload []byte) ([]byte, error) {
	sig, err := sshsig.Sign(bytes.NewReader(payload), signer, sshsig.HashSHA512, Namespace)
	if err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}
	return sig.Marshal(), nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sshsign/ -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Run the full suite and tidy**

Run: `go mod tidy && go test ./...`
Expected: all packages PASS; `go.mod` clean.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/sshsign/
git commit -m "feat: sshsign package signs payloads with private-key files"
```

---

### Task 2: Encrypted private keys with injectable passphrase prompt

**Files:**
- Modify: `internal/sshsign/sshsign.go` (add `TTYPrompt`)
- Modify: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/sshsign/sshsign_test.go`:

```go
func TestSignWithEncryptedKey(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "letmein")
	payload := []byte("payload bytes")
	calls := 0
	blob, err := sshsign.Sign(privPath, payload, func(path string) ([]byte, error) {
		calls++
		if path != privPath {
			t.Fatalf("prompt path = %q, want %q", path, privPath)
		}
		return []byte("letmein"), nil
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if calls != 1 {
		t.Fatalf("prompt called %d times, want 1", calls)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignWithWrongPassphrase(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, _ := writeKeyFiles(t, priv, "letmein")
	_, err = sshsign.Sign(privPath, []byte("p"), func(string) ([]byte, error) {
		return []byte("wrong"), nil
	})
	if err == nil {
		t.Fatal("expected error for wrong passphrase")
	}
}
```

- [ ] **Step 2: Run tests to verify status**

Run: `go test ./internal/sshsign/ -run 'TestSignWith(Encrypted|Wrong)' -v`
Expected: PASS already — Task 1's implementation handles the prompt path. (These tests pin the behavior; the new code in this task is `TTYPrompt`, which is untestable without a TTY and verified by review.)

- [ ] **Step 3: Add the production prompt**

Append to `internal/sshsign/sshsign.go` (imports gain `golang.org/x/term`):

```go
// TTYPrompt reads a passphrase from the controlling terminal without echo.
// It is the PassphrasePrompt used outside tests.
func TTYPrompt(path string) ([]byte, error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("opening terminal to prompt for passphrase (key %s): %w", path, err)
	}
	defer tty.Close()
	fmt.Fprintf(tty, "Enter passphrase for %s: ", path)
	defer fmt.Fprintln(tty)
	return term.ReadPassword(int(tty.Fd()))
}
```

- [ ] **Step 4: Run the package tests**

Run: `go test ./internal/sshsign/ -v && go vet ./internal/sshsign/`
Expected: PASS, no vet findings.

- [ ] **Step 5: Commit**

```bash
git add internal/sshsign/
git commit -m "feat: sshsign supports encrypted keys with TTY passphrase prompt"
```

---

### Task 3: Agent signing via a `.pub` key path

**Files:**
- Modify: `internal/sshsign/sshsign.go`
- Modify: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/sshsign/sshsign_test.go` (imports gain `net`, `golang.org/x/crypto/ssh/agent`):

```go
// startTestAgent serves an in-memory ssh-agent holding keys over a unix
// socket and points $SSH_AUTH_SOCK at it.
func startTestAgent(t *testing.T, keys ...crypto.PrivateKey) {
	t.Helper()
	kr := agent.NewKeyring()
	for _, k := range keys {
		if err := kr.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			t.Fatal(err)
		}
	}
	// Short dir: unix sun_path is capped at ~104 bytes on macOS/BSD and
	// t.TempDir() embeds the long test name.
	dir, err := os.MkdirTemp("", "amber-agent-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go agent.ServeAgent(kr, conn)
		}
	}()
	t.Setenv("SSH_AUTH_SOCK", sock)
}

func TestSignWithAgentKey(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, pub := writeKeyFiles(t, priv, "")
	startTestAgent(t, priv)
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(pubPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignAgentKeyNotLoaded(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, pub := writeKeyFiles(t, priv, "")
	startTestAgent(t, otherPriv) // agent holds a different key
	_, err = sshsign.Sign(pubPath, []byte("p"), noPrompt(t))
	if err == nil {
		t.Fatal("expected error when key absent from agent")
	}
	if want := ssh.FingerprintSHA256(pub); !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name fingerprint %s", err, want)
	}
}

func TestSignAgentUnavailable(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, _ := writeKeyFiles(t, priv, "")
	t.Setenv("SSH_AUTH_SOCK", "")
	_, err = sshsign.Sign(pubPath, []byte("p"), noPrompt(t))
	if err == nil || !strings.Contains(err.Error(), "SSH_AUTH_SOCK") {
		t.Fatalf("error = %v, want mention of SSH_AUTH_SOCK", err)
	}
}
```

Add `"strings"` to the test imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sshsign/ -run TestSignAgent -v && go test ./internal/sshsign/ -run TestSignWithAgentKey -v`
Expected: FAIL — `Sign` currently tries to parse the `.pub` as a private key and errors with "parsing signing key".

- [ ] **Step 3: Implement agent signing**

In `internal/sshsign/sshsign.go`, add imports `net` and `golang.org/x/crypto/ssh/agent`, insert the public-key branch at the top of `Sign` (right after the `os.ReadFile` error check):

```go
	if pub, _, _, _, perr := ssh.ParseAuthorizedKey(b); perr == nil {
		return signWithAgent(pub, payload)
	}
```

and append:

```go
// signWithAgent signs with the agent key matching pub.
func signWithAgent(pub ssh.PublicKey, payload []byte) ([]byte, error) {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil, errors.New("signing key is a public key, but no ssh-agent is available ($SSH_AUTH_SOCK is not set)")
	}
	conn, err := net.Dial("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("connecting to ssh-agent: %w", err)
	}
	defer conn.Close()
	signers, err := agent.NewClient(conn).Signers()
	if err != nil {
		return nil, fmt.Errorf("listing ssh-agent keys: %w", err)
	}
	want := pub.Marshal()
	for _, s := range signers {
		if bytes.Equal(s.PublicKey().Marshal(), want) {
			return rawSign(s, payload)
		}
	}
	return nil, fmt.Errorf("signing key %s is not loaded in the ssh-agent", ssh.FingerprintSHA256(pub))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sshsign/ -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add internal/sshsign/
git commit -m "feat: sshsign signs via ssh-agent when key path is a .pub"
```

---

### Task 4: Reject FIDO2 `sk-*` key files; add `CheckKeyFile`

**Files:**
- Modify: `internal/sshsign/sshsign.go`
- Modify: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/sshsign/sshsign_test.go`:

```go
// fakeSKKeyPEM builds an openssh-key-v1 container whose embedded public key
// announces a FIDO2 sk- type. Only the cleartext header matters for the
// detection under test; no private section is needed.
func fakeSKKeyPEM(t *testing.T) []byte {
	t.Helper()
	pubBlob := ssh.Marshal(struct {
		Algo  string
		Bytes []byte
	}{"sk-ssh-ed25519@openssh.com", make([]byte, 32)})
	body := append([]byte("openssh-key-v1\x00"), ssh.Marshal(struct {
		CipherName, KdfName, KdfOpts string
		NumKeys                      uint32
		PubKey                       []byte
	}{"none", "none", "", 1, pubBlob})...)
	return pem.EncodeToMemory(&pem.Block{Type: "OPENSSH PRIVATE KEY", Bytes: body})
}

func TestSignRejectsSKKeyFileWithHint(t *testing.T) {
	p := filepath.Join(t.TempDir(), "id_ed25519_sk")
	if err := os.WriteFile(p, fakeSKKeyPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := sshsign.Sign(p, []byte("p"), noPrompt(t))
	if err == nil || !strings.Contains(err.Error(), ".pub") || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("error = %v, want sk- hint mentioning agent and .pub", err)
	}
}

func TestCheckKeyFile(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plainPath, pubPath, _ := writeKeyFiles(t, priv, "")
	_, encPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encPath, _, _ := writeKeyFiles(t, encPriv, "letmein")
	skPath := filepath.Join(t.TempDir(), "sk")
	if err := os.WriteFile(skPath, fakeSKKeyPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	garbagePath := filepath.Join(t.TempDir(), "garbage")
	if err := os.WriteFile(garbagePath, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, path string
		wantErr    bool
	}{
		{"private key", plainPath, false},
		{"public key", pubPath, false},
		{"encrypted private key", encPath, false},
		{"sk private key", skPath, true},
		{"garbage", garbagePath, true},
		{"missing", filepath.Join(t.TempDir(), "absent"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sshsign.CheckKeyFile(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckKeyFile(%s) = %v, wantErr %v", tc.path, err, tc.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sshsign/ -run 'TestSignRejectsSK|TestCheckKeyFile' -v`
Expected: FAIL — `CheckKeyFile` undefined; the sk test fails because the error lacks the hint.

- [ ] **Step 3: Implement sk detection and CheckKeyFile**

In `internal/sshsign/sshsign.go`, add imports `encoding/pem` and `strings`. Insert into `Sign`, after the agent branch and **before** `ssh.ParsePrivateKey`:

```go
	if err := skKeyError(keyPath, b); err != nil {
		return nil, err
	}
```

Append:

```go
// skKeyError returns a tailored error when b is an OpenSSH private key whose
// (cleartext) public-key header announces a FIDO2 sk-* type: pure Go cannot
// drive the FIDO2 touch flow, so these keys are usable only via an agent.
// Any malformed input returns nil — the regular parser reports those.
func skKeyError(path string, b []byte) error {
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "OPENSSH PRIVATE KEY" {
		return nil
	}
	const magic = "openssh-key-v1\x00"
	if !bytes.HasPrefix(block.Bytes, []byte(magic)) {
		return nil
	}
	var hdr struct {
		CipherName, KdfName, KdfOpts string
		NumKeys                      uint32
		PubKey                       []byte
		Rest                         []byte `ssh:"rest"`
	}
	if err := ssh.Unmarshal(block.Bytes[len(magic):], &hdr); err != nil {
		return nil
	}
	var pk struct {
		Algo string
		Rest []byte `ssh:"rest"`
	}
	if err := ssh.Unmarshal(hdr.PubKey, &pk); err != nil {
		return nil
	}
	if !strings.HasPrefix(pk.Algo, "sk-") {
		return nil
	}
	return fmt.Errorf("signing key %s is a FIDO2 security-key (%s) private key, which cannot be used directly — load it into an ssh-agent and configure the .pub path instead", path, pk.Algo)
}

// CheckKeyFile reports whether path holds a usable signing key: an OpenSSH
// public key, or a private key. Encrypted private keys pass without a
// passphrase prompt; FIDO2 sk-* private keys are rejected with a hint.
func CheckKeyFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading signing key: %w", err)
	}
	if _, _, _, _, err := ssh.ParseAuthorizedKey(b); err == nil {
		return nil
	}
	if err := skKeyError(path, b); err != nil {
		return err
	}
	if _, err := ssh.ParsePrivateKey(b); err != nil {
		var miss *ssh.PassphraseMissingError
		if errors.As(err, &miss) {
			return nil
		}
		return fmt.Errorf("signing key %s: %w", path, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sshsign/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sshsign/
git commit -m "feat: sshsign rejects FIDO2 sk- key files and validates key files"
```

---

### Task 5: OpenSSH interop test (`ssh-keygen -Y verify`)

**Files:**
- Modify: `internal/sshsign/sshsign_test.go`

- [ ] **Step 1: Write the test**

Append (imports gain `os/exec`):

```go
// TestSignatureVerifiesWithOpenSSH proves "verify later" works with stock
// tooling: armor a produced signature and check ssh-keygen -Y verify accepts
// it against an allowed_signers entry.
func TestSignatureVerifiesWithOpenSSH(t *testing.T) {
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("ssh-keygen not on PATH")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(privPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	sig, err := sshsig.ParseSignature(blob)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	sigPath := filepath.Join(dir, "ref.sig")
	if err := os.WriteFile(sigPath, sshsig.Armor(sig), 0o644); err != nil {
		t.Fatal(err)
	}
	signersPath := filepath.Join(dir, "allowed_signers")
	line := "tester@amber " + string(ssh.MarshalAuthorizedKey(pub))
	if err := os.WriteFile(signersPath, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("ssh-keygen", "-Y", "verify",
		"-f", signersPath, "-I", "tester@amber",
		"-n", sshsign.Namespace, "-s", sigPath)
	cmd.Stdin = bytes.NewReader(payload)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh-keygen -Y verify failed: %v\n%s", err, out)
	}
}
```

- [ ] **Step 2: Run it**

Run: `go test ./internal/sshsign/ -run TestSignatureVerifiesWithOpenSSH -v`
Expected: PASS (macOS ships ssh-keygen; skips elsewhere if absent).

- [ ] **Step 3: Commit**

```bash
git add internal/sshsign/
git commit -m "test: sshsign signatures verify with ssh-keygen -Y verify"
```

---

### Task 6: `userconfig.SigningKey` field

**Files:**
- Modify: `internal/userconfig/userconfig.go:19-21`
- Create: `internal/userconfig/userconfig_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/userconfig/userconfig_test.go`:

```go
package userconfig_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/userconfig"
)

func TestSigningKeyRoundTrip(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := userconfig.Save(userconfig.Config{User: "alice", SigningKey: "/home/alice/.ssh/id_ed25519"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SigningKey != "/home/alice/.ssh/id_ed25519" {
		t.Fatalf("SigningKey = %q", cfg.SigningKey)
	}
}

func TestSigningKeyOmittedWhenEmpty(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	if err := userconfig.Save(userconfig.Config{User: "alice"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "signing_key") {
		t.Fatalf("empty SigningKey serialized: %s", b)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/userconfig/ -v`
Expected: FAIL — `cfg.SigningKey` undefined.

- [ ] **Step 3: Add the field**

In `internal/userconfig/userconfig.go`, replace the `Config` struct and drop the now-stale "leaves room" sentence from the package comment:

```go
// Config is the persisted user configuration.
type Config struct {
	User string `json:"user"`
	// SigningKey is the path to the SSH key used to sign references: a
	// private-key file, or a .pub whose key the ssh-agent holds. Empty
	// means references are created unsigned.
	SigningKey string `json:"signing_key,omitempty"`
}
```

Package comment line 2-3 becomes: `// a JSON file holding the user identity recorded in references it creates and the optional reference-signing key.`

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/userconfig/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/userconfig/
git commit -m "feat: userconfig carries optional signing_key path"
```

---

### Task 7: `config-user --signing-key` flag

**Files:**
- Modify: `cmd/amber-store/config_user.go`
- Modify: `cmd/amber-store/config_user_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/amber-store/config_user_test.go` (imports gain `crypto/ed25519`, `crypto/rand`, `encoding/pem`, `os`, `golang.org/x/crypto/ssh`):

```go
// writeTestPrivateKey writes an unencrypted ed25519 OpenSSH key, returns path.
func writeTestPrivateKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfigUserSetsSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "alice", "--signing-key", keyPath}); err != nil {
		t.Fatalf("config-user: %v", err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SigningKey != keyPath {
		t.Fatalf("SigningKey = %q, want %q", cfg.SigningKey, keyPath)
	}
}

func TestConfigUserPreservesSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "alice", "--signing-key", keyPath}); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "bob"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "bob" || cfg.SigningKey != keyPath {
		t.Fatalf("cfg = %+v, want user bob with signing key preserved", cfg)
	}
}

func TestConfigUserClearsSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	keyPath := writeTestPrivateKey(t)
	if err := newApp().Run([]string{"amber-store", "config-user", "alice", "--signing-key", keyPath}); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "alice", "--signing-key", ""}); err != nil {
		t.Fatal(err)
	}
	cfg, err := userconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SigningKey != "" {
		t.Fatalf("SigningKey = %q, want cleared", cfg.SigningKey)
	}
}

func TestConfigUserRejectsBadSigningKey(t *testing.T) {
	t.Setenv("AMBER_STORE_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	bad := filepath.Join(t.TempDir(), "garbage")
	if err := os.WriteFile(bad, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newApp().Run([]string{"amber-store", "config-user", "alice", "--signing-key", bad}); err == nil {
		t.Fatal("expected error for non-key signing-key file")
	}
}
```

Note: `TestConfigUserSetsSigningKey` compares against `keyPath` — the implementation stores `filepath.Abs(keyPath)`, and `t.TempDir()` paths are already absolute, so the comparison holds. On macOS `t.TempDir()` may go through `/var` → `/private/var` symlinks; `filepath.Abs` does **not** resolve symlinks, so the stored string equals the input string either way.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run TestConfigUser -v`
Expected: new tests FAIL (flag not defined → urfave/cli error); pre-existing TestConfigUser* tests still PASS.

- [ ] **Step 3: Implement the flag**

Replace the body of `configUserCommand()` in `cmd/amber-store/config_user.go` (imports gain `errors`, `path/filepath`, `github.com/draganm/amber-store/internal/sshsign`):

```go
func configUserCommand() *cli.Command {
	var signingKey string
	return &cli.Command{
		Name:      "config-user",
		Usage:     "record the user name written into references created by this machine",
		ArgsUsage: "NAME",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "signing-key",
				Usage:       "SSH key that signs created references: a private-key file, or a .pub whose key the ssh-agent holds; pass an empty value to clear",
				Destination: &signingKey,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("config-user requires exactly one NAME argument, got %d", c.NArg())
			}
			name := c.Args().First()
			if err := reference.ValidateUser(name); err != nil {
				return fmt.Errorf("invalid user name: %w", err)
			}
			// Start from the existing config so an omitted --signing-key
			// preserves the stored key; a missing config is a fresh start.
			cfg, err := userconfig.Load()
			if err != nil && !errors.Is(err, userconfig.ErrNotConfigured) {
				return err
			}
			cfg.User = name
			if c.IsSet("signing-key") {
				if signingKey == "" {
					cfg.SigningKey = ""
				} else {
					abs, err := filepath.Abs(signingKey)
					if err != nil {
						return err
					}
					if err := sshsign.CheckKeyFile(abs); err != nil {
						return err
					}
					cfg.SigningKey = abs
				}
			}
			if err := userconfig.Save(cfg); err != nil {
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run TestConfigUser -v`
Expected: PASS (all, including pre-existing).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/config_user.go cmd/amber-store/config_user_test.go
git commit -m "feat: config-user --signing-key sets, preserves, clears, validates"
```

---

### Task 8: `ref create` signs when a key is configured

**Files:**
- Modify: `cmd/amber-store/ref.go:62-70` (the `refCreateCommand` action tail)
- Modify: `cmd/amber-store/ref_test.go` (shared helpers + tests)

- [ ] **Step 1: Write the failing tests**

Append to `cmd/amber-store/ref_test.go` (imports gain `crypto/ed25519`, `crypto/rand`, `encoding/json`, `encoding/pem`, `github.com/draganm/amber-store/internal/sshsign`, `github.com/hiddeco/sshsig`, `golang.org/x/crypto/ssh`; `bytes`, `context`, `os`, `filepath` are already imported there — verify and add any missing):

```go
// writeSigningKey writes an unencrypted ed25519 OpenSSH private key and
// returns its path and SSH public key.
func writeSigningKey(t *testing.T) (string, ssh.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "signing-key")
	if err := os.WriteFile(p, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return p, sshPub
}

// configureTestUserWithKey is configureTestUser plus a signing key path.
func configureTestUserWithKey(t *testing.T, user, keyPath string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("AMBER_STORE_CONFIG", p)
	b, err := json.Marshal(userconfig.Config{User: user, SigningKey: keyPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// mustVerifyRef checks rec carries a valid SSHSIG by pub over its payload.
func mustVerifyRef(t *testing.T, rec reference.Reference, pub ssh.PublicKey) {
	t.Helper()
	if len(rec.Signature) == 0 {
		t.Fatal("reference has no signature")
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := sshsig.ParseSignature(rec.Signature)
	if err != nil {
		t.Fatalf("parsing stored signature: %v", err)
	}
	if err := sshsig.Verify(bytes.NewReader(payload), sig, pub, sshsig.HashSHA512, sshsign.Namespace); err != nil {
		t.Fatalf("verifying stored signature: %v", err)
	}
}

func TestRefCreateSignsWhenConfigured(t *testing.T) {
	configureTestUser(t, "signer")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "src")

	keyPath, pub := writeSigningKey(t)
	configureTestUserWithKey(t, "signer", keyPath)
	if err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "signed", root}); err != nil {
		t.Fatalf("ref create: %v", err)
	}
	rec, err := client.New(sock).GetRef(context.Background(), "signed")
	if err != nil {
		t.Fatal(err)
	}
	mustVerifyRef(t, rec, pub)
}

func TestRefCreateFailsClosedOnBadSigningKey(t *testing.T) {
	configureTestUser(t, "signer")
	sock := startDaemon(t)
	root := ingestTestTree(t, sock, "src")

	configureTestUserWithKey(t, "signer", filepath.Join(t.TempDir(), "absent-key"))
	err := newApp().Run([]string{"amber-store", "ref", "create", "--socket", sock, "signed", root})
	if err == nil {
		t.Fatal("expected ref create to fail when the configured signing key is unusable")
	}
	if _, gerr := client.New(sock).GetRef(context.Background(), "signed"); gerr == nil {
		t.Fatal("reference was created despite signing failure")
	}
}
```

The `reference` and `userconfig` imports may already be present in `ref_test.go`; add whichever are missing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run TestRefCreate -v`
Expected: `TestRefCreateSignsWhenConfigured` FAILS at "reference has no signature"; `TestRefCreateFailsClosedOnBadSigningKey` FAILS at "expected ref create to fail" (the key path is never read today).

- [ ] **Step 3: Implement signing in ref create**

In `cmd/amber-store/ref.go` (imports gain `github.com/draganm/amber-store/internal/sshsign`), replace the action tail of `refCreateCommand` — currently:

```go
			rec := reference.Reference{
				Name:      name,
				Key:       k[:],
				User:      ucfg.User,
				CreatedAt: time.Now().UnixNano(),
			}
			return client.New(socketpath.Resolve(socket)).PutRef(c.Context, rec)
```

with:

```go
			rec := reference.Reference{
				Name:      name,
				Key:       k[:],
				User:      ucfg.User,
				CreatedAt: time.Now().UnixNano(),
			}
			if ucfg.SigningKey != "" {
				payload, err := rec.SignaturePayload()
				if err != nil {
					return fmt.Errorf("encoding reference for signing: %w", err)
				}
				// Fail closed: a configured key never silently yields an
				// unsigned reference.
				sig, err := sshsign.Sign(ucfg.SigningKey, payload, sshsign.TTYPrompt)
				if err != nil {
					return fmt.Errorf("signing reference %q: %w", name, err)
				}
				rec.Signature = sig
			}
			return client.New(socketpath.Resolve(socket)).PutRef(c.Context, rec)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -run TestRefCreate -v`
Expected: PASS (including pre-existing `TestRefCreateShowLsRm`).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/ref.go cmd/amber-store/ref_test.go
git commit -m "feat: ref create signs the record when a signing key is configured"
```

---

### Task 9: daemon-mode `ingest` signs its reference

**Files:**
- Modify: `cmd/amber-store/ingest.go:233`, `cmd/amber-store/ingest.go:255-260`, `cmd/amber-store/ingest.go:330-345`
- Modify: `cmd/amber-store/ingest_ref_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/amber-store/ingest_ref_test.go`:

```go
func TestIngestSignsReferenceWhenConfigured(t *testing.T) {
	keyPath, pub := writeSigningKey(t)
	configureTestUserWithKey(t, "ingester", keyPath)
	sock := startDaemon(t)
	ingestTestTree(t, sock, "signed/backup")

	rec, err := client.New(sock).GetRef(context.Background(), "signed/backup")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	mustVerifyRef(t, rec, pub)
}
```

(Helpers come from `ref_test.go`, same package.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/amber-store/ -run TestIngestSignsReference -v`
Expected: FAIL — "reference has no signature".

- [ ] **Step 3: Implement signing in ingest**

In `cmd/amber-store/ingest.go` (imports gain `github.com/draganm/amber-store/internal/sshsign`):

At `runIngest`'s variable declaration (line ~233), change

```go
	var refName, dir, user string
```

to

```go
	var refName, dir, user, signingKey string
```

After `user = ucfg.User` (line ~259) add:

```go
		signingKey = ucfg.SigningKey
```

At the reference-creation site (line ~335), between building `rec` and `cl.PutRef`, insert:

```go
		if signingKey != "" {
			payload, err := rec.SignaturePayload()
			if err != nil {
				return fmt.Errorf("encoding reference %q for signing: %w", refName, err)
			}
			// Fail closed, but the tree is already stored — reuse the
			// existing recovery-hint style so the work is not lost.
			sig, err := sshsign.Sign(signingKey, payload, sshsign.TTYPrompt)
			if err != nil {
				return fmt.Errorf("tree stored (root %s) but signing reference %q failed: %w\nretry with: amber-store ref create %s %s",
					root, refName, err, shellQuote(refName), root)
			}
			rec.Signature = sig
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/amber-store/ -v`
Expected: PASS — the whole command package, including pre-existing ingest tests.

- [ ] **Step 5: Run the full suite**

Run: `go test ./... && go vet ./...`
Expected: all PASS, no vet findings.

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/ingest.go cmd/amber-store/ingest_ref_test.go
git commit -m "feat: ingest signs the reference it creates when a key is configured"
```

---

### Task 10: Documentation

**Files:**
- Modify: `architecture/references.md:21-23` and `architecture/references.md:62-71`

- [ ] **Step 1: Update the record section**

Replace the signature-payload paragraph (lines 21–23):

```markdown
The **signature payload** is the deterministic encoding of the record without
key 4 — the canonical bytes of `{0,1,2,3}`. When the user config carries a
signing key, clients store an **SSHSIG v1** signature (the `ssh-keygen -Y
sign` / git SSH-signing format) over this payload in key 4: namespace
`amber-store-ref`, SHA-512 message hash, raw binary blob (not PEM-armored).
The key may be a private-key file (passphrase-protected ones prompt on the
terminal) or a `.pub` resolved through the ssh-agent. Signing failures abort
the command — a configured key never silently yields an unsigned reference.
Verification is not yet implemented; the daemon stores the field opaquely.
```

- [ ] **Step 2: Update the CLI section**

Replace the `config-user` line in the CLI block (line 64):

```markdown
amber-store config-user NAME [--signing-key PATH]   # required once before creating refs; a key signs them
```

- [ ] **Step 3: Check rendering and consistency**

Run: `grep -n "signature\|signing" architecture/references.md`
Expected: no remaining "Nothing currently produces" sentence; payload definition unchanged.

- [ ] **Step 4: Commit**

```bash
git add architecture/references.md
git commit -m "docs: references.md documents the SSHSIG reference-signature format"
```

---

## Self-review notes

- Spec coverage: signature format + namespace + hash (Tasks 1, 5), file keys plain/encrypted (Tasks 1–2), agent/.pub (Task 3), sk-* rejection + config-time validation (Task 4), `signing_key` config + flag semantics (Tasks 6–7), both creation sites fail-closed (Tasks 8–9), error table behaviors (Tasks 1, 3, 4, 8), OpenSSH interop (Task 5), docs (Task 10). Daemon/wire: no task — intentionally untouched per spec.
- `reference.SignaturePayload()` already exists (`reference/reference.go:151`); no task recreates it.
- Type consistency: `sshsign.Sign(keyPath string, payload []byte, prompt PassphrasePrompt) ([]byte, error)`, `sshsign.CheckKeyFile(path string) error`, `sshsign.TTYPrompt`, `sshsign.Namespace` are used with identical signatures in Tasks 7–9.
