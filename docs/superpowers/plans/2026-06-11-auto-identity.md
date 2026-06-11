# Auto-generated SSH Identities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `amber-store serve` and `amber-store daemon` generate and persist their own ed25519 SSH identity in the store directory when no key flag is given.

**Architecture:** A new `internal/identity` package owns load-or-create of `<store>/identity` (+ `.pub`). `serve` resolves `--identity` through a small helper that falls back to the package; the daemon resolves its default `--remote-key` signer the same way. Existing explicit-key paths are untouched.

**Tech Stack:** Go, `golang.org/x/crypto/ssh` (already a dependency; `ssh.MarshalPrivateKey` is available at the pinned v0.53.0).

**Spec:** `docs/superpowers/specs/2026-06-11-auto-identity-design.md`

**Conventions:** run tests with `go test ./<pkg>/...`. Wrap errors with `%w` and lowercase messages, matching the codebase. Commit after each task.

---

### Task 1: `internal/identity` package

**Files:**
- Create: `internal/identity/identity.go`
- Create: `internal/identity/identity_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/identity/identity_test.go`:

```go
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestLoadOrCreateRoundTrip(t *testing.T) {
	// A nested, not-yet-existing dir exercises the MkdirAll path.
	dir := filepath.Join(t.TempDir(), "store")
	s1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Fatal("second LoadOrCreate returned a different key")
	}

	info, err := os.Stat(filepath.Join(dir, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o, want 0600", info.Mode().Perm())
	}
	pubInfo, err := os.Stat(filepath.Join(dir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	if pubInfo.Mode().Perm() != 0o644 {
		t.Fatalf("identity.pub mode = %o, want 0644", pubInfo.Mode().Perm())
	}

	pubBytes, err := os.ReadFile(filepath.Join(dir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	pub, comment, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatalf("identity.pub is not a valid authorized_keys line: %v", err)
	}
	if comment != Comment {
		t.Fatalf("identity.pub comment = %q, want %q", comment, Comment)
	}
	if !bytes.Equal(pub.Marshal(), s1.PublicKey().Marshal()) {
		t.Fatal("identity.pub does not match the private key")
	}
	if pub.Type() != ssh.KeyAlgoED25519 {
		t.Fatalf("key type = %s, want %s", pub.Type(), ssh.KeyAlgoED25519)
	}
}

func TestLoadOrCreateRejectsCorruptKey(t *testing.T) {
	dir := t.TempDir()
	garbage := []byte("not a key")
	if err := os.WriteFile(filepath.Join(dir, "identity"), garbage, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("corrupt identity accepted")
	}
	// The bad file must survive untouched — never silently regenerated.
	got, err := os.ReadFile(filepath.Join(dir, "identity"))
	if err != nil || !bytes.Equal(got, garbage) {
		t.Fatalf("identity file was modified: %q, %v", got, err)
	}
}

func TestLoadOrCreateRejectsEncryptedKey(t *testing.T) {
	dir := t.TempDir()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "identity"), pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadOrCreate(dir)
	if err == nil || !strings.Contains(err.Error(), "passphrase") {
		t.Fatalf("err = %v, want passphrase-protected error", err)
	}
}

func TestLoadOrCreateHealsMissingPub(t *testing.T) {
	dir := t.TempDir()
	s1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	pubPath := filepath.Join(dir, "identity.pub")
	if err := os.Remove(pubPath); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatal(err)
	}
	pubBytes, err := os.ReadFile(pubPath)
	if err != nil {
		t.Fatalf("identity.pub not regenerated: %v", err)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pub.Marshal(), s1.PublicKey().Marshal()) {
		t.Fatal("regenerated identity.pub does not match the private key")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/identity/...`
Expected: FAIL — package does not compile (`LoadOrCreate` and `Comment` undefined).

- [ ] **Step 3: Write the implementation**

Create `internal/identity/identity.go`:

```go
// Package identity manages a store's own SSH identity: an ed25519 keypair
// generated on first use and persisted in the store directory, used by the
// remote server and by the daemon's remote sync when no explicit key is
// configured.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
)

// Comment is the comment embedded in generated keys.
const Comment = "amber-store"

// LoadOrCreate returns the store's own SSH identity, generating an ed25519
// keypair on first use. The private key lives at <storeDir>/identity (0600)
// and its public half at <storeDir>/identity.pub (0644) — the latter is what
// an operator copies into a server's allowed-keys file. An existing identity
// that cannot be parsed (including passphrase-protected keys) is an error;
// the file is never overwritten, because the key may already be trusted
// elsewhere.
func LoadOrCreate(storeDir string) (ssh.Signer, error) {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating store directory: %w", err)
	}
	keyPath := filepath.Join(storeDir, "identity")
	b, err := os.ReadFile(keyPath)
	switch {
	case err == nil:
		return load(keyPath, b)
	case os.IsNotExist(err):
		return create(keyPath)
	default:
		return nil, fmt.Errorf("reading identity %s: %w", keyPath, err)
	}
}

// load parses an existing identity and self-heals a missing .pub so the
// public key is always available for copy-paste.
func load(keyPath string, b []byte) (ssh.Signer, error) {
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		var miss *ssh.PassphraseMissingError
		if errors.As(err, &miss) {
			return nil, fmt.Errorf("identity %s is passphrase-protected; auto-managed store identities must be unencrypted — configure the key explicitly instead", keyPath)
		}
		return nil, fmt.Errorf("parsing identity %s: %w", keyPath, err)
	}
	pubPath := keyPath + ".pub"
	if _, err := os.Stat(pubPath); os.IsNotExist(err) {
		if err := writePub(pubPath, signer.PublicKey()); err != nil {
			return nil, err
		}
	}
	return signer, nil
}

func create(keyPath string) (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating identity: %w", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, Comment)
	if err != nil {
		return nil, fmt.Errorf("encoding identity: %w", err)
	}
	// Temp-file + rename so a crash cannot leave a half-written key behind.
	tmp := keyPath + ".tmp"
	if err := os.WriteFile(tmp, pem.EncodeToMemory(block), 0o600); err != nil {
		return nil, fmt.Errorf("writing identity: %w", err)
	}
	if err := os.Rename(tmp, keyPath); err != nil {
		return nil, fmt.Errorf("writing identity: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, fmt.Errorf("building signer: %w", err)
	}
	if err := writePub(keyPath+".pub", signer.PublicKey()); err != nil {
		return nil, err
	}
	return signer, nil
}

// writePub writes an authorized_keys-format line with the package comment.
func writePub(pubPath string, pub ssh.PublicKey) error {
	line := bytes.TrimRight(ssh.MarshalAuthorizedKey(pub), "\n")
	line = append(line, ' ')
	line = append(line, Comment...)
	line = append(line, '\n')
	if err := os.WriteFile(pubPath, line, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", pubPath, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/identity/...`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add internal/identity
git commit -m "feat: internal/identity generates and persists a store SSH identity"
```

---

### Task 2: `serve` auto-generates its identity

**Files:**
- Modify: `cmd/amber-store/serve.go` (the `--identity` flag at ~lines 64-69; identity resolution in `runServe` at ~lines 128-132)
- Test: `cmd/amber-store/serve_test.go`

- [ ] **Step 1: Write the failing test**

Add to `cmd/amber-store/serve_test.go`. Note `TestServeRequiresFlags` currently has a "no identity" case — it must be **removed** in this task (no `--identity` is now valid), leaving the no-allowed-keys and no-store cases:

```go
func TestServeRequiresFlags(t *testing.T) {
	identity, allowed := writeServeFixtures(t)
	cases := [][]string{
		{"amber-store", "serve", "--store", t.TempDir(), "--identity", identity},    // no allowed-keys
		{"amber-store", "serve", "--identity", identity, "--allowed-keys", allowed}, // no store
	}
	for _, args := range cases {
		if err := newApp().Run(args); err == nil {
			t.Fatalf("serve %v succeeded, want missing-flag error", args[2:])
		}
	}
}
```

New test (also add `bytes`, `context`, `net`, `time`, and `github.com/draganm/amber-store/remoteclient` to the imports):

```go
// TestServeAutoIdentity starts serve without --identity and checks that the
// served identity matches the auto-generated <store>/identity.pub.
func TestServeAutoIdentity(t *testing.T) {
	_, allowed := writeServeFixtures(t)
	storeDir := filepath.Join(t.TempDir(), "store")

	// Reserve a port so the test can find the server.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- newApp().RunContext(ctx, []string{
			"amber-store", "serve", "--store", storeDir,
			"--allowed-keys", allowed, "--listen", addr, "--sync=false",
		})
	}()

	var pubWire []byte
	deadline := time.Now().Add(10 * time.Second)
	for {
		pubWire, err = remoteclient.FetchIdentity(ctx, "http://"+addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not come up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	pubBytes, err := os.ReadFile(filepath.Join(storeDir, "identity.pub"))
	if err != nil {
		t.Fatalf("identity.pub not written: %v", err)
	}
	filePub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(filePub.Marshal(), pubWire) {
		t.Fatal("served identity does not match the generated identity.pub")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify the new one fails**

Run: `go test ./cmd/amber-store/ -run 'TestServeAutoIdentity|TestServeRequiresFlags' -v`
Expected: `TestServeAutoIdentity` FAILS (serve errors out: `Required flag "identity" not set`); `TestServeRequiresFlags` passes.

- [ ] **Step 3: Implement**

In `cmd/amber-store/serve.go`:

1. The `--identity` flag loses `Required: true` and gets new usage text:

```go
&cli.StringFlag{
	Name:        "identity",
	Usage:       "server SSH identity: a private-key file, or a .pub resolved through the ssh-agent (default: auto-generated in the store directory)",
	Destination: &cfg.identity,
},
```

2. Add a resolution helper next to `remoteIdentitySigner` (import `github.com/draganm/amber-store/internal/identity`):

```go
// resolveIdentity loads the explicitly configured identity, or the store's
// auto-generated one when no path was given.
func resolveIdentity(identityPath, storeDir string) (ssh.Signer, func(), error) {
	if identityPath == "" {
		signer, err := identity.LoadOrCreate(storeDir)
		return signer, func() {}, err
	}
	return remoteIdentitySigner(identityPath)
}
```

3. In `runServe`, replace the identity loading (and rename the local variable so it doesn't shadow the new package import — it is used at the `server.Config{...Identity:}` field and in the startup log):

```go
	signer, closeIdentity, err := resolveIdentity(cfg.identity, cfg.store)
	if err != nil {
		return err
	}
	defer closeIdentity()
```

…and further down use `Identity: signer` in the `server.Config` literal and `"identity", ssh.FingerprintSHA256(signer.PublicKey())` in the `serve listening` log line.

- [ ] **Step 4: Run the package tests**

Run: `go test ./cmd/amber-store/`
Expected: PASS (including `TestServeRejectsEncryptedIdentityFile`, which exercises the unchanged explicit-flag path).

- [ ] **Step 5: Commit**

```bash
git add cmd/amber-store/serve.go cmd/amber-store/serve_test.go
git commit -m "feat: serve auto-generates its identity when --identity is omitted"
```

---

### Task 3: daemon auto-generates its default remote signer

**Files:**
- Modify: `cmd/amber-store/daemon.go` (the `--remote-key` usage at ~lines 77-84; signer wiring in `runDaemon` at ~lines 161-168; the `daemon listening` log at ~line 208)
- Test: `cmd/amber-store/daemon_remote_test.go`
- Test: `cmd/amber-store/remote_test.go`

- [ ] **Step 1: Write the failing unit tests**

Add to `cmd/amber-store/daemon_remote_test.go` (add `bytes`, `os`, `path/filepath` to its imports):

```go
func TestDefaultRemoteSignerAutoGenerates(t *testing.T) {
	dir := t.TempDir()
	s1, err := defaultRemoteSigner(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "identity.pub")); err != nil {
		t.Fatalf("identity.pub not written: %v", err)
	}
	s2, err := defaultRemoteSigner(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Fatal("auto identity not stable across restarts")
	}
}

func TestDefaultRemoteSignerKeepsConfigured(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	configured := signerFromKeyPath(t, keyPath)
	dir := t.TempDir()
	got, err := defaultRemoteSigner(configured, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.PublicKey().Marshal(), configured.PublicKey().Marshal()) {
		t.Fatal("configured signer was replaced")
	}
	if _, err := os.Stat(filepath.Join(dir, "identity")); !os.IsNotExist(err) {
		t.Fatal("identity file generated despite a configured key")
	}
}
```

- [ ] **Step 2: Write the failing end-to-end test**

Add to `cmd/amber-store/remote_test.go`. It resolves the signer exactly the way `runDaemon` does without `--remote-key` (via `defaultRemoteSigner(nil, …)`), allows the resulting `identity.pub` on the server, and pushes. The ref-signing user key is deliberately a *different* key, demonstrating that the transport identity and the ref-signing identity are independent:

```go
// TestRemotePushWithAutoIdentity pushes through a daemon whose transport
// signer is the auto-generated store identity, against a server whose
// allowlist holds the generated identity.pub.
func TestRemotePushWithAutoIdentity(t *testing.T) {
	storeDir := t.TempDir()
	signer, err := defaultRemoteSigner(nil, storeDir) // what runDaemon does without --remote-key
	if err != nil {
		t.Fatal(err)
	}

	pubBytes, err := os.ReadFile(filepath.Join(storeDir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	srv := startRemoteServer(t, pub)
	sock := startDaemonWithRemotes(t, signer)

	refKeyPath, _ := writeSigningKey(t)
	configureTestUserWithKey(t, "tester", refKeyPath)

	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "--yes", "origin", srv.URL); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("auto identity"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, "", "ingest", "--no-progress", "--socket", sock, "snap", dir); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	out, err := runApp(t, "", "remote", "push-objects", "--socket", sock, "origin", "snap")
	if err != nil {
		t.Fatalf("push-objects: %v", err)
	}
	if !strings.Contains(out, "pushed") {
		t.Fatalf("push-objects output: %q", out)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./cmd/amber-store/ -run 'TestDefaultRemoteSigner|TestRemotePushWithAutoIdentity' -v`
Expected: FAIL — does not compile (`defaultRemoteSigner` undefined).

- [ ] **Step 4: Implement**

In `cmd/amber-store/daemon.go`:

1. New usage text for `--remote-key`:

```go
&cli.StringSliceFlag{
	Name: "remote-key",
	Usage: "SSH identity for remote sync: PATH (default for all remotes) " +
		"or NAME=PATH (per-remote override); repeatable. Passphrase-protected " +
		"keys must be used via the ssh-agent (.pub path). Default: an " +
		"auto-generated identity stored in the store directory.",
	Destination: &cfg.remoteKeys,
},
```

2. Add the helper (import `github.com/draganm/amber-store/internal/identity`):

```go
// defaultRemoteSigner returns the daemon's default remote-sync signer: the
// configured one when --remote-key gave a bare PATH, otherwise the store's
// auto-generated identity.
func defaultRemoteSigner(configured ssh.Signer, storeDir string) (ssh.Signer, error) {
	if configured != nil {
		return configured, nil
	}
	return identity.LoadOrCreate(storeDir)
}
```

3. In `runDaemon`, right after the `parseRemoteKeys` call:

```go
	defSigner, overrides, err := parseRemoteKeys(cfg.remoteKeys.Value())
	if err != nil {
		return err
	}
	defSigner, err = defaultRemoteSigner(defSigner, cfg.store)
	if err != nil {
		return err
	}
```

4. Extend the startup log so the operator sees which key the daemon signs with:

```go
	logger.Info("daemon listening", "socket", sock, "store", cfg.store,
		"identity", ssh.FingerprintSHA256(defSigner.PublicKey()))
```

- [ ] **Step 5: Run the package tests**

Run: `go test ./cmd/amber-store/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/amber-store/daemon.go cmd/amber-store/daemon_remote_test.go cmd/amber-store/remote_test.go
git commit -m "feat: daemon auto-generates its remote-sync identity when --remote-key is omitted"
```

---

### Task 4: docs + full verification

**Files:**
- Modify: `architecture/remote.md` (flag-shape block at lines 17-24 and 30-42; "Identity and trust" section at lines 43-48)

- [ ] **Step 1: Update the flag-shape block**

Replace lines 17-24 of `architecture/remote.md`:

```
amber-store serve \
  --store DIR \
  --listen ADDR \        # default :8590
  --allowed-keys FILE \  # authorized_keys format; 'admin' option marks ops keys; reloaded on SIGHUP
  [--identity PATH] \    # server's SSH key: private-key file or .pub via ssh-agent; default: auto-generated in DIR
  [--tls-cert FILE --tls-key FILE]
```

and the daemon block (lines 32-37):

```
amber-store daemon \
  [--remote-key PATH] \      # default signing key for all remotes; default: auto-generated in the store dir
  [--remote-key NAME=PATH] \ # per-remote override (repeatable)
  ...
```

- [ ] **Step 2: Document the auto-generated identity**

In the "Identity and trust" section, extend the **Server side** paragraph (after line 48) with a new paragraph:

```
**Default identities.** When no key flag is given, each service generates an
ed25519 keypair on first start and persists it in its store directory as
`identity` (0600) and `identity.pub` (0644). The same key is loaded on every
later start; an existing file that cannot be parsed is an error, never
overwritten. `identity.pub` is what an operator copies into a server's
`--allowed-keys` file to authorize a daemon.
```

- [ ] **Step 3: Full verification**

Run: `gofmt -l . && go vet ./... && go test ./...`
Expected: no gofmt output, vet clean, all packages PASS.

- [ ] **Step 4: Commit**

```bash
git add architecture/remote.md
git commit -m "docs: remote.md documents auto-generated identities"
```
