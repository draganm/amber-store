# Reference Signing with SSH Keys — Design

**Date:** 2026-06-10
**Status:** approved
**Scope:** sign references at creation time with the user's SSH key.
Verification is a deliberate non-goal (follow-up project); the only guarantee
shipped here is that signed records carry a standard, later-verifiable
signature.

## Background

A reference record ([architecture/references.md](../../../architecture/references.md))
already reserves CBOR key 4 for an opaque signature, and
`reference.SignaturePayload()` already returns the bytes a signature runs
over: the canonical deterministic encoding of the record without key 4.
Nothing currently produces signatures. The per-user config
(`internal/userconfig`) was written to leave room for a signing key.

## Goals

- Sign refs created by `ingest NAME DIR` and `ref create NAME KEY` with the
  user's SSH key.
- Support the three places real keys live, via two mechanisms:
  - **private-key files** on disk, including passphrase-protected ones;
  - **ssh-agent-backed keys** — Secretive (Secure Enclave), yubikey-agent,
    1Password, plain `ssh-agent` — selected by pointing the config at the
    corresponding `.pub` file.
- Pure Go (approach chosen over shelling out to `ssh-keygen -Y sign`): no
  runtime dependency on OpenSSH binaries.

## Non-goals

- Verification (`ref verify`, allowed-signers, daemon enforcement) — later.
- FIDO2 `sk-*` private-key *files*: pure Go cannot drive the FIDO2 touch
  flow. These keys are supported only through an agent; the error message
  says so.
- SSH certificates as signing identities.

## Signature format

**SSHSIG v1** — the format produced by `ssh-keygen -Y sign` and used by git's
SSH signing, so stored signatures remain verifiable with stock OpenSSH
tooling.

- Namespace: `amber-store-ref` (SSHSIG namespaces prevent cross-protocol
  signature reuse).
- Message hash: SHA-512 (OpenSSH's default).
- Stored form: the **raw binary SSHSIG blob** (not PEM-armored) in record
  key 4. Sizes are ~300 B (ed25519) to ~1 KiB (RSA), far under the 64 KiB
  field cap. Armoring for external verification is a trivial wrapper and is
  exercised in the interop test.
- The signer's **public key** is stored alongside in record key 5 (SSH wire
  format, ≤16 KiB), set from the resolved signer before signing.
- Signed message: exactly `Reference.SignaturePayload()` — the canonical
  CBOR bytes of map keys `{0,1,2,3,5}`, so the signature covers the public
  key. (Amended 2026-06-10: originally `{0,1,2,3}` with no stored key.)

Implementation uses **`github.com/hiddeco/sshsig`** (small, MIT, used by
fluxcd) rather than hand-rolling the format: it brings `Verify` for free
(round-trip tests now, the verification feature later) and handles RSA's
`rsa-sha2-512` algorithm selection. Supporting deps: `golang.org/x/crypto`
(ssh, ssh/agent), `golang.org/x/term` (no-echo passphrase prompt).

## Components

### `internal/sshsign` (new)

Entry points (two-phase, so the signer's public key is available before the
payload — which covers it — is encoded):

```go
// Signer resolves the key at keyPath; close releases any agent connection.
func Signer(keyPath string, prompt PassphrasePrompt) (ssh.Signer, func(), error)
// SignWith signs payload with signer, returning a raw SSHSIG blob.
func SignWith(signer ssh.Signer, payload []byte) ([]byte, error)
```

Key resolution is by **file content**, not extension:

1. **File parses as an OpenSSH public key** (authorized_keys format — the
   `.pub` case): sign via the ssh-agent.
   - Dial `$SSH_AUTH_SOCK`; absent/undialable → error naming the variable
     and suggesting an agent be started.
   - List the agent's signers; match by marshaled-public-key equality.
   - Not loaded → error including the key's SHA256 fingerprint.
   - Touch/biometric/PIN prompts are the agent's concern; amber-store just
     blocks on the Sign call.
2. **Otherwise, a private key**: `ssh.ParsePrivateKey`.
   - On `*ssh.PassphraseMissingError`: prompt on `/dev/tty` via
     `x/term.ReadPassword`, retry with `ParsePrivateKeyWithPassphrase`.
     The prompt is an injectable `func() ([]byte, error)` so tests run
     without a TTY. Wrong passphrase → the parse error, command fails.
   - `sk-*` key files fail to parse in pure Go; the wrapped error tells the
     user to load the key into an agent and point `signing_key` at the
     `.pub` instead.

### `internal/userconfig` (extended)

```go
type Config struct {
    User       string `json:"user"`
    SigningKey string `json:"signing_key,omitempty"` // path to private key or .pub
}
```

### `config-user` command (extended)

```sh
amber-store config-user NAME --signing-key ~/.ssh/id_ed25519        # file key
amber-store config-user NAME --signing-key ~/.ssh/id_secretive.pub  # agent key
```

- Flag omitted → existing `signing_key` value is preserved.
- `--signing-key=` (explicit empty) → clears it.
- When set, the file is read and must parse as *some* OpenSSH key — public,
  private, or encrypted private (detected without a passphrase prompt) — so
  typos fail at config time, not at first ingest.

### Ref creation sites (`cmd/amber-store/ingest.go`, `cmd/amber-store/ref.go`)

After building record fields 0–3, when `cfg.SigningKey != ""`:

```go
payload, err := rec.SignaturePayload()
sig, err := sshsign.Sign(cfg.SigningKey, payload)
rec.Signature = sig
```

**Fail closed:** any signing error fails the command — a configured key
never silently produces an unsigned ref. No `signing_key` configured →
unsigned record, byte-identical to today's behavior.

### Unchanged

Daemon, wire protocol and `ref ls` work as-is: keys 4 and 5 are stored and
transported opaquely (the list wire format's `signed` field reflects key 4;
the CLI does not print it). `ref show` additionally renders the stored
public key in authorized_keys form (hex if unparseable).

## Error handling summary

| Condition | Behavior |
| --- | --- |
| No `signing_key` in config | Unsigned ref (status quo) |
| Key file missing/unreadable | Command fails with path in error |
| Encrypted key, wrong passphrase | Command fails with parse error |
| `.pub` configured, no `$SSH_AUTH_SOCK` | Command fails, names the variable |
| `.pub` configured, key not in agent | Command fails with SHA256 fingerprint |
| `sk-*` private-key file | Command fails, suggests agent + `.pub` |
| Agent declines / user cancels touch | Command fails with agent error |

## Testing

- **`sshsign` unit tests:** ed25519 and RSA file keys; encrypted ed25519
  key with injected passphrase prompt (correct and wrong passphrase); agent
  path via `agent.NewKeyring` served with `agent.ServeAgent` over a unix
  socketpair, `SSH_AUTH_SOCK` pointed at it; `.pub` whose key is absent
  from the agent. Every produced signature round-trips through
  `sshsig.Verify` with the `amber-store-ref` namespace.
- **Interop test** (skipped when `ssh-keygen` is not on PATH): armor a
  signature, verify with `ssh-keygen -Y verify` against an
  allowed-signers line — proving "verify later" works with stock OpenSSH.
- **CLI-level test:** `config-user` with a signing key → `ingest` → GET the
  record → signature present and verifies against the configured public
  key.

## Docs

[architecture/references.md](../../../architecture/references.md): replace
the "Nothing currently produces or verifies signatures" paragraph with the
concrete format — SSHSIG v1, namespace `amber-store-ref`, SHA-512 hash, raw
blob in key 4, payload unchanged — and note that verification remains
unimplemented. Update the CLI section's `config-user` line with the
`--signing-key` flag.
