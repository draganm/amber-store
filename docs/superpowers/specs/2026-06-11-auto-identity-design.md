# Auto-generated SSH identities for serve and daemon

## Problem

`amber-store serve` requires `--identity` and the daemon needs a
`--remote-key` before remote sync works. Both should work out of the box:
when no key file is specified, the service generates its own identity and
stores it in the store directory.

Out of scope (already implemented): `remote add` fingerprint confirmation
(TOFU) and keeping the daemon free of acceptable-identities configuration —
trust in servers comes only from keys pinned at `remote add`.

## New package: `internal/identity`

One exported function:

```go
// LoadOrCreate returns the store's own SSH identity, generating an
// ed25519 keypair on first use.
func LoadOrCreate(storeDir string) (ssh.Signer, error)
```

Behavior:

- `os.MkdirAll(storeDir)` first, so callers are free to resolve the identity
  before or after opening the store.
- If `<storeDir>/identity` exists, parse it as an unencrypted OpenSSH
  private key. A passphrase-protected or unparseable file is an **error** —
  the file is never overwritten or regenerated, because the key may already
  be pinned by clients or listed in a server's allowlist.
- If absent: generate an ed25519 keypair, write:
  - `identity` — OpenSSH PEM private key, mode 0600, written via
    temp-file + rename so a crash cannot leave a half-written key;
  - `identity.pub` — authorized_keys format, mode 0644, comment
    `amber-store`.
- If `identity` exists but `identity.pub` is missing, rewrite the `.pub`
  from the private key (self-healing; makes the public key always
  available for copy-paste into a server's allowed-keys file).

## `serve` changes

- `--identity` becomes **optional**. New usage text: "server SSH identity:
  a private-key file, or a .pub resolved through the ssh-agent (default:
  auto-generated in the store directory)".
- When unset, `identity.LoadOrCreate(cfg.store)` provides the signer.
- The existing startup log line already prints the fingerprint — no change
  needed there.

## `daemon` changes

- When no bare-PATH `--remote-key` is given, the **default signer** comes
  from `identity.LoadOrCreate(cfg.store)`, resolved eagerly at startup.
- Per-remote `NAME=PATH` overrides are unaffected and freely mix with the
  auto-generated default.
- The "daemon listening" log line gains the identity fingerprint so the
  operator can see which key the daemon will sign with.

## Error handling

- Key generation or file-write failure aborts startup with a clear error.
- An existing-but-invalid `identity` file aborts startup (no silent
  regeneration — see above).

## Testing

- `internal/identity` unit tests: create-then-load round-trip returns the
  same public key; file modes (0600 / 0644); corrupt private key file is
  rejected; missing `.pub` is regenerated.
- CLI tests:
  - `serve` without `--identity` starts, and `GET /v1/identity` returns a
    key matching `<store>/identity.pub`; a second start reuses the same key.
  - `daemon` without `--remote-key` pushes successfully to a server whose
    allowlist contains the daemon's generated `identity.pub`.

## Docs

Update `architecture/remote.md`: the flag-shape block (`--identity` and
`--remote-key` no longer required) and the "Identity and trust" section
describe the auto-generated default and where it lives.
