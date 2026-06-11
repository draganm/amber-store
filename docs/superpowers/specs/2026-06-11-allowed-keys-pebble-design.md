# Allowed keys in Pebble — design

2026-06-11

## Problem

The remote server keeps its allowed client keys in an authorized_keys-format
file (`--allowed-keys`), wrapped by `internal/allowfile`: hand edits plus
SIGHUP reload the list, and the admin UI mutates the file through atomic
rewrites. Now that the admin UI exists, the file is redundant state with two
writers (operator and server) and its own reload machinery. The server should
own the allowed keys in a Pebble database instead.

## Decisions

- **The Pebble DB is the sole source of truth.** The authorized_keys file,
  the `--allowed-keys` flag, `allowlist.Load`, and the SIGHUP reload are all
  removed. No import or migration path: existing deployments re-add their
  keys through the admin UI. This is a breaking CLI change.
- **The admin UI is the only management path.** No new CLI subcommand. A
  fresh server starts with an empty allowlist and authorizes nobody until an
  operator sets `AMBER_ADMIN_PASSWORD` and adds keys through the browser.

## Storage

A dedicated Pebble DB at `<store>/allowed-keys`, opened the same way as
`refstore` (silenced pebble logger; the `--sync` flag selects
`pebble.Sync`/`pebble.NoSync`).

- **Pebble key:** the SSH wire-format public key (`pub.Marshal()`) — already
  the canonical lookup key the server authenticates with. Duplicate entries
  are structurally impossible.
- **Value:** JSON `{"admin": bool, "comment": string}`.

The verbatim authorized_keys line is not stored; the canonical line shown in
the admin UI is rebuilt from options + marshalled key + comment.

## New package: `internal/allowstore`

Replaces `internal/allowfile` with the same shape, so the admin handler and
`serve.go` change minimally:

- `Open(dir string, sync bool) (*Store, error)` — opens the DB, scans it
  once, and builds the in-memory `*allowlist.List` held in an
  `atomic.Pointer`.
- `Current() *allowlist.List` — lock-free; the server's per-request
  `Allow func() *allowlist.List` config is untouched.
- `List() ([]Key, error)` — same `Key{Line, Type, Fingerprint, Comment,
  Admin}` JSON shape as allowfile, so the SPA is untouched. Iteration is in
  deterministic Pebble key order; file insertion order is gone and no
  longer meaningful.
- `Add(line string, admin bool) error` — parses one authorized_keys line
  (rejecting trailing content), canonicalizes, rejects keys already present,
  writes the record, swaps the list.
- `Remove(fingerprint string) error` — scans for the matching wire key,
  deletes it; an unknown fingerprint is an error.
- `Close() error` — closes the DB.

Mutations serialize under a mutex and rebuild + swap the in-memory list,
mirroring allowfile's concurrency model.

## Removed

- `internal/allowfile` (package deleted; its tests port to allowstore).
- `allowlist.Load` (nothing reads key files anymore). `allowlist.Parse`
  stays: tests across six packages use it to build in-memory lists.
- `--allowed-keys` flag and the SIGHUP handler in `cmd/amber-store/serve.go`.

## serve.go wiring

Open the allowstore after the diskstore (same pattern as refstore:
`filepath.Join(store, "allowed-keys")`), close it on shutdown, and pass
`keys.Current` to `server.New` and the store to `admin.New` as before.

Startup logging:

- Empty allowlist → warn that the server authorizes nobody.
- Empty allowlist **and** no admin password → louder warning: this server
  can never authorize anyone.

## Error handling

- `Add` of a present key → error naming the fingerprint (as today).
- `Remove` of an unknown fingerprint → error (as today).
- DB open failure at startup → fatal, like refstore.

## Testing

- Port allowfile's tests to allowstore: add/list/remove round-trips,
  duplicate rejection, trailing-content rejection, admin option handling,
  unknown-fingerprint removal, and persistence across `Close`/`Open`.
- `admin` tests swap `*allowfile.File` for `*allowstore.Store`.
- The serve CLI tests drop the `--allowed-keys` flag and seed keys through
  the admin API (or by pre-populating the store directory).

## Docs

Update `architecture/remote.md` and the admin-UI spec where they describe
the allowed-keys file, SIGHUP reload, or the `--allowed-keys` flag.
