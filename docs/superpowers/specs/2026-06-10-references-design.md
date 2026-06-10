# References — named pointers to keys

**Status:** design approved 2026-06-10.

## Purpose

Give roots names. Today the pack stream deliberately carries no root key and
"naming or persisting roots is the caller's concern"
([daemon.md](../../../architecture/daemon.md)); this design moves that concern
into the store. A **reference** is a global name pointing at a key (file or
directory), recorded with who created it and when, with room for a signature.
Ingesting against a daemon now always names its root.

## The reference record

A new top-level package `reference` defines the record. Encoding follows the
existing `fstree.Entry` convention: canonical CBOR map with integer keys,
RFC 8949 §4.2 core-deterministic mode.

| CBOR key | Field | CBOR type | Notes |
| --- | --- | --- | --- |
| 0 | `Name` | text string | the global reference name (rules below) |
| 1 | `Key` | 32-byte byte string | the pointed-to key; must be canonical (`key.Validate`); may be any object type (file or directory) |
| 2 | `User` | text string | creator, from the user config |
| 3 | `CreatedAt` | int64 | ns since the Unix epoch (same convention as `Entry.Mtime`) |
| 4 | `Signature` | byte string, omitted when absent | opaque bytes; reserved |

**Signature payload.** The bytes a signature runs over are the deterministic
CBOR encoding of the record **without key 4** — the canonical encoding of
`{0: name, 1: key, 2: user, 3: createdAt}`. The package exposes
`SignaturePayload() ([]byte, error)` so future signers and verifiers share one
definition. Nothing in this design produces or verifies signatures; the daemon
stores the field opaquely.

**Name rules**, validated by `reference.ValidateName`, enforced in the CLI
(fail fast) and in the daemon (authoritative, `422`):

- 1–1024 bytes, valid UTF-8;
- must not contain `@` (the ref/path separator) or control characters
  (< 0x20, 0x7F).

`/` is allowed, enabling hierarchical names (`backups/2026/06`); it has no
structural meaning — names are opaque strings, compared whole.

Everything else is allowed. Names are global to a store; there is exactly one
record per name.

**Mutability.** References are overwritable: a `PUT` for an existing name
replaces the record unconditionally (last writer wins, new user/date/signature
included). There is no history.

## Daemon storage

The daemon opens a **second Pebble DB** at `<store-dir>/refs/`, next to the
object store's `db/` (Pebble v2 is already a dependency; the refs DB uses a
small cache, not the diskstore's 1 GiB one). DB key = reference name bytes;
value = the CBOR record verbatim. Writes use `pebble.Sync` when the daemon
runs with `--sync`, `pebble.NoSync` otherwise — the same durability policy as
objects. Listing is a plain full iterator scan; names come out in lexicographic
byte order, which is the documented ordering. The DB lives inside the store
directory, so a store stays one self-contained directory owned exclusively by
the daemon.

## Wire protocol

Four routes join the existing `/v1` handler. The name travels as a `?name=`
query parameter, not a path segment: names may contain `/`, and
`http.ServeMux` path-cleaning would mangle names with `..`, `.`, or empty
segments. Standard query escaping handles every allowed character; this also
matches the existing `?path=` convention. A `GET`/`PUT`/`DELETE` to `/v1/refs`
without a `name` parameter is the list operation (`GET`) or `422` (others).

| Route | Body / response | Errors |
| --- | --- | --- |
| `PUT /v1/refs?name=N` | CBOR record in → `204` | missing `name`, undecodable record, record name ≠ query name, invalid name, non-canonical key → `422`; pointed-to key absent from store → `404` |
| `GET /v1/refs?name=N` | CBOR record out, `application/cbor` | absent → `404` |
| `GET /v1/refs` | NDJSON, one ref per line: `name`, `key` (hex), `user`, `created_at` (RFC 3339), `signed` (bool); name order | — |
| `DELETE /v1/refs?name=N` | `204` | missing `name` → `422`; absent → `404` |

`PUT` validation order: decode → name rules → name match → key canonical →
`store.Has(key)`. **No dangling references**: the pointed-to key must already
be in the store (ingest creates the ref after the upload; `ref create` targets
keys already present). Store failures are `500`, consistent with existing
routes.

**Resolution is client-side.** A command given `ref:NAME@PATH` does one
`GET /v1/refs?name=`, extracts the key, and calls the existing key routes
unchanged. None of the four existing routes change. The resolve-then-read pair
is not atomic; an overwrite between the two reads a complete, valid tree under
the old key, which is acceptable.

## User config

`amber-store config-user NAME` writes `{"user": "NAME"}` (JSON) to
`os.UserConfigDir()/amber-store/config.json`
(`~/Library/Application Support/amber-store/` on macOS,
`~/.config/amber-store/` on Linux). `$AMBER_STORE_CONFIG` overrides the full
file path (used by tests). JSON leaves room for a future signing key.

Commands that **create** references — daemon-mode `ingest` and `ref create` —
load the config first and refuse with
`no user configured — run 'amber-store config-user <name>' first` when it is
missing. Read commands and `ref ls`/`show`/`rm` do not need it.

## Command surface

| Command | Behaviour |
| --- | --- |
| `amber-store config-user NAME` | Write the user config. |
| `amber-store ingest NAME DIR` | Daemon mode now **requires** a reference name. Check config (before walking anything), stream the pack exactly as today, then `PUT /v1/refs?name=NAME` with the root key, configured user, current time. Root key still printed. |
| `amber-store ingest -o FILE DIR` | Unchanged: no name, no reference (no daemon involved). Name the tree later with `ref create` after `load`. |
| `amber-store ref create NAME KEY` | Point NAME at an existing key (create or overwrite). Needs the user config. |
| `amber-store ref ls` | One line per ref: name, key, user, date. |
| `amber-store ref show NAME` | Full record as JSON; signature rendered as hex when present. |
| `amber-store ref rm NAME` | Delete the name. Objects stay in the store. |

**Addressing.** Everywhere a command accepts `KEY[/PATH]` today (`ls`, `dump`,
`restore`, `content-keys`) it also accepts `ref:NAME[@PATH]`, e.g.
`amber-store ls ref:backups@home/dragan`. The parser recognises the `ref:`
prefix, splits on the **first** `@` (unambiguous — `@` is banned in names),
resolves the name daemon-side, and proceeds with the resolved key. Bare
hex-key syntax is unchanged. An empty name after `ref:`, or a name failing
validation, is a client-side usage error.

## Error handling

- Missing user config aborts ingest before any filesystem walk.
- If the ref `PUT` fails after a successful ingest upload, the error message
  includes the root key and the exact `amber-store ref create NAME KEY`
  command to retry — uploaded objects are not wasted.
- `ref:NAME` resolution failure surfaces as `reference "NAME": reference not
  found`.
- Daemon-side name validation is authoritative; client-side validation exists
  only to fail fast with friendlier messages.

## Testing

Follows existing patterns (table-driven; `daemon_test.go`-style handler tests;
`e2e_test.go`-style CLI tests):

- **`reference`**: encode/decode round-trip; determinism (two encodes are
  byte-identical); `SignaturePayload` excludes the signature field and matches
  the encoding of an unsigned record; name-validation table (valid incl. `/`,
  `..` segments, empty, too long, `@`, control chars, non-UTF-8).
- **`daemon`**: PUT/GET/DELETE happy paths; overwrite replaces the record;
  names with `/`, `..`, and empty segments round-trip through the query
  parameter; `404` on missing ref and on dangling key; `422` on bad name,
  missing `name` parameter, mismatched query name, bad CBOR, non-canonical
  key; NDJSON listing in name order; refs
  survive a daemon restart (close and reopen the Pebble DB).
- **CLI e2e**: `config-user` then `ingest NAME DIR` creates the ref and the
  resolved key equals the printed root; ingest without config refuses;
  `ls ref:NAME@sub` output equals `ls KEY/sub`; `ref create`/`ls`/`show`/`rm`
  round-trip; offline `ingest -o` + `load` + `ref create` flow.

## Documentation updates

- New `architecture/references.md`: record layout, name rules, signature
  payload, Pebble storage, routes.
- `architecture/daemon.md`: add the four routes to the route table; replace
  "naming or persisting roots is the caller's concern" with a pointer to
  references.
- `README.md`: updated usage (`ingest NAME DIR`, `ref` subcommands,
  `ref:NAME@PATH` addressing, `config-user`).

## Out of scope

- Signature creation and verification (field is reserved, stored opaquely).
- References as GC roots for `reachable` walks.
- Reference history / audit log (overwrite discards the old record).
- Daemon-side resolution of `ref:` specs in read routes.
