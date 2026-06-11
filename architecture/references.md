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
| 4 | signature | byte string, omitted when absent | raw SSHSIG blob (see below) |
| 5 | public_key | byte string, omitted when absent | signer's public key, SSH wire format |

The **signature payload** is the deterministic encoding of the record without
key 4 — the canonical bytes of `{0,1,2,3,5}` — so the signature covers the
signer's public key. When the user config carries a signing key, clients set
key 5 to the signer's public key (SSH wire format) and store an **SSHSIG v1**
signature (the `ssh-keygen -Y sign` / git SSH-signing format) over the
payload in key 4: namespace `amber-store-ref`, SHA-512 message hash, raw
binary blob (not PEM-armored). The signing key may be a private-key file
(passphrase-protected ones prompt on the terminal) or a `.pub` resolved
through the ssh-agent. Signing failures abort the command — a configured key
never silently yields an unsigned reference. `ref verify-signature` checks a
stored signature client-side against the recorded public key (integrity
only; no trust model yet). The daemon stores both fields opaquely.

**Name rules:** 1–1024 bytes of valid UTF-8; no `@` (the ref/path separator)
and no control characters (< 0x20 or 0x7F). `/` is allowed
(`backups/2026/06`) but has no structural meaning — names are opaque strings,
compared whole.

**Field bounds:** the user string is limited to 1024 bytes (same character
rules as names, but `@` is allowed for email-style identities); a signature
may be at most 64 KiB. Decoders reject records whose bytes are not the
canonical deterministic encoding.

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
| `PUT /v1/refs?name=N` | CBOR record in (≤1 MiB) → `204` | missing name, bad CBOR, record/query name mismatch, invalid name, non-canonical key → `422`; pointed-to key absent → `404` |
| `GET /v1/refs?name=N` | CBOR record out (`application/cbor`) | absent → `404` |
| `GET /v1/refs` | NDJSON: `name`, `key` (hex), `user`, `created_at` (RFC 3339), `signed`; name order | — |
| `DELETE /v1/refs?name=N` | `204` | missing name → `422`; absent → `404` |

`PUT` requires the pointed-to key to exist in the store — no dangling
references. Resolution of `ref:NAME@PATH` is client-side: one
`GET /v1/refs?name=`, then the ordinary key routes.

## CLI

```sh
amber-store config-user NAME [--signing-key PATH]   # required once before creating refs; a key signs them
amber-store ingest NAME DIR              # daemon ingest names its root
amber-store ref create NAME KEY          # name an existing key (e.g. after load)
amber-store ref ls                       # name, key, user, date (tab-separated)
amber-store ref show NAME                # full record as JSON
amber-store ref verify-signature NAME    # check the stored signature (integrity only)
amber-store ref rm NAME                  # delete the name; objects stay
amber-store ls ref:NAME[@PATH]           # any KEY[/PATH] argument accepts this
```

## References on a remote server

The remote server adds several constraints on top of the local rules (see
[remote.md](remote.md) for the full protocol):

**Signed records only.** The server rejects any `PUT /v1/refs` whose record
lacks CBOR keys 4 (signature) and 5 (public key), or whose signature does not
verify against the embedded key. The local daemon never allows an unsigned ref
to go over the wire — `push-ref` fails client-side with a clear error before
any network traffic.

**Signer-key ownership.** The signer key (CBOR key 5) owns the reference name
on the server. An existing name may only be overwritten by a record carrying the
same signer key. A different signer → `403`. Transport keys marked `admin` in
the server's allowed-keys database bypass this check — they are the operations
override for lockout or migration.

**Admin-only deletion.** A `DELETE` request carries no record, so the server
cannot verify the signing identity of the caller. Deletion is therefore
restricted to `admin` transport keys.

**No dangling references.** The pointed-to content must be complete in the
server's store before the `PUT` is accepted: the server walks the tree under
the referenced key and rejects the record if any reachable object is missing.
This enforces the natural transfer order: `remote push-objects` (or `remote
pull-objects` locally) before `remote push-ref` (or `remote pull-ref` locally).
