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
amber-store config-user NAME             # required once before creating refs
amber-store ingest NAME DIR              # daemon ingest names its root
amber-store ref create NAME KEY          # name an existing key (e.g. after load)
amber-store ref ls                       # name, key, user, date (tab-separated)
amber-store ref show NAME                # full record as JSON
amber-store ref rm NAME                  # delete the name; objects stay
amber-store ls ref:NAME[@PATH]           # any KEY[/PATH] argument accepts this
```
