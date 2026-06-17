# Remote Server

`amber-store serve` is a TCP/HTTP(S) sibling of the local daemon: it owns its
own packstore and refs DB through the same packages, but listens on TCP and
requires a signed request for every route. Local daemons are its only clients.
The daemon performs all remote communication and the tree walks that drive it —
walks need many small random-access gets, which belong next to the store,
consistent with the existing division of labor described in
[daemon.md](daemon.md).

A transfer is a single operation per direction. `push` sends every object the
server lacks and then the signed reference; `pull` fetches the reference, then
every object reachable from it, then writes the reference locally. Objects move
before the reference so the no-dangling-refs rule holds on both ends, and either
operation is idempotent — re-running after an interruption transfers only what
is still missing.

## The shape

```
amber-store serve \
  --store DIR \
  --listen ADDR \        # default :8590
  [--identity PATH] \    # server's SSH key: private-key file or .pub via ssh-agent; default: auto-generated in DIR
  [--tls-cert FILE --tls-key FILE]
```

The server creates the store directory on first start. Every route except
`GET /v1/identity` requires a signed request; every response is signed with the
server's identity key.

Client identities are configured at daemon start:

```
amber-store daemon \
  [--remote-key PATH] \      # default signing key for all remotes; default: auto-generated in the store dir
  [--remote-key NAME=PATH] \ # per-remote override (repeatable)
  ...
```

An unencrypted private-key file is loaded directly. A passphrase-protected file
is rejected — load it into the ssh-agent and configure the `.pub` path instead.
A `.pub` path signs via ssh-agent (reusing `internal/sshsign`).

## Identity and trust

**Server side:** allowed client keys live in a Pebble database at
`<store>/allowed-keys`, one entry per SSH public key. An entry may be marked
`admin` for operations keys that bypass ownership checks and are permitted
to delete references. The admin UI is the only way to manage the set; a
fresh server allows nobody and logs a warning until keys are added.

**Admin UI.** Setting `AMBER_ADMIN_PASSWORD` (or `--admin-password`) enables a
browser console at `/admin/` — a solid-js SPA embedded in the binary
(`go generate ./cmd/amber-store` rebuilds it) — where an operator signs in
with that password and inspects, adds, and removes allowed keys. Edits write
straight to the allowed-keys database and take effect immediately. Sessions
are in-memory cookies (12h); when the password is not configured, the
`/admin/` surface does not exist.

**Default identities.** When no key flag is given, each service generates an
ed25519 keypair on first start and persists it in its store directory as
`identity` (0600) and `identity.pub` (0644). The same key is loaded on every
later start; an existing file that cannot be parsed is an error, never
overwritten. `identity.pub` is what an operator pastes into a server's
admin UI to authorize a daemon.

**Client side — trust-on-first-use.** Registering a remote fetches the
server's public key via the unauthenticated `GET /v1/identity` route, prints
its SHA256 fingerprint (ssh-keygen style), and asks the user to confirm:

```sh
amber-store remote add NAME URL
# → "https://store.example.com key fingerprint: SHA256:… (ecdsa-sha2-nistp256)"
# → "Trust this key and register the remote? [y/N]"
```

On confirmation the daemon persists `NAME → {URL, server public key}` in the
store's `remotes` registry. The `--yes` flag skips the prompt for scripted use.

The pinned key is enforced on every response: if a response from the remote
does not carry a valid signature by the pinned key, the daemon aborts with a
"server identity mismatch" error. A changed server key requires explicit
`remote rm` + `remote add`.

## Request/response signing

**Four `Amber-*` headers carry the request signature:**

| Header            | Content                                             |
|-------------------|-----------------------------------------------------|
| `Amber-Public-Key` | client's public key, SSH wire format, base64       |
| `Amber-Timestamp` | ns since the Unix epoch, decimal                    |
| `Amber-Nonce`     | 16 random bytes, base64                             |
| `Amber-Signature` | raw SSHSIG blob, base64                             |

The signed payload is a canonical CBOR map (deterministic, integer keys — the
project-wide convention) of `{method, path+query, timestamp, nonce,
blake3(body)}`, signed as an SSHSIG v1 blob in namespace `amber-store-http`
via `internal/sshsign`. The expensive part — hashing a multi-megabyte pack —
is BLAKE3; SSHSIG's internal SHA-512 only ever covers the ~100-byte canonical
payload, so signing cost is constant regardless of body size.

**Server-side verification order:**

1. Timestamp within the configured window (default ±5 min; set with
   `--auth-window`).
2. Nonce not seen before — an LRU set sized to the window, keyed by
   fingerprint, prevents replay within the window.
3. Signature verifies over the reconstructed canonical payload.
4. Public key present in the allowed-keys database.

The server reads the (size-capped) body fully and verifies before any side
effect: nothing is stored or written on a request that fails auth. Bad or
expired timestamp, replayed nonce, or bad signature → `401`; valid signature by
an unlisted key → `403`.

**Response signing:** every response is signed with the server's identity key
over canonical CBOR `{request nonce, status, blake3(body)}`. Including the
request nonce binds each response to its request, preventing replay or
substitution between requests.

- Non-streaming responses carry the signature in an `Amber-Signature` header.
- The fetch response (`POST /v1/objects/get`) streams the pack while hashing it
  and appends the signature **in-band** at the end of the body: the base64
  SSHSIG followed by its big-endian `uint32` length. The signature is therefore
  part of the body, not an HTTP trailer — a deliberate choice, because reverse
  proxies routinely drop trailers and the client would then reject the response
  as unsigned. The server still streams (it holds one record, never the whole
  pack); the client buffers the response, splits off the trailing signature,
  verifies it over the exact pack bytes against the pinned key, and only then
  parses the pack. A truncated or tampered body fails verification closed.

TLS is orthogonal: signatures give authenticity and integrity even over plain
HTTP; TLS adds confidentiality. Run the server behind a TLS-terminating reverse
proxy by omitting `--tls-cert`/`--tls-key`, or configure TLS directly with
both flags.

**Transport is HTTP/1.1 by choice.** The daemon's client disables HTTP/2:
h2 would multiplex every sync worker onto a single TCP connection whose
upload flow-control window (1 MiB in Go's server) caps push throughput at
roughly window ÷ RTT regardless of bandwidth — ~10 MiB/s at 100 ms — while
parallel HTTP/1.1 connections each get kernel-tuned TCP windows that grow to
the path's bandwidth-delay product. The server still accepts h2 (e.g. from
older daemons when it terminates TLS itself) and raises its h2 receive
windows to just under 4 MiB, the largest value `net/http.HTTP2Config`
documents as valid.

## Wire protocol

All routes are versioned under `/v1`. `GET /v1/identity` is unauthenticated;
every other route requires the four `Amber-*` headers. The server enforces a
hard per-request body cap of 64 MiB.

| Route                        | Body / response                                                       |
|------------------------------|-----------------------------------------------------------------------|
| `GET /v1/identity`           | server public key (SSH wire format); unauthenticated                  |
| `POST /v1/objects/missing`   | concatenated 32-byte keys in → subset the server lacks, same encoding  |
| `POST /v1/objects`           | amberpack pack in → store stats JSON out                              |
| `POST /v1/objects/get`       | concatenated 32-byte keys in → amberpack pack out (in-band sig)       |
| `POST /v1/objects/reachable` | 32-byte root key in → the reachable set as concatenated 32-byte keys  |
| `PUT /v1/refs?name=`         | CBOR ref record in → `204`                                            |
| `GET /v1/refs?name=`         | CBOR ref record out; omit `name=` for NDJSON listing                  |
| `DELETE /v1/refs?name=`      | `204`; admin transport keys only                                      |

**Key-list encoding:** key lists are raw concatenated 32-byte keys — dense,
zero parsing ambiguity. The body length must be a multiple of 32 bytes;
anything else is a `422`.

**Pack encoding:** `POST /v1/objects` and `POST /v1/objects/get` carry an
*amberpack pack* — an 8-byte magic, a sequence of self-describing records (each
a CRC-protected, per-record-zstd CAS object, the same framing packstore writes
on disk), and a final end marker so truncation is detected rather than read as a
clean EOF. The reader validates framing, CRC, and key canonicality; payload
hashes are re-checked in the storage path before anything is stored.

**`POST /v1/objects/get`** pre-checks existence for every requested key and
returns `404` naming the absent keys before the first body byte — following the
project-wide convention of doing the work before streaming, so errors surface
as proper statuses.

**`POST /v1/objects/reachable`** walks the server's tree from the given root key
and returns the whole reachable set as a key list. It walks to completion before
responding (the same do-the-work-before-streaming convention), so an incomplete
tree is a clean `500` rather than a truncated body. Pull uses it to learn the
whole key set up front, replacing a depth-proportional fetch-then-discover walk.

`GET /v1/identity` is the trust bootstrap: its response is signed with the very
key it returns (self-signed). Trust comes from the user confirming the
fingerprint at `remote add`, not from the signature itself.

## Sync algorithms

**Byte-balanced batching.** Both push and pull bin keys into batches whose
estimated size approaches a configurable target (default 60 MiB; `--batch-bytes`
flag) without exceeding it. Push sizes each object by its **stored
(post-compression) length**, read from the local index — the bytes that actually
travel — so the target maps directly to wire size and sits just under the
server's 64 MiB body cap once ~46 B/record of framing is added. Pull, which
knows only the keys, estimates from each key's logical length (a blob's exact
size; a nominal value for nodes); that over-estimate keeps the compressed
response comfortably under the cap. A single object larger than the target gets
its own batch. A key-count cap (8192 keys per batch) prevents pathological
trees of tiny objects from producing arbitrarily large key-list bodies.

Sizes come from the keys themselves: a blob key encodes its exact payload
length. Directory and file-node keys encode cumulative or logical sizes, not
encoded sizes, so they cannot be used directly:

- **Push:** the daemon has every object locally and reads actual stored sizes.
- **Pull:** unknown non-blob objects get a fixed nominal estimate (4 KiB). This
  estimate only affects batch balance, not correctness.

**Push** is phased. The daemon resolves the local reference to its key, reads
and validates the signed record up front, and walks the reachable set (a
parallel breadth-first walk). Then:

1. **Negotiate** the whole set against the server in `--jobs`-parallel chunks
   (`POST /v1/objects/missing`); keys the server already has settle immediately,
   the rest are unioned into the missing set.
2. **Upload** byte-balanced packs of exactly the missing objects
   (`POST /v1/objects`, one amberpack pack each) with `--jobs` parallel workers.
   Collecting the whole missing set before batching coalesces sparse misses into
   full packs rather than one sliver per check.

It then `PUT`s the signed reference. Nothing the server already has crosses the
wire; re-running is idempotent (already-pushed objects drop out in negotiation).

**Pull** is list-then-fetch. The daemon fetches and verifies the reference
(`GET /v1/refs?name=`, checked against the pinned key and the record's own
signature), then asks the server for the whole reachable set
(`POST /v1/objects/reachable`, which the server produces by walking its tree
from the verified key). It fetches every key the local store lacks as
byte-balanced packs in parallel (`POST /v1/objects/get`), re-hashing each object
against its key before storing. A local completeness walk is then the
authoritative gate: any object still missing — a key the server omitted from its
list — is fetched and the gate re-runs, to a fixpoint; in the common case it
walks once and fetches nothing. Only after the gate passes is the verified
reference written into the local refstore (overwrite, consistent with local
mutability). Re-running is idempotent — already-present objects are skipped.

## Reference semantics

The server enforces a five-step validation on `PUT /v1/refs`, in order:

1. **Request authentication** — the transport key must be allowed.
2. **Record validity** — canonical CBOR, name matches the query parameter, name
   and bounds are valid.
3. **Signature required** — the record must carry CBOR keys 4 (signature) and 5
   (public key), and the SSHSIG must verify against the embedded public key. The
   server only stores signed references. The transport key and the signer key are
   independent identities and need not match.
4. **Ownership** — if the name already exists on the server, the new record's
   signer key (CBOR key 5) must equal the existing record's signer key. The
   signing identity owns the name; the same user may update their reference from
   any of their amber instances. A different signer → `403`. Keys marked `admin`
   in the allowlist bypass this check.
5. **No dangling references** — the pointed-to content must be complete: the
   server walks the tree under the referenced key and rejects the record with
   `404` if any reachable object is missing from its store. This is why `push`
   sends objects before the reference within its single operation.

**Deletion is admin-only.** Ownership lives in the reference signature, but a
`DELETE` request carries no record to sign, so the server cannot tie a deletion
to the signing identity. `DELETE /v1/refs?name=` is restricted to transport
keys marked `admin`.

For more on the reference record format and the local reference semantics see
[references.md](references.md).

## Status mapping

| Condition                                                         | Status |
|-------------------------------------------------------------------|--------|
| Bad/expired timestamp, replayed nonce, bad request signature      | `401`  |
| Key not in allowlist; ref ownership violation; non-admin DELETE   | `403`  |
| Absent object in `objects/get`; absent reference                  | `404`  |
| Malformed key list; bad pack stream; hash mismatch; unsigned or invalid ref record | `422` |
| Body over the per-request cap (64 MiB)                            | `413`  |
| Store failures                                                    | `500`  |

Any `401` or `403`, or a pinned-key verification failure, aborts the whole
operation immediately with a clear error. `404` on `objects/get` names the
missing keys. Push and pull operations are idempotent — the recovery story for
any interruption is simply re-running the command.
