# Remote Server Design

A remote server (`amber-store serve`) that local amber daemons reach over
HTTP(S) to push and pull objects and references. Communication is minimal
(only missing objects cross the wire), parallelizable (batched, concurrent
transfers), and authenticated in both directions with SSH keys and
blake3-based request/response signatures.

## 1. Architecture & components

Three roles:

**The remote server — `amber-store serve`** (new subcommand, same binary).
Structurally a sibling of the local daemon: it owns its own diskstore + refs
Pebble DB through the same packages, but listens on TCP with HTTP(S) instead
of a unix socket, and every route requires a signed request. Config at start:

- `--store DIR` — its own store directory
- `--listen ADDR`
- `--allowed-keys FILE` — authorized_keys format; the options field may carry
  `admin` for ops keys; loaded at start, reloaded on SIGHUP
- `--identity PATH` — the server's own SSH key (signs every response)
- `--tls-cert/--tls-key` — optional; omit to run plain HTTP behind a
  TLS-terminating reverse proxy

**The local daemon grows a remote-client role.** The daemon performs all
remote communication and the tree walks that drive it (walks need many small
random-access gets, which belong next to the store — consistent with the
existing division of labor).

- Client identity is given at daemon start and held in memory:
  `--remote-key PATH` (default for all remotes), repeatable
  `--remote-key NAME=PATH` (per-remote override). An unencrypted private-key
  file is loaded directly; a passphrase-protected file is **rejected** with a
  message to use the agent; a `.pub` path signs via ssh-agent (reusing
  `internal/sshsign`).
- Remotes are **registered**, not flag-passed: `amber-store remote add NAME URL`
  fetches the server's public key, prints its SHA256 fingerprint
  (ssh-keygen style), and asks the user to confirm — trust-on-first-use, like
  SSH itself. On confirmation the daemon persists `NAME → {URL, server public
  key}` in `<store-dir>/remotes`, a small file next to `refs/`.
- The pinned server key is enforced: every response from that remote must
  carry a valid signature by the pinned key, or the daemon aborts with a
  "server identity mismatch" error — no fallback, no re-prompt mid-operation.
  A changed server key requires explicit `remote rm` + `remote add`.

**CLI commands stay thin triggers** (object and ref transfer are separate,
composable steps; `REMOTE` may be omitted when exactly one is registered).
In v1 every transfer command operates on **one reference**: `NAME` is always
a reference name, never a bare key.

```sh
amber-store remote add NAME URL      # fetch server key, confirm fingerprint, persist
amber-store remote rm NAME
amber-store remote ls
amber-store remote push-objects [REMOTE] NAME   # objects reachable from local ref NAME
amber-store remote push-ref     [REMOTE] NAME   # the ref record itself
amber-store remote pull-objects [REMOTE] NAME   # objects reachable from *server* ref NAME
amber-store remote pull-ref     [REMOTE] NAME   # the ref record into the local refstore
amber-store remote ls-refs      [REMOTE]
```

`push-objects` resolves `NAME` in the **local** refstore; `pull-objects`
resolves it on the **server** (one `GET /v1/refs?name=`). This keeps the
composition order symmetric and satisfies the no-dangling-refs rule on both
ends: push objects before the ref (the server requires the pointed-to key),
pull objects before the ref (the local daemon requires it too).

Each maps to one new local-daemon route (`/v1/remote/...`); the daemon
forwards progress so the CLI can render it.

## 2. Authentication & signing

**Request signing (client → server).** Every request carries four headers:

| Header | Content |
| --- | --- |
| `Amber-Public-Key` | client's public key, SSH wire format, base64 |
| `Amber-Timestamp` | ns since the Unix epoch |
| `Amber-Nonce` | 16 random bytes, base64 |
| `Amber-Signature` | raw SSHSIG blob, base64 |

The signed payload is a canonical CBOR map (deterministic, integer keys —
the project-wide convention) of `{method, path+query, timestamp, nonce,
blake3(body)}`, signed as SSHSIG v1 with a new namespace `amber-store-http`
via `internal/sshsign`. The expensive part — hashing a multi-megabyte pack —
is blake3; the SSHSIG's internal SHA-512 only ever covers the ~100-byte
canonical payload, so signing cost is constant regardless of body size.
This is why bounded batches matter: the body hash must be known before
sending, and batch bodies are small enough to build, hash, and send.

**Server-side verification, in order:** timestamp within a configurable
window (default ±5 min) → nonce not seen before (an LRU set sized to the
window, per key) → signature verifies over the reconstructed payload →
public key present in the allowlist. The server reads the (size-limited)
body fully and verifies **before** any side effect — nothing is stored or
written on a request that fails auth. Bad/expired timestamp, replayed nonce,
or bad signature → `401`; valid signature but unlisted key → `403`.

**Response signing (server → client).** Every response is signed with the
server's identity key over canonical CBOR `{request nonce, status,
blake3(body)}` — including the request's nonce binds each response to its
request, so a response cannot be replayed or swapped between requests. Small
responses carry the signature in an `Amber-Signature` header; streaming
responses (amberpack downloads) carry it in an `Amber-Signature` **HTTP
trailer**, since the body hash is only known at the end. The client verifies against the pinned key: small responses
verify-then-use; streamed objects are individually self-verifying (re-hashed
against their key) so they may be stored on arrival, and the trailer
signature is checked at batch end — a mismatch aborts the operation
(already-stored valid objects are harmless in a content-addressed bag).

TLS remains orthogonal: signatures give authenticity and integrity even over
plain HTTP; TLS adds confidentiality.

## 3. Wire protocol & sync algorithms

**Server routes** (versioned `/v1`, all signed except `identity`):

| Route | Body / response |
| --- | --- |
| `GET /v1/identity` | server public key (SSH wire format); unauthenticated, used by `remote add` |
| `POST /v1/objects/missing` | concatenated 32-byte keys in → the subset the server lacks, same encoding, out |
| `POST /v1/objects` | amberpack stream in → store stats out (same verify-and-store as the local daemon) |
| `POST /v1/objects/get` | concatenated 32-byte keys in → amberpack stream of those objects out |
| `PUT /v1/refs?name=` | CBOR ref record in → `204` (validation in §4) |
| `GET /v1/refs?name=` | CBOR ref record out |
| `GET /v1/refs` | NDJSON listing, name order (same shape as local) |

`identity` is the trust bootstrap: its response is signed with the very key
it returns (self-signed), so trust comes from the user confirming the
fingerprint at `remote add`, not from the signature.

Key lists are raw concatenated 32-byte keys — dense, zero parsing ambiguity
(body length must be a multiple of 32). `objects/get` pre-checks existence
and returns `404` naming the absent keys **before** the first body byte,
following the existing do-the-work-before-streaming convention.

**Byte-balanced batching.** Batches target a configurable size (default
8 MiB; the server enforces a hard cap of 64 MiB per request). Sizes come
from the keys themselves: a blob key encodes its exact payload length.
Directory keys encode *cumulative subtree* size, so they cannot be used
directly — on **push** the daemon has every object locally and uses the
actual stored size; on **pull** unknown non-blob objects get a fixed nominal
estimate (they are small tree/file records; the estimate only affects batch
balance, not correctness).

**Push-objects:** the daemon resolves the reference name in the local
refstore, walks the reachable set from its key (the existing reachable-keys
walk), bins keys into byte-balanced batches, and N
parallel workers each do two round trips per batch: `objects/missing` with
the batch's keys, then `objects` with an amberpack of exactly the missing
ones. Nothing the server already has crosses the wire. Re-running after an
interruption is naturally resumable — already-pushed objects drop out in the
missing-check.

**Pull-objects:** the daemon resolves the reference name on the server
(`GET /v1/refs?name=`, response verified against the pinned key and the
record's own signature), then runs a BFS from the record's key. A frontier
key already present in
the local store is not re-fetched, but its children are still enqueued, so a
partial local tree completes correctly. Missing keys are binned into
byte-balanced batches; N workers fetch them via `objects/get`; each arriving
object is re-hashed against its key, stored, and — if it is a tree/file
node — parsed so its children join the frontier. The walk ends when the
frontier drains.

**Ref transfer:** `push-ref` reads the record from the local refstore and
PUTs it verbatim; it must be signed — unsigned refs are rejected client-side
with a clear error before any network traffic. `pull-ref` GETs the record,
verifies it is canonical and its signature checks out against the embedded
public key, then writes it into the local refstore (overwrite, consistent
with local mutability).

## 4. Server-side ref semantics, errors, testing

**Ref validation on `PUT /v1/refs` (server), in order:**

1. Request authentication (§2) — the transport key must be allowed.
2. The record is canonical CBOR, the name matches the query, and the usual
   local-PUT rules hold (name validity, bounds).
3. The record **must be signed** (keys 4 and 5 present) and the SSHSIG must
   verify against the embedded public key (key 5). The transport key and the
   signer key are independent identities and need not match.
4. **Ownership:** if the name already exists on the server, the new record's
   signer key (key 5) must equal the existing record's signer key — the
   signing identity owns the name; the same user may update their ref from
   any of their ambers. A different signer → `403`. Keys marked `admin` in
   the allowlist bypass ownership (transport-level override for ops/lockout).
5. The pointed-to key must exist in the server's store (no dangling refs,
   same as local) — which enforces the natural order: push objects, then the
   ref.

**Remote ref deletion** is admin-only in v1: ownership lives in the ref
signature, but a DELETE request carries no record to sign, so the server
cannot tie a deletion to the signing identity. Rather than invent a signed
tombstone, v1 restricts `DELETE /v1/refs?name=` to transport keys marked
`admin`. (A signed-tombstone scheme can be added later if needed.)

**Error mapping** (extends the existing conventions):

| Condition | Status |
| --- | --- |
| bad/expired timestamp, replayed nonce, bad request signature | `401` |
| key not in allowlist; ref ownership violation; non-admin DELETE | `403` |
| absent object in `objects/get`; absent ref | `404` |
| malformed key list, bad pack stream, hash mismatch, unsigned/invalid ref record | `422` |
| body over the per-request cap | `413` |
| store failures | `500` |

Client-side: any `401/403` or a pinned-key verification failure aborts the
whole operation immediately with a clear error; `404` on pull names the
missing keys. Push/pull operations are idempotent, so the recovery story for
any interruption is simply re-running the command.

**Testing:**

- Unit: byte-balanced binning (blob sizes from keys, estimates for nodes);
  canonical request/response payload encoding; allowlist parsing (incl.
  `admin`); nonce-replay LRU.
- Signing round trips: request signing/verification with file keys and the
  agent (extending the existing `sshsign` test patterns, incl.
  `ssh-keygen -Y` cross-checks where applicable).
- Server handler tests over `httptest` with real diskstore/refstore: auth
  rejection matrix (bad sig, stale timestamp, replayed nonce, unlisted key),
  missing-subset correctness, pack upload verify-and-store, ref validation
  order incl. ownership conflicts and admin override.
- End-to-end: two local daemons + one server in-process; ingest on A,
  push-objects + push-ref, pull-ref + pull-objects on B, byte-identical
  `restore` output; interrupted-push resume; partial local tree completion
  on pull; server identity mismatch (wrong pinned key) aborts.
