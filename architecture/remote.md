# Remote Server

`amber-store serve` is a TCP/HTTP(S) sibling of the local daemon: it owns its
own diskstore and refs DB through the same packages, but listens on TCP and
requires a signed request for every route. Local daemons are its only clients.
The daemon performs all remote communication and the tree walks that drive it —
walks need many small random-access gets, which belong next to the store,
consistent with the existing division of labor described in
[daemon.md](daemon.md).

Transfer is decomposed into separate, composable steps: objects first, then the
reference. This ordering satisfies the no-dangling-refs rule on both ends and
lets each step be re-run independently on interruption.

## The shape

```
amber-store serve \
  --store DIR \
  --listen ADDR \        # default :8590
  --allowed-keys FILE \  # authorized_keys format; 'admin' option marks ops keys; reloaded on SIGHUP
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

**Server side:** the `--allowed-keys` file is in authorized_keys format. The
`options` field may carry `admin` for operations keys that bypass ownership
checks and are permitted to delete references. The file is loaded at start and
reloaded on SIGHUP without restarting the server.

**Default identities.** When no key flag is given, each service generates an
ed25519 keypair on first start and persists it in its store directory as
`identity` (0600) and `identity.pub` (0644). The same key is loaded on every
later start; an existing file that cannot be parsed is an error, never
overwritten. `identity.pub` is what an operator copies into a server's
`--allowed-keys` file to authorize a daemon.

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
4. Public key present in the `--allowed-keys` list.

The server reads the (size-capped) body fully and verifies before any side
effect: nothing is stored or written on a request that fails auth. Bad or
expired timestamp, replayed nonce, or bad signature → `401`; valid signature by
an unlisted key → `403`.

**Response signing:** every response is signed with the server's identity key
over canonical CBOR `{request nonce, status, blake3(body)}`. Including the
request nonce binds each response to its request, preventing replay or
substitution between requests.

- Non-streaming responses carry the signature in an `Amber-Signature` header.
- Streaming responses (`POST /v1/objects/get`) carry the signature in an
  `Amber-Signature` HTTP **trailer**, because the body hash is only known at
  the end. Streamed objects are individually self-verifying (re-hashed against
  their key) and may be stored on arrival; the trailer signature is checked at
  batch end — a mismatch aborts the operation. Already-stored valid objects are
  harmless in a content-addressed store.

TLS is orthogonal: signatures give authenticity and integrity even over plain
HTTP; TLS adds confidentiality. Run the server behind a TLS-terminating reverse
proxy by omitting `--tls-cert`/`--tls-key`, or configure TLS directly with
both flags.

## Wire protocol

All routes are versioned under `/v1`. `GET /v1/identity` is unauthenticated;
every other route requires the four `Amber-*` headers. The server enforces a
hard per-request body cap of 64 MiB.

| Route                     | Body / response                                                      |
|---------------------------|----------------------------------------------------------------------|
| `GET /v1/identity`        | server public key (SSH wire format); unauthenticated                 |
| `POST /v1/objects/missing`| concatenated 32-byte keys in → subset the server lacks, same encoding |
| `POST /v1/objects`        | amberpack stream in → store stats JSON out                           |
| `POST /v1/objects/get`    | concatenated 32-byte keys in → amberpack stream out (trailer sig)    |
| `PUT /v1/refs?name=`      | CBOR ref record in → `204`                                           |
| `GET /v1/refs?name=`      | CBOR ref record out; omit `name=` for NDJSON listing                 |
| `DELETE /v1/refs?name=`   | `204`; admin transport keys only                                     |

**Key-list encoding:** key lists are raw concatenated 32-byte keys — dense,
zero parsing ambiguity. The body length must be a multiple of 32 bytes;
anything else is a `422`.

**`POST /v1/objects/get`** pre-checks existence for every requested key and
returns `404` naming the absent keys before the first body byte — following the
project-wide convention of doing the work before streaming, so errors surface
as proper statuses.

`GET /v1/identity` is the trust bootstrap: its response is signed with the very
key it returns (self-signed). Trust comes from the user confirming the
fingerprint at `remote add`, not from the signature itself.

## Sync algorithms

**Byte-balanced batching.** Both push and pull bin keys into batches whose
estimated payload size approaches a configurable target (default 8 MiB;
`--batch-bytes` flag) without exceeding it. A single object larger than the
target gets its own batch. A key-count cap (8192 keys per batch) prevents
pathological trees of tiny objects from producing arbitrarily large key-list
bodies.

Sizes come from the keys themselves: a blob key encodes its exact payload
length. Directory and file-node keys encode cumulative or logical sizes, not
encoded sizes, so they cannot be used directly:

- **Push:** the daemon has every object locally and reads actual stored sizes.
- **Pull:** unknown non-blob objects get a fixed nominal estimate (4 KiB). This
  estimate only affects batch balance, not correctness.

**Push-objects:** the daemon resolves the local reference name to its key,
walks the reachable set (the existing reachable-keys walk), bins keys into
byte-balanced batches, and N parallel workers (default 4; `--jobs` flag) each
do two round trips per batch:

1. `POST /v1/objects/missing` with the batch's key list.
2. `POST /v1/objects` with an amberpack of exactly the missing ones.

Nothing the server already has crosses the wire. Re-running after an
interruption is naturally idempotent — already-pushed objects drop out in the
missing-check.

**Pull-objects:** the daemon resolves the reference name on the server
(`GET /v1/refs?name=`, response verified against the pinned key and the record's
own signature), then runs a BFS from the record's key:

- A frontier key already present in the local store is not re-fetched, but its
  children are still enqueued — so a partial local tree completes correctly.
- Missing keys are binned into byte-balanced batches; N workers fetch them via
  `POST /v1/objects/get`.
- Each arriving object is re-hashed against its key, stored, and — if it is a
  tree or file node — parsed so its children join the frontier.
- The walk ends when the frontier drains.

**Ref transfer:** `push-ref` reads the record from the local refstore and PUTs
it verbatim; the daemon rejects unsigned refs client-side before any network
traffic. `pull-ref` GETs the record from the server, verifies it is canonical
and its signature checks out against the embedded public key, then writes it
into the local refstore (overwrite, consistent with local mutability).

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
5. **No dangling references** — the pointed-to key must exist in the server's
   store. This enforces the natural transfer order: push objects, then the
   reference.

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
