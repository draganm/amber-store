# Admin UI for server allowed keys — design

## Problem

The remote server's allowed-keys story is weak: the operator writes an
authorized_keys file by hand before startup and SIGHUPs the process to
reload it. There is no way to inspect or change the allowlist while the
server runs short of shelling into the box.

## Goal

A browser SPA, served by `amber-store serve` itself, where an admin logs
in with a password (provided via environment variable) and can inspect,
add, and remove the SSH keys allowed to talk to the server.

Constraints fixed by the user:

- SPA in **solid-js**.
- UI **embedded in the Go binary** of `cmd/amber-store`.
- Frontend dist generated via **`//go:generate`**.
- Nix flake must carry all tools needed to build it.
- Visuals follow the **Fables for Robots design system** (handoff bundle
  from claude.ai/design: black/white core, teal `#2DB6BE`, blue
  `#358BF2`, Montserrat display / Inter body / JetBrains Mono meta,
  signature black-with-radial-glows backdrop, blocky radii, 1.5px
  borders, no emoji).

## Decisions

### Source of truth stays the allowed-keys file

The `--allowed-keys` file remains authoritative. The admin API edits it
in place (atomic temp-file + rename) and hot-swaps the in-memory list,
exactly like a SIGHUP reload. Hand edits + SIGHUP keep working. Nothing
new to back up; the existing ops story is preserved.

Alternatives rejected:

- *Store allowlist in the diskstore*: invents a second source of truth,
  breaks the documented SIGHUP flow and the `admin` option semantics.
- *Separate admin daemon*: more moving parts for no benefit; the server
  already terminates HTTP.

### New package `internal/allowfile`

A mutexed manager that owns the allowed-keys file:

- `Open(path) (*File, error)` — loads and validates.
- `Current() *allowlist.List` — lock-free (atomic pointer) view used by
  the server's `Allow` callback; replaces the atomic pointer currently
  living in `serve.go`.
- `Reload() error` — SIGHUP path.
- `List() []Key` — parsed entries: authorized_keys line, key type,
  SHA256 fingerprint, comment, admin flag.
- `Add(line string) error` — validates the single line parses as an
  authorized_keys entry, rejects duplicates, appends, rewrites
  atomically, swaps the list.
- `Remove(fingerprint string) error` — drops the matching line(s),
  rewrites, swaps. Unknown fingerprint is an error.

Comments and blank lines in the file are preserved verbatim; edits are
line-level.

### New package `admin`

`admin.New(admin.Config{Password, Keys *allowfile.File, UI fs.FS, Log,
Secure})` returns an `http.Handler` mounted under `/admin/`:

| Route | Behavior |
|---|---|
| `POST /admin/api/login` | `{"password": "…"}`; constant-time compare; ~500ms delay on failure; sets HttpOnly, SameSite=Strict (Secure under TLS) session cookie |
| `POST /admin/api/logout` | drops the session |
| `GET /admin/api/session` | 204 if logged in, 401 otherwise |
| `GET /admin/api/keys` | JSON list from `allowfile.List()` |
| `POST /admin/api/keys` | `{"line": "ssh-ed25519 AAAA… comment"}` or with `"admin": true` prefixing the option |
| `DELETE /admin/api/keys?fingerprint=…` | removes the key |
| `GET /admin/` + assets | embedded SPA, index fallback |

Sessions are random 256-bit tokens in an in-memory map with 12h expiry
— a server restart logs admins out, which is fine. Mutating routes
reject cross-origin requests (Origin/Sec-Fetch-Site check) on top of
SameSite=Strict.

The admin UI is **enabled only when the password is configured** via
`--admin-password` / env `AMBER_ADMIN_PASSWORD` (env is the intended
path). When unset, `/admin/…` does not exist — same surface as today.

### Embedding and generation

- SPA source: `cmd/amber-store/ui/` (vite + vite-plugin-solid).
- `cmd/amber-store/ui.go`: `//go:generate` runs `npm ci && npm run
  build` in `ui/`; `//go:embed all:ui/dist` exposes the dist.
- The built `ui/dist` is committed so `go build` works without node.
- Fonts (Montserrat, Inter, JetBrains Mono via @fontsource) and the
  brand marks are bundled — the admin page must work with no internet.

### UI

Single page, two states (no router):

1. **Login** — the signature brand hero: pure black with teal/blue
   radial glows, oversized white robot-in-gear mark bleeding off the
   right edge, ALL-CAPS tracked eyebrow (`AMBER STORE · ADMIN`), heavy
   Montserrat headline, dark-variant input, primary button.
2. **Allowed keys** — light surface. Sticky translucent header (92%
   white, blur) with the black lockup and a logout ghost button.
   Eyebrow + display headline ("Keys that may talk to this server.").
   Key list as bordered cards/rows: key type + comment in UI font,
   fingerprint in JetBrains Mono, teal-outline `ADMIN` pill badge where
   set, remove action with confirm. Add panel: labeled textarea for the
   authorized_keys line, admin checkbox, help/error text per the inputs
   spec (`#d23` error red). Empty/loading states in brand voice
   (plainspoken, sentence case, no emoji).

### Nix flake

Add `nodejs` to the dev shell next to `go`.

### Testing

- `internal/allowfile`: load/list/add/remove, duplicate and parse
  rejection, atomic persistence, comment preservation, hot-swap.
- `admin`: httptest — login success/failure/delay, session cookie
  required (401), CRUD round-trip mutating the file and the live list,
  SPA serving + fallback, origin check.
- `cmd/amber-store`: serve wiring — admin routes 404 when no password,
  live when set (existing e2e test style).

Frontend is typechecked/built in generate; no JS unit tests — the API
tests cover the contract.

### Docs

`architecture/remote.md` gains a short "admin UI" section describing
the env var and what the UI can do.
