# Admin ref browser — design

## Problem

The admin UI manages allowed keys and nothing else. An operator who
wants to know what a server actually holds — which refs exist, what
tree a ref points to, what a file contains — has to drive the signed
`/v1` protocol from a shell with an authorized SSH key. There is no way
to inspect content from the browser.

## Goal

A read-only ref browser inside the existing admin SPA:

- a list of all refs on the server;
- clicking a ref opens a directory browser (breadcrumbs, clickable
  subdirectories) or, for a ref that points to a file, the file itself;
- files can be viewed in the browser or downloaded;
- directories can be downloaded as `.tar` or `.tar.gz`.

Constraints fixed by the user:

- Implemented as new views in the existing **solid-js SPA** plus JSON
  endpoints in the `admin` package (not server-rendered pages).
- **View** opens the raw file in a new tab with a sniffed Content-Type
  — the browser renders text, images, PDFs natively.
- Directory listings show **full metadata** (name, kind, size, mode,
  mtime) like `ls -l`.
- Directories can hold **100k+ entries**, so listings are paginated
  server-side; a click never pulls a whole huge directory.
- Both **`.tar` and `.tar.gz`** downloads are offered.
- **Every new endpoint is authenticated** with the existing admin
  session (see Authentication).

## Decisions

### Authentication

All new routes register through the same `h.authed(...)` middleware
that guards the keys API: no live session cookie, no response — a 401
JSON error, never content. There are no exceptions and no anonymous
"share link" mode.

View/Download/tar actions are plain `<a href>` navigations from the
SPA. They authenticate because the session cookie (path `/admin`,
HttpOnly, SameSite=Strict, Secure under TLS) accompanies same-site
navigations, including new tabs opened from the admin page. A
cross-site hotlink to `/admin/api/raw?...` gets a 401: Strict cookies
are not sent cross-site. All new endpoints are read-only GETs, so the
existing CSRF posture (SameSite=Strict + Origin check on mutating
routes) is unchanged.

Tests assert a 401 without a session for every new endpoint.

### New fstree primitives (large-directory support)

Directory prolly trees already store, in every `DirNode` pair, the
greatest entry name of the child subtree (`SepName`). Two new functions
exploit that:

- `LookupEntry(dir key.Key, name []byte, get Getter) (Entry, error)` —
  O(log n): binary-search `DirNode` pairs for the first
  `SepName >= name`, descend, scan the one `DirLeaf`. Wraps
  `ErrNotFound`.
- `ListEntries(dir key.Key, after []byte, limit int, get Getter)
  ([]Entry, more bool, err error)` — in-order walk that skips subtrees
  whose `SepName <= after`, collects entries with `name > after`,
  stops at `limit`, reports whether more follow. Touches
  O(log n + limit) objects; this is the pagination cursor.

Targeted improvement while here: `ResolvePath` currently calls
`CollectEntries` — decoding entire directories — per path component.
It is reimplemented on `LookupEntry` (same signature and behavior).
A new `ResolveEntry(root key.Key, path string, get Getter) (*Entry,
error)` resolves a path to its final entry of any kind (file, dir,
symlink, …); intermediate components must be directories; the empty
path returns nil — the ref root has no entry metadata of its own.

### Admin API

Ref names may contain slashes (only `@` and control characters are
forbidden), so refs and paths travel as query parameters, never as URL
path segments. All endpoints are GET, under `/admin/api/`, authed.

| Route | Behavior |
| --- | --- |
| `GET /admin/api/refs` | `{"refs":[{name, key, user, created_at, kind}]}` sorted by name (refstore order). `kind` from the key's type: DirLeaf/DirNode → `"dir"`, Blob/FileNode → `"file"`, anything else → `"invalid"`. Records that fail `reference.Decode` also appear as `kind:"invalid"` so the operator can see them. |
| `GET /admin/api/tree?ref=N&path=P&after=A&limit=L` | Directory: `{"kind":"dir","entries":[…],"more":bool}`, plus `"next"` (the raw last entry name, percent-encoded, ready to append as `&after=`) when `more` is true — JSON names are lossy for non-UTF-8 bytes, so clients page with `"next"`, never with the displayed name; entries carry `name, kind (dir/file/symlink/fifo/char/block/socket), size, mode, mtime, target` (symlinks). File path: `{"kind":"file","stat":{…}}`. `limit` defaults to 500, clamped to 1..1000; `after` is the last entry name of the previous page. Sizes come from the content key's encoded length; mtime is ns since epoch, formatted client-side. |
| `GET /admin/api/raw?ref=N&path=P[&dl=1]` | Streams file bytes (FileNode descent, Blob concatenation). Content-Type from the filename extension, else sniffed from the first 512 bytes. `Content-Length` from the key length. Without `dl`: `Content-Disposition: inline` (the View action); with `dl=1`: `attachment` with a sanitized filename. Regular files only — anything else is a 400. |

A ref may point directly at a file (its key is Blob/FileNode). Then
the empty path addresses that file: `tree` returns `kind:"file"` with
a stat derived from the key alone (size; no mode/mtime — a bare file
key carries no entry metadata), and `raw` streams it, using the last
segment of the ref name as the filename. Likewise `archive` with an
empty path on a dir ref archives the whole tree — the common case.
The file stat for a non-empty path carries `name, kind, size, mode,
mtime` from its directory entry.
| `GET /admin/api/archive?ref=N&path=P&format=tar\|tgz` | Resolves to a directory key, streams `tarexport.Write` — through a `gzip.Writer` for `tgz`. Content-Type `application/x-tar` / `application/gzip`; attachment filename from the path basename (or ref name) plus `.tar` / `.tar.gz`. |

Wiring: `admin.Config` gains `Objects` and `Refs` fields, typed as
small interfaces declared in `admin` (`Get(key.Key) ([]byte, error)`;
`All() ([]refstore.Record, error)`). `serve.go` passes the existing
diskstore and refstore. Both are required; `admin.New` errors if
either is nil.

### Untrusted content on the admin origin

Stored files are untrusted. Rendering a stored HTML/SVG file inline on
the admin origin would otherwise let it script against the admin
session. Every `raw` response therefore carries
`Content-Security-Policy: sandbox` (renders, but no scripts, no same-
origin access) and `X-Content-Type-Options: nosniff`. Archive
responses are attachments and carry `nosniff` as well.

### Error handling

- Unknown ref, unknown path, or an object missing from the store
  (partially synced tree): 404 with a JSON `{error}` saying which.
- Bad parameters, wrong entry kind (raw on a symlink, archive on a
  file): 400.
- Streaming routes resolve the target key fully before writing
  headers, so resolution errors still produce proper statuses. A fetch
  failure mid-stream cannot change the status anymore; the handler
  aborts the connection and logs.
- Entry names are bytes and may be invalid UTF-8; JSON cannot carry
  them faithfully. Such entries are listed with a lossily decoded name
  and marked unnavigable (`"raw_name_invalid":true`) rather than
  hidden; the tar download still preserves their exact names, so the
  data remains reachable. Pagination stays sound for such names via
  the `"next"` cursor.

### UI

Hash routing inside the signed-in state — no router dependency, just
`location.hash`:

- `#/keys` — the existing keys console, now one of two header tabs.
- `#/refs` — **RefsPage**: table of refs (name, kind, user, created)
  with a client-side filter box. A dir ref links into the browser; a
  file ref offers View / Download directly.
- `#/refs/<ref>/<path>` (URL-encoded components) — **BrowserPage**:
  clickable breadcrumbs (ref name + path components), `ls -l`-style
  table (name, kind, human-formatted size, octal mode, local mtime;
  symlink targets shown, not clickable), **Load more** when
  `more=true` (cursor = last entry name), and per-directory
  **Download .tar** / **.tar.gz** buttons. File View opens in a new
  tab (`target=_blank rel=noopener`); View/Download/tar are plain
  links — the session cookie does the auth.

`api.js` gains `listRefs()` and `listTree(ref, path, after, limit)`.
A 401 anywhere signs the user out, as today. Styling reuses the
existing `app.css` classes (Fables for Robots design system: light
console surface, bordered rows, mono for keys/sizes, no emoji).

The built `ui/dist` is committed; the work ends with
`go generate ./cmd/amber-store` and committing the rebuilt bundle.

### Testing

- `fstree`: `LookupEntry` / `ListEntries` against directories large
  enough to force multi-level `DirNode` trees (existing builders):
  hits, misses, first/last names, pages concatenating to exactly
  `CollectEntries` output for various `limit`s; `ResolvePath`
  equivalence with the previous implementation.
- `admin`: httptest against a temp diskstore + refstore seeded with a
  small ingested tree — refs listing (including an invalid record),
  tree listing + pagination cursor, file stat, raw content + headers
  (Content-Type, CSP sandbox, nosniff, inline vs attachment), archive
  round-trip for both formats (read the tar back, compare entries and
  bytes), 404/400 cases, and **401 without a session on every new
  endpoint**.
- UI: built by `go generate` (vite); no JS unit tests — the API tests
  cover the contract, manual browser verification against a seeded
  store.

## Out of scope

- Mutating anything (deleting refs, editing trees) from the browser.
- Search across trees, file diffing, content previews beyond what the
  browser renders natively.
- Range requests / resumable downloads on `raw` and `archive`.
- Pagination of the refs list itself (records are small; the full list
  is fine in one response, as the `/v1/refs` NDJSON listing already
  assumes).
