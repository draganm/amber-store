# FUSE Mount

This document describes `amber-store fuse`: a command that mounts a tag (or any
directory key) as a **read-only filesystem**, reconstructing each file the
moment it is opened and serving its bytes through **kernel passthrough** so reads
bypass the daemon entirely. It is **Linux-only** — on every other platform the
command is not registered at all.

The object model lives in [types.md](types.md) and [fstree.md](fstree.md); the
daemon/CLI split and the wire protocol in [daemon.md](daemon.md); references
("tags") in [references.md](references.md).

## The shape

Like every command other than `daemon` and `serve`, `fuse` is a
[client of the daemon](daemon.md#the-shape): it opens no store directory and
reaches the CAS only over the unix socket. It differs in one respect — it is
**long-lived**. Where `ls` or `restore` perform one operation and exit, `fuse`
mounts, then stays running to service kernel requests until it is unmounted
(Ctrl-C, `SIGTERM`, or `fusermount -u`).

```text
open()/read() ── kernel ──▶ amber-store fuse ──── unix socket ────▶ daemon
                   │           (reconstructs into a memfd at open)    (owns the store)
                   └── reads go straight to the memfd via passthrough, skipping the daemon
```

The mount is read-only by construction: the immutable, content-addressed store
has no notion of writing back. Write-mode opens are refused with `EROFS`, and no
mutating operations (create, unlink, rename, setattr, write) are implemented.

## The read path: reconstruct at open, passthrough thereafter

When the kernel opens a regular file, the command:

1. Resolves the file's **content key** — already known from the directory
   listing (`ls` returns each entry's key), so no path resolution is needed.
2. Fetches the whole reconstructed file from the daemon over
   `GET /v1/file/{key}` (see [below](#the-daemon-route)) and writes it into an
   anonymous, RAM-backed **`memfd`** (`memfd_create(2)`).
3. Hands the kernel that descriptor as a FUSE **passthrough backing file**. From
   then on the kernel serves every `read` and every `mmap` against the memfd
   directly — the `fuse` process is never woken for I/O.

The cost is paid once, at open: a file is fully materialized (and fully resident
in RAM) before its first byte is read. The payoff is that reads run at native
speed with zero userspace hops — ideal for files read in full, read repeatedly,
or `mmap`ed (e.g. executing a binary out of the mount).

### Deduplication by content key

Backing files are cached **by content key**, not by path. Because the store is
content-addressed, two paths with identical bytes share a single memfd, and
re-opening a file reuses the materialization already in memory. Each cache entry
is reference-counted by the number of open handles; the kernel registers one
backing file per inode, duplicating the descriptor, so several inodes can safely
share one memfd.

A closed file's memfd is freed on its last release unless an idle budget is set
with `--cache-bytes N`, which keeps up to `N` bytes of recently-closed files
resident (LRU eviction) so re-opens stay free. `--max-file-size N` refuses to
open regular files larger than `N` bytes (`EFBIG`), guarding against
materializing a huge file into RAM; the default, `0`, is unlimited.

## Directories, metadata, symlinks

Directory structure is served **lazily**: a directory is listed via
`ls` (`GET /v1/ls/{key}`) only when the kernel first looks inside it, and the
result is cached for the mount's lifetime — safe because a directory object is
immutable, so its listing never changes. This scales to the prolly-tree's very
large directories ([fstree.md](fstree.md)): a 100K-entry directory costs one
listing on first touch, not an up-front walk of the whole tree.

All filesystem metadata — mode, uid, gid, mtime, size — comes straight from the
directory entry the store recorded ([types.md](types.md)). Symlinks return their
stored target. Device nodes expose their `[major, minor]`. Sockets, which the
store does not model, do not appear.

## Passthrough requires privilege

FUSE passthrough has a hard prerequisite beyond a recent kernel (≥ 6.9): the
process that registers a backing descriptor must hold **`CAP_SYS_ADMIN`**, and
the kernel checks it against the **initial** user namespace — a `CAP_SYS_ADMIN`
granted only inside a user namespace (e.g. `unshare -Ur`) does **not** satisfy
it. Registration by an unprivileged process fails with `EPERM`.

This does not break the mount. When passthrough cannot be set up — old kernel,
kernel without the capability advertised, or missing `CAP_SYS_ADMIN` — the
command **falls back** to serving reads itself, copying out of the same memfd
that was materialized at open. Reads are still RAM-fast and never touch the
daemon; they just take one userspace hop instead of zero. The command prints a
one-line hint at mount time when it detects passthrough will be unavailable.

To get true passthrough without running everything as root, grant the capability
to the binary once:

```sh
sudo setcap cap_sys_admin+ep "$(command -v amber-store)"
```

## The daemon route

The mount needs a way to read **one file's bytes** — something the read-side API
([daemon.md](daemon.md#wire-protocol)) previously lacked, since `tar` works only
on whole directory trees. `fuse` adds a single route:

| Route                | Body / response                                              |
|----------------------|-------------------------------------------------------------|
| `GET /v1/file/{key}` | the reconstructed bytes of one regular file (octet-stream)  |

`{key}` is a `Blob` or `FileNode` content key. The daemon walks the file's
content tree — concatenating `Blob` leaves under any `FileNode` index levels,
the same descent `tar` uses — and streams the result. `Content-Length` is set
from the key's encoded length (the key carries the exact byte count), so a
mid-stream store failure surfaces client-side as a short read rather than a
silent truncation. There is no `?path=`: the content key alone identifies the
bytes, which is also what makes the dedup-by-key cache possible.

Range requests are intentionally not supported: the mount materializes whole
files, so it never needs them. A future lazy-read mode for very large files
would add range support here.

## Usage

```sh
amber-store fuse ref:backups/home /mnt/home     # mount a tag read-only
amber-store fuse ref:backups/home@etc /mnt/etc  # mount a subdirectory of a tag
amber-store fuse KEY[/PATH] /mnt/snapshot       # mount by key, optional subpath
```

The spec is resolved like every other read command: `KEY[/PATH]` or
`ref:NAME[@PATH]`. It must name a **directory** — the mount root is always a
directory object.

The command runs in the foreground; Ctrl-C (or `SIGTERM`) unmounts and exits.
If the mount is busy — something has it open or a shell is `cd`'d into it,
common for a git repo under `$HOME` — the unmount fails with `EBUSY` and the
reason is printed; free the mount and press Ctrl-C to retry, or press Ctrl-C a
second time to force a lazy (detach) unmount and exit. Signals are never
swallowed, so Ctrl-C always has an effect.

| Flag              | Meaning                                                                            |
|-------------------|-----------------------------------------------------------------------------------|
| `--socket`        | daemon unix socket (default: `$AMBER_STORE_SOCKET` or the per-user path)           |
| `--allow-other`   | let other users access the mount (needs `user_allow_other` in `/etc/fuse.conf`)    |
| `--max-file-size` | refuse to open regular files larger than this many bytes (`0` = unlimited)         |
| `--cache-bytes`   | keep up to this many bytes of closed files resident for faster re-open (`0` = off) |
| `--debug`         | log FUSE protocol traffic to stderr                                               |

## Why Linux-only

Two facilities the design leans on are Linux-specific: `memfd_create(2)` for the
anonymous RAM backing files, and the `FUSE_PASSTHROUGH` protocol for handing
descriptors to the kernel. Rather than ship a degraded mount elsewhere, the
command is compiled in only on Linux (build-tagged `_linux.go` files, mirroring
the existing `xattr_linux.go` / `xattr_darwin.go` split) and omitted entirely on
other platforms — it does not appear in `--help` and the FUSE dependency is not
pulled into non-Linux builds.

## Limitations

- **Read-only.** The store is immutable; writes are refused with `EROFS`.
- **Whole-file materialization.** Each open file is held in RAM in full; there is
  no streaming or partial read of a single file. `--max-file-size` bounds the
  exposure.
- **Passthrough needs `CAP_SYS_ADMIN`** (see above); without it the mount works
  but reads take the fallback path.
- **Device-node `rdev`** is encoded best-effort; archived trees rarely contain
  device or special files, and sockets are not represented at all.
