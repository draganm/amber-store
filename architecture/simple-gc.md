# Simple Garbage Collection

Live is what a reference reaches; the rest is garbage. The packstore stays
exactly as it is. For every root a reference names we keep its **closure** —
the sorted tails of every key reachable from it — on disk; the union of all
closures lives in RAM. A cycle walks every pack's index against that union to
measure how much of each pack is garbage; every pack that is more than half
garbage is **reaped**: its live records are copied into the active segment,
then the pack is deleted.

A **tail** is `key[24:32]` as a big-endian u64 — the last 8 bytes of a key,
always inside the uniformly distributed hash, and already what the packstore's
filters hash and its indexes fan out on. Two keys share a tail with
probability 2⁻⁶⁴; for membership tests that is exact.

Goals, in order: an accepted reference never dangles — a reference write is
optimistic and may have to be retried instead; writes and reads never wait
for the collector; no format change, no generations, no upload ordering;
no auxiliary database, lock file or pin list; consistent after a crash at any
point. The small sibling of the [generational collector](generational-gc.md).

## Layout

```
<store>/packstore/                 unchanged
<store>/closures/<root-hex>.tails  one closure per root named by a reference
<store>/closures/tmp/              staging; swept at open
```

```
magic   "AMBERCL\x01"   8 bytes
root    [32]byte        checked against the file name
count   u64 BE
tails   count × u64 BE  ascending, deduplicated
crc     u32 BE          CRC-32C over everything before it
```

- **Per root.** A root's tree is immutable, so its closure is too: references
  to one root share a file; an unchanged snapshot adds none.
- **8 bytes per object.** ≈ 20 MB per TB of tree; N snapshots of one tree
  cost N closures — 2 % of the tree per thousand. Full keys would cost 4×
  for nothing GC needs: when a live record is copied, its key comes from the
  pack's index.
- **Derived, ground truth for the union.** A reference without a closure is
  walked and gets one (at first start on an existing store, before any cycle
  runs); a file failing CRC or magic counts as absent. A closure whose root
  no reference names is deleted — at open and whenever a reference is
  deleted or overwritten — so a present closure always implies live, present
  objects.

## The union

The collector holds the union of all closures in RAM: a sorted array of
`(tail u64, refcount u32)`, ≈ 12 B per live object (30 MB per TB of unique
content). It is built at open by merging every named root's closure, and
kept current: a reference PUT merges its root's tails in (+1), a DELETE or
overwrite merges the old root's out (−1), each an O(union) sequential pass
producing a fresh array that is swapped in. Membership is a binary search
under a 64 K-entry fanout on the top tail bits — one or two cache misses,
exact. Nothing about the union is persistent; a restart rebuilds it.

## Writing a reference

`PUT /v1/refs` — server, and now the daemon too — under the collector's
**removal lock**, held shared from start to commit:

1. `closures/<root>.tails` exists and validates → reuse it.
2. Else walk the tree (`fstree.CheckComplete`, keeping the keys it visits);
   a missing object → `404` naming it, as the server does today. Write the
   tails to `closures/tmp/`, fsync, rename, fsync the directory.
3. Merge the tails into the union; commit the record; if the name pointed at
   another root, merge that root's tails out (and delete its file if no
   reference names it).

`DELETE /v1/refs` removes the record, merges the root's tails out, deletes
its file if unreferenced. No walk. The daemon's PUT gains the server's
completeness walk: minutes on a multi-TB root, on a tree hot in cache.

**Writing a reference is optimistic.** Nothing is reserved for an upload:
its objects sit in the store as garbage until the PUT names them, and the
PUT's walk is the only check. If a cycle reaped a pack holding some of them
in between — they were dead at the time, and the [horizon](#safety) did not
cover the upload (typically: they were deduplicated against objects nobody
named any more, or the reference came long after the upload) — the PUT
fails with `404` naming the missing objects. The caller re-sends exactly
those and retries the PUT: push negotiation recomputes the missing set,
`load` and `ingest` dedup everything already present. This is the contract:
a reference PUT may fail and must be retried; it never succeeds dangling.

## One cycle

Every `--gc-interval` and on `gc run`; skipped when the union did not change
and no pack crossed the horizon. One goroutine, cycles never overlap.

```
snapshot   union pointer; horizon
score      per pack, in parallel: walk its footer index; entry live iff its
           tail is in the union; garbage = 1 − Σ (46 + slen) / body
select     every eligible pack (sealed before the horizon) with
           garbage > --gc-garbage, most garbage first
reap       per victim: live records, file order, raw → active segment
delete     removal lock (exclusive): re-test the victim's index against the
           current union, copy what is live and not yet elsewhere; drop the
           pack from the probe list; munmap, unlink, fsync dir
```

**Scoring.** Every stored record is tested once per cycle, against RAM, from
the footer index alone — no pack body is read, no merge is performed. Cost:
one probe per stored record, packs in parallel; seconds on a multi-TB store.

**Selection.** Every eligible pack with more than `--gc-garbage` (0.5)
garbage is reaped, most garbage first, so each victim frees at least as much
as it copies. A pack below the line keeps its garbage until more of it dies.
A pack with no live bytes is deleted without copying. Under `--gc-min-free`
the line drops to 0.1 for that cycle; `gc run --garbage G` forces a pass at
any level.

**Reaping.** The victim is immutable and stays readable. Its index is walked
once more to list live entries; their records are read in file order,
CRC-checked, and re-appended raw (`AppendRecord`: no decode, no re-encode)
unless the key already exists outside the victim (`HasOutside`). Batches with
one fsync each, `--gc-rate`-throttled; the active segment rotates as usual.
Survivors are a pack's long-lived part, so stable data clusters over cycles
without generations.

**Deleting.** Reaping used the snapshot union; references written since may
name more of the victim. Under the removal lock held exclusively the victim's
index is re-tested against the current union (a few thousand probes) and the
new hits copied; then the pack is removed from the probe list under the
packstore write lock (drains in-flight reads, like a seal). Release; munmap
after in-flight scrubs (as `Close` does), unlink, fsync the directory.

## Safety

**Horizon.** Objects arrive long before their reference. Only packs sealed
before the horizon are eligible:

> horizon = min(now − `--gc-grace`, start of the oldest in-flight write,
> start of the oldest upload lease)

- In-flight writes: `Put`/`WriteBatch`/`WriteParallel` calls in progress;
  packstore records their start. A daemon `ingest` or `load` is one
  `WriteParallel`, protected however long it runs.
- Upload leases: the inbox takes one per root on its first pack, refreshes
  it per pack, releases it on the reference PUT; a lease expires
  `--gc-grace` after its last refresh. A pull holds one for its root.
- Grace covers upload-end → reference: `push-objects` … `push-ref`,
  `load` … `ref create`.

Seal time is the `.seg` mtime (its last write is the footer). The active
segment is never a victim. Everything the horizon does not cover falls back
on the [optimistic PUT](#writing-a-reference): the reference fails naming
the missing objects and is retried after re-sending them.

**Removal lock.** A PUT holds it shared from its first read to its commit;
deletion holds it exclusively around the re-test and the swap. A walk that
read an object from a victim therefore commits — and lands in the union —
before the victim can go, and the re-test sees it. So every object an
accepted reference reaches is outside the victim when it leaves the probe
list. The exclusive section is microseconds and blocks only reference writes.

**Readers.** A survivor is in the active segment (or a newer pack) before the
victim is touched; `Get` probes newest first. No live key is ever in neither
place.

**Crash.** No cycle survives a crash; the union is rebuilt from the closure
files of the references that exist. `tmp/` is swept; orphan closures are
deleted and missing ones walked before the first cycle. A half-reaped victim
is an ordinary pack with duplicates in the active segment — rescored,
finished (`HasOutside` skips what was copied). A pack swapped out but not
unlinked is reloaded at open, scores as duplicated, and is deleted without
copying. Copies are ordinary appends: record-level durable, tail-scan
recoverable.

## Configuration and CLI

| Flag | Default | Meaning |
| --- | --- | --- |
| `--gc` | on | run the collector |
| `--gc-interval` | 1 h | time between cycles |
| `--gc-grace` | 1 h | minimum age of a sealed pack before it can be reaped; upload-lease idle timeout |
| `--gc-garbage` | 0.5 | an eligible pack with more garbage than this is reaped |
| `--gc-min-free` | 5 % of the filesystem | below this free space the cycle reaps packs above 0.1 garbage |
| `--gc-rate` | unlimited | copier bandwidth cap (bytes/s) |

`--segment-size` is the reaping granularity: smaller packs reclaim with less
copying, cost more filter probes per lookup.

```sh
amber-store gc status               # packs: id, sealed, bytes, garbage, eligible;
                                    # totals; closures; union; last cycle  GET  /v1/gc
amber-store gc run [--garbage G]    # score now, reap packs above G        POST /v1/gc/run
amber-store gc why KEY              # refs whose closure holds KEY's tail  GET  /v1/gc/why/{key}
```

Server routes are signed; `gc run` needs `admin`. The same figures are
exported on the debug listener (`amber_gc_*`). `Wipe` cancels a running cycle,
empties `closures/` and the union.

## Code shape

**packstore**, format-neutral: `Segments()` (id, seal time, body bytes, key
count); `ScanIndex(id, fn(key, off, slen))` — the footer index in order;
`Record(id, off)` — the raw record at an offset, CRC-checked;
`HasOutside(id, k)`; `AppendRecord(k, raw)` — re-append an encoded record
through the normal append path; `Remove(id)` — probe-list swap under the
write lock, wait for scrubs, munmap, unlink, fsync; `OldestInflightWrite()`.

**fstree**: `CheckComplete` returns the keys it visited.

**`gc`** (new): closure codec and directory, the union, removal lock, upload
leases, the cycle, status. API: `PrepareRef(root) (commit func(), err)` and
`ReleaseRef(root)` for the PUT/DELETE handlers, `Lease(root)` for the inbox
and `remotesync.Pull`, `Run`/`Status`/`Why` for the routes. `daemon`,
`serve` and `embedded` open a collector next to the packstore and refstore
they already open. Wire protocol and pack formats are unchanged.

## Compared with the generational collector

Both copy live records forward and delete whole packs. The
[generational design](generational-gc.md) buys cheap young collections with
three generations, an age invariant, ordered uploads, promotion by rename and
a repairing ref barrier, and marks at collection time. This one has one
directory, one eligibility rule, one lock — and pays with closure files
(8 B per reference × object), a union in RAM (12 B per live object), and one
probe per stored record per cycle. Pick it unless many thousands of
snapshots share a tree and 8 B per (snapshot, object) is too much.

## Not done, deliberately

- **No mark at collection time.** It would cost nothing on disk but read
  every internal node cold; the closure walk runs once per root, warm, and
  cycles touch only footer indexes and RAM.
- **No per-reference filters.** Filters cannot be merged or subtracted, so
  every dead record would be tested against every reference's filter, all
  filters resident (RAM ∝ references × objects, not live objects), and a
  false positive would pin a dead record for as long as the reference lives.
  A filter safe against that (32-bit fingerprints) is barely smaller than a
  tail.
- **No live list probed against pack filters.** That is |live| × packs
  probes; walking the indexes is one probe per stored record.
- **No tombstones, pins or global index.** Closures and the union give
  liveness; leases and grace cover in-flight work; scoring uses the indexes
  each pack already carries.
