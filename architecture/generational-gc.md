# Generational Garbage Collection

Objects become garbage when nothing names them: a reference is deleted or
overwritten (an expired snapshot), an upload is aborted half-way, a `load` is
never followed by `ref create`. GC finds those objects and reclaims their disk
space. It is a **generational copying collector over packstores**: three
generations, each a set of ordinary [packstore](../packstore/segment.go)
directories, and one background collector that copies live records forward,
promotes whole packstores by renaming their directory, and removes swept ones.
The segment format is unchanged — the reserved delete tag (`0x02`) stays
unused; liveness is recomputed, never recorded.

Goals, in order: **no stop-the-world** (the only exclusive sections are
pointer swaps, microseconds long); **writes into the CAS are never blocked by
the collector**; no new index, database, lock file or pin list; crash at any
point leaves a consistent store.

## Generations

A generation is a directory of **packstores**. Each is an ordinary packstore
directory, named by a store-wide sequence number, and is either **appending**
or **frozen** (active segment sealed, never written again). Young owns the one
appending packstore that receives all external writes — the **write target** —
and freezes and replaces it when it reaches Y. Everything else is frozen,
except the **to-space** a running cycle copies into. Targets are staggered:
Y = 5 GiB for young, 2Y for survivor, the rest of the disk for old.

```
          ingest · load · inbox drain            (all external writes)
                        │
                        ▼
   ┌─── young (write target rotates at Y) ──────────────┐
   │ young/…0041/  write target (appending)             │
   │ young/…0039/  frozen, waiting for leases/grace     │
   │ young/…0040/  frozen to-space of the last cycle    │──┐  promote = rename
   └────────────────────────────────────────────────────┘  │  a whole packstore
                        ┌──────────────────────────────────┘  directory
                        ▼
   ┌─── survivor (target 2Y) ───────────────────────────┐
   │ survivor/…0012/  survivor/…0027/  survivor/…0033/  │
   │ (+ a to-space while a survivor cycle runs)         │──┐
   └────────────────────────────────────────────────────┘  │
                        ┌──────────────────────────────────┘
                        ▼
   ┌─── old (capacity − 3Y − reserve) ──────────────────┐
   │ old/…0001/  old/…0005/  old/…0009/  …              │  no upper level:
   │ (+ a to-space while an old cycle runs)             │  garbage is copied
   └────────────────────────────────────────────────────┘  out, never promoted
```

**Promotion is a directory rename.** The open `*packstore.Store` is moved from
one generation's probe list to the next under the store lock and told its new
path; its fds, mmaps and directory flock follow the rename. Nothing is copied,
and there is no instant at which a key is in neither list.

Reads and dedup probe young (write target, then frozen packstores newest
first) → survivor → old; first hit wins. The first hit also defines a key's
**youngest copy** — the same key may legally exist in several packstores (a
copy in flight, a stale older copy); contents are identical by construction.

## The age invariant and ordered uploads

Liveness comes from a mark over the roots. If every young cycle had to walk a
multi-terabyte old generation, the cheap generation would be the expensive
one, so young cycles must be able to skip older generations. That is safe
exactly when:

> **Age invariant** — for every reachable object, each child's youngest copy
> is in the same generation as the object's youngest copy, or in an older one.

A mark for generation *g* then never descends into an object whose youngest
copy is older than *g*: by the invariant its whole subtree is there too.

```
ref ─▶ root (young) ─▶ dir (young) ─┬─▶ file (young) ─▶ blob (young) · blob (old)
                                    ├─▶ dir (survivor) ╳  pruned: by the age invariant
                                    └─▶ dir (old)      ╳  its whole subtree is there too
```

The same property lets a packstore move up as a whole (its objects' children
are inside it or below). Three mechanisms keep the invariant:

1. **Uploads are ordered children-first, so a tree lands bottom-up.** The
   fstree builders already emit children before parents; what changes is the
   path from there to the segment. The `ingest` client's workers enqueue a
   parent only after its children are enqueued (the `-o` pack and `load`
   inherit the order); `WriteParallel` keeps verifying in parallel but appends
   in arrival order; `fstree.ReachableKeys` — and the remote's reachable-keys
   route — return **post-order** (children first), so `remote push-objects`
   uploads the negotiated missing set in that order, `pull` downloads in that
   order, and the inbox drains one root's packs in sequence (a `Seq` in
   `inbox.Meta`), parallel only across roots. Order matters only where an
   upload spans a freeze or dedups against an old copy; inside one write
   target it is irrelevant.
2. **The ref PUT repairs what ordering missed.** A tree becomes reachable
   through exactly one gate, `PUT /v1/refs`, whose completeness walk (see
   [ref barrier](#ref-barrier)) runs post-order and, for an internal node whose
   youngest copy is older than one of its children's, appends the node again
   into the write target. The walk already fetches every internal node and
   probes every child, so the check is free; repairs are rare (a dedup hit
   against a stale copy in old whose children were collected earlier).
3. **The collector moves only whole, closed packstores up.** Sweeps copy into
   one to-space per cycle, so a to-space is closed when its cycle ends: every
   child of its objects was in a victim (and is now in the to-space) or already
   in an older generation. Any frozen packstore is promoted only after a
   sequential scan of its internal nodes (youngest copies only) finds no child
   in another packstore of the same generation. Children are older than
   parents, so "has a child in" is acyclic across packstores and every
   packstore closes once those below it have moved up.

## One cycle

A generation is either **appending** (no cycle running) or **copying**. Young
runs a cycle as soon as it has an eligible frozen packstore (see [leases and
grace](#leases-and-grace)); survivor when its footprint reaches 2Y; old when
free space falls under `--gc-min-free` + 3Y. `*` marks the write target:

```
young/   [39 frozen][41 write*]         39 eligible: its leases ended, grace passed
         open to-space 42                victims = {39}
         mark from the roots, pruned at survivor/old → live filter
         39 is 40 % live → sweep: live(39) ──copy, file order──▶ 42 ; rm -r 39
         freeze 42                       [42 frozen][41 write*]        cycle ends
later    41 reaches Y → freeze, open 43  [42][41][43 write*]
         42: ~100 % live, closed   → mv young/42 survivor/42
         41: 95 % live, closed now → mv young/41 survivor/41          [43 write*]
```

Ingest keeps appending to the write target throughout; copies go through the
same append path into the to-space, in small batches, so nothing ever waits
for the copier. The collector marks once — from the roots, pruned at
generations older than the one being collected, into a live filter; each
packstore's live ratio is one index scan against it — then applies these rules
to every frozen packstore, looping while a promotion closes another:

1. **Closed and ≥ `--gc-promote-ratio` (default 90 %) live → promote**: rename
   the directory into the upper generation. Copying it would write nine bytes
   to reclaim one; its little garbage rides along and dies in that
   generation's next sweep. Old has no upper level and skips such packstores.
2. **Garbage-rich → sweep**: read it sequentially (mmap, file order), copy
   every live record — raw, CRC-checked, no decode — into the cycle's
   to-space, then close it and `rm -r` it. A copy is skipped only when the key
   is already present in the to-space's generation or a younger one.
3. **Mostly live but not yet closed → wait**; it closes when the packstores
   holding its children move up — later in the same pass or next cycle.
4. **Old** picks victims garbage-richest first — enough projected reclaim to
   clear its trigger, at most ~2Y of projected live bytes per cycle, so its
   to-spaces stay the size of a promoted packstore — and its sweep yields
   between packstores to young and survivor cycles (its filter stays valid;
   see the barrier).

At the end of a cycle the to-space is frozen and becomes an ordinary frozen
packstore of its generation; being almost all live and closed, the next cycle
usually promotes it. A frozen write target closes once the to-space below it
has moved up, so it follows one cycle later. Under archival load young
therefore does almost no copying — bytes are written once, then renamed upward
twice; copying happens in proportion to garbage.

## Mark: roots and liveness

**Roots** are every reference in the refstore and every pending inbox root
(`inbox.Meta.Root`, whose tree may still be incomplete — missing objects are
tolerated for these).

**The walk** is `fstree.ReachableKeys`' traversal, pruned as above:
`DirNode`/`DirLeaf`/`FileNode` are fetched and decoded, `Blob`/`XattrSet`
recorded without a read. An exact visited set of internal-node keys keeps
shared subtrees (snapshots dedup 99 %) from being re-walked. The mark is
read-only and runs beside normal traffic.

**The live set is a binary fuse filter** (16-bit fingerprints over the key
tail `k[24:32]`, the same code as segment footers), built in shards of a few
million keys, ~18 bits per live object. It can only err *towards* liveness: a
false positive keeps a dead object (≈ 15 per million) until the next cycle's
filter, seeded afresh, lets it go; a live object is never missed.

**Cost.** A young mark is O(young internal nodes) — seconds. Survivor adds
survivor. Only old pays the full walk, minutes on a multi-TB store, and rarely.

## Safety

**Victims are immutable** and readable until the instant they are removed;
copies are ordinary appends (record-level durable, tail-scan recoverable).
Nothing is ever modified in place.

### Leases and grace

An upload writes its objects long before the reference that names them
exists. Two guards keep such objects out of victim sets:

- **Write leases.** `POST /v1/objects` (daemon ingest and `load`) holds a
  lease for the duration of the request. The inbox holds one **per root**:
  taken when the first pack for `Meta.Root` arrives, refreshed by every
  further pack, released by the ref PUT for that root, expiring `--gc-grace`
  after the last refresh — a multi-day push stays protected as long as packs
  keep coming. A frozen packstore is eligible only once every lease that was
  active while it was the write target has ended…
- **Grace.** …and `--gc-grace` (default 1 h) has passed since it was frozen.
  This covers the gap between an upload's end and its `PUT /v1/refs`, and
  `load` followed by `ref create`. A push idle for longer, or a ref created
  later than that, may find objects gone: the PUT's completeness check reports
  it and the sender re-pushes exactly the missing objects (negotiation
  recomputes them) — a retry, never a dangling reference.

Leases and grace gate only packstores that were once the write target;
to-spaces are eligible as soon as they are frozen. Under heavy ingest young's
footprint is therefore about the last `--gc-grace` of ingest rather than Y — a
soft target; only a full disk fails a write, as today.

### Ref barrier

A reference created or replaced *during* a cycle may name a tree the mark
considered dead. Every ref PUT therefore walks its tree with
`fstree.CheckComplete` — the local daemon too, which today checks only that
the root exists — post-order, holding the collector's **removal lock shared**,
and for every visited object:

- repairs the age invariant (mechanism 2 above);
- during a *mark* phase, appends the new root to that mark's frontier (the
  switch to sweeping happens under the lock, after draining);
- during a *sweep* phase, if the object's youngest copy is in a victim and not
  in that cycle's live filter, copies it into the cycle's to-space right there.

The collector takes the removal lock exclusively only around a removal — a
probe-list swap plus `rm -r`. An accepted ref is thus always complete in
non-victim packstores and satisfies the invariant, and ref writes wait at most
microseconds. Ref deletes need no barrier: their objects die at the next mark.
Object writes never take this lock.

### Freeze, promote, remove

Freezing seals the active segment (today's `sealActiveLocked`) and, for the
write target, flips the pointer to a fresh packstore under the store lock.
Removal takes the packstore out of its generation's probe list under the write
lock (readers hold the read lock for their whole read, so this drains them),
`Close()`s it (waits for in-flight scrubs as today, munmaps, releases the
flock), `rm -r`s the directory and fsyncs the generation directory. Promotion
is the rename described under Generations.

### Crash

Cycle state lives in memory. After a crash there is no cycle; the write target
resumes after its tail-scan, a to-space that was not yet frozen is frozen on
open, and victims not yet removed are ordinary
frozen packstores whose live records may also exist in a to-space (legal —
duplicates are dedup hits). A directory rename is atomic, so a crash around a
promotion leaves the packstore in exactly one generation, correct either way.
A crash costs one re-mark.

## Never stop the world

```
 time ─────────────────────────────────────────────────────────────────▶
 ingest     ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  appends never wait
 readers    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  hit victim or to-space
 ref PUT    ·········▒▒▒·······················▒▒▒▒·················  walk, repair, barrier copy
 collector  ─┤ mark (pruned) ├── sweep 39 ──┤r├─ freeze 42 ─┤ idle ├─ mv 42 ─┤ mv 41 ─┤
                                             ▲                        ▲        ▲
 exclusive, µs: probe-list swaps (r = rm -r, mv = rename) · write-target rotation
```

The copier is throttled (`--gc-rate`, bytes/s) and writes in small batches
with one fsync each, so it never monopolises the append mutex or the disk.

## Layout, migration, configuration

```
<store>/packstore/young/<seq>/      write target, frozen packstores, to-space
<store>/packstore/survivor/<seq>/   frozen packstores (+ to-space during a cycle)
<store>/packstore/old/<seq>/        frozen packstores (+ to-space during a cycle)
```

`<seq>` is a store-wide 16-hex-digit counter, 1 + the largest found anywhere
at open. First open of a v1 store moves `packstore/*.seg*` into
`packstore/old/0000000000000001/` (frozen; existing data is treated as old)
and opens `young/…0002/` as the write target.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--gc` | on | run the collector (off: plain append-only behaviour) |
| `--gc-young-size Y` | 5 GiB | write-target rotation size; survivor target is 2Y |
| `--gc-min-free` | 5 % of the filesystem | space kept free; old's target is capacity − 3Y − this |
| `--gc-promote-ratio` | 0.9 | live ratio at/above which a closed packstore is promoted (old: skipped) |
| `--gc-grace` | 1 h | minimum frozen age before a packstore can be a victim; also the inbox root-lease idle timeout |
| `--gc-rate` | unlimited | copier bandwidth cap |

CLI: `amber-store gc status` (per-generation footprints and packstores, phase,
last cycle's copied / promoted / freed bytes, mark duration — also exported on
the debug listener) and `gc run` (start a cycle now). `Wipe` cancels a running
cycle and wipes all three generations.

## Code shape

A new package `genstore` owns the generations, the sequence counter, leases,
the removal lock and the collector goroutine, and exposes packstore's surface
(`Put`/`WriteBatch`/`WriteParallel`/`Get`/`GetRecord`/`Has`/`Missing`/
`StoredSize`/`SortByLocation`/`Verify`/`Wipe`/`Close`) plus `Lease()`,
`Locate()` and the barrier hook used by ref PUTs. `daemon`, `serve` and
`embedded` open a `genstore` where they open a packstore today.

packstore gains, all small and format-neutral: `Freeze()`; `Records()` —
sequential raw records in file order; `AppendRecord(key, raw)` — re-append an
already encoded record, CRC verified; `Size()`; `Relocate(dir)` after a
rename; arrival-order appends in `WriteParallel`. fstree: post-order
`ReachableKeys`, a post-order `CheckComplete` with a per-object hook.
remotesync/inbox: post-order push and pull, `Meta.Seq`, per-root sequencing.

## Not done, deliberately

- **No pins.** Per-root inbox leases and grace cover in-flight work; anything
  older than that is a retry, never a loss.
- **No tombstones.** Delete records (`0x02`) would add state that liveness
  recomputation makes redundant.
- **No global index.** Marks pay the many-filter `Get` cost instead; pruning
  keeps it proportional to the generation being collected.
