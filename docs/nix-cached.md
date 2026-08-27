# nix-cached — a p2p pull-through cache for cache.nixos.org

`nix-cached` is a local Nix binary cache that fills itself from
cache.nixos.org on demand and shares what it has with other machines you
run — over the LAN, over the internet, and through NATs. Upstream
signatures are preserved verbatim: there are no new signing keys to
create, distribute, or protect.

Every machine runs the same daemon. Point Nix at it, and each store path
is downloaded from the nearest place that has it: your own disk, a peer,
or cache.nixos.org as the last resort.

## Quick start

```console
$ nix-cached --dir ~/.local/share/nix-cached \
    --catalog-url https://channels.nixos.org/nixos-unstable/store-paths.xz
nix-cached: serving on http://127.0.0.1:8321
```

Then tell Nix about it:

```
# nix.conf
substituters = http://127.0.0.1:8321 https://cache.nixos.org
```

Order does not matter: Nix ranks substituters by the priority number
each cache advertises (lower wins), and nix-cached advertises 10 to
cache.nixos.org's 40, so it is always asked first.

No extra `trusted-public-keys` entry is needed — the cache serves paths
signed by `cache.nixos.org-1`, which Nix already trusts.

The **catalog** is the list of store paths the node is willing to serve
(a channel's `store-paths.xz`). It is a safety filter: the node only
mirrors paths that provably belong to a channel, so it cannot be used to
distribute arbitrary NARs. Requests for uncatalogued paths return 404
and Nix falls through to the next substituter.

### Other caches

Existing substituters (Cachix, a company cache, ...) keep working
unchanged: nix-cached answers 404 for anything outside its catalog and
Nix moves on to the next cache. It only ever fetches from its one
upstream, so content from other caches is never mirrored or shared
with peers.

### NixOS

Add `amber-store` to your flake inputs and import the module:

```nix
{
  imports = [ amber-store.nixosModules.nix-cached ];
  services.nix-cached = {
    enable = true;
    catalogUrls = [ "https://channels.nixos.org/nixos-unstable/store-paths.xz" ];
  };
}
```

## Peering

Peering is off until a node is given `-p2p-port`, `-peer` or
`-serve-relay`. The minimal form:

```console
$ nix-cached --dir ... --p2p-port 8322
INFO swarm endpoint id=9a3fdb47… udp_port=8322
INFO swarm address ticket=endpointac…
```

Give one node the other's endpoint id and address, or its ticket:

```console
$ nix-cached --dir ... --peer 9a3fdb47…@203.0.113.7:8322
$ nix-cached --dir ... --peer 9a3fdb47…@seeder.example.org:8322
$ nix-cached --dir ... --peer endpointac…
```

The endpoint id is derived from `p2p.key` in the state directory and
stays stable. The ticket additionally carries the node's current
addresses and home relay and is re-logged when those change. Only one
side needs `-peer`; connections are used in both directions.

The transport is QUIC over one UDP port (8322 unless `-p2p-port` says
otherwise); open it in the firewall of any node that should accept
peers directly. A node bound to the wildcard address advertises the
addresses of its network interfaces.

### NAT and relays

Nodes keep a connection to a *relay* (by default the public n0 relays)
which forwards traffic between peers that cannot reach each other
directly and helps them punch through NATs to a direct UDP path. A
node's home relay is part of its ticket. `-relay <url>` replaces the
default set, `-no-relay` turns relays off for swarms where every node
is directly reachable.

A swarm that should not depend on third-party relays can run its own on
the seeder:

```console
$ nix-cached --dir /var/lib/nix-cached --seed --serve-relay :3340 \
    --relay-url https://seeder.example.org:3340 \
    --relay-cert cert.pem --relay-key key.pem
```

and point the leaves at it with `--relay https://seeder.example.org:3340`
(plus `--relay-ca ca.pem` for a private CA). With a certificate the
relay serves HTTPS on the TCP port and QUIC address discovery on the
same UDP port, which tells NATed leaves their public address so they
can hole-punch a direct path. Without `--relay-cert` it speaks plain
HTTP and only forwards, so two NATed leaves stay on the relay. Peers
given as `id@host:port` are also tried through the configured relays
when UDP to them is blocked. Relay traffic shows up as
`nix_cached_relay_*` metrics on the seeder.

### Seeding a mirror

A **seeder** (`-seed`) downloads every catalogued path eagerly instead
of waiting for requests, and allows 64 concurrent peer transfers. One
seeder per site turns first-time downloads for every other machine into
LAN transfers:

```console
$ nix-cached --dir /var/lib/nix-cached --seed \
    --catalog-url https://channels.nixos.org/nixos-unstable-small/store-paths.xz \
    --catalog-ttl 7d --budget-bytes 50000000000
```

The store is content-defined-chunk deduplicated, so keeping several
channel revisions in the catalog costs far less than their summed NAR
sizes (measured: ~1.8× dedup across adjacent nixos-unstable-small
revisions). `--catalog-ttl` ages out paths that left the channel;
`--budget-bytes` bounds the store by total NAR size (disk usage is
lower, thanks to dedup), evicting rarely requested paths first.

Changing `--catalog-ttl` applies to already-aged paths immediately:
only the moment a path left the catalog is recorded, and every pass
compares it against the current TTL. Shortening the TTL purges
everything past the new deadline on the next pass. Enabling a TTL on a
store that ran without one starts the clock at the next pass, so paths
that departed earlier still get one full TTL from that point.

## Operations

An admin socket lives at `<dir>/admin.sock`:

```console
$ curl --unix-socket ~/.local/share/nix-cached/admin.sock http://x/metrics
$ curl --unix-socket ~/.local/share/nix-cached/admin.sock -X POST http://x/-/gc
```

`POST /-/gc` answers 409 while a cycle (manual or eviction-triggered)
is already running.

`GET /-/liveness` reports per-segment live/dead bytes: what a GC run
would reclaim, without running one.

### Pinning paths

A pinned path is never evicted and never aged out, regardless of
budget, TTL, or catalog membership. Pins are keyed by the store path's
32-character hash part — the part before the first `-` in the store
path's base name:

```console
$ basename /nix/store/8irc6vfhcvcp6g1ik4ghn6d5wsvfg2q1-hello-2.12 | cut -d- -f1
8irc6vfhcvcp6g1ik4ghn6d5wsvfg2q1
```

To pin a whole closure, pin every path `nix path-info -r` prints.
Pins survive restarts:

```console
$ curl --unix-socket /var/lib/nix-cached/admin.sock \
    -X POST http://x/-/pin/8irc6vfhcvcp6g1ik4ghn6d5wsvfg2q1
$ curl --unix-socket /var/lib/nix-cached/admin.sock \
    -X DELETE http://x/-/pin/8irc6vfhcvcp6g1ik4ghn6d5wsvfg2q1
```

Unpinning returns the path to the normal lifecycle; if it already left
the catalog, aging starts at the next pass.

`/metrics` is Prometheus text format. The most useful signals:

| Metric | Meaning |
|---|---|
| `nix_cached_ingest_total{source=...}` | paths fetched, split by `swarm` vs `upstream` — your p2p hit rate |
| `nix_cached_narinfo_requests_total{result=...}` | what clients ask for and how it was answered |
| `nix_cached_swarm_peers{path=...}` | connected peers, split by `direct` vs `relay` path |
| `nix_cached_known_peers` | configured peers |
| `nix_cached_relay_*_total` | embedded relay counters (`-serve-relay`) |
| `nix_cached_store_bytes` | store size on disk |

The daemon logs every ingest with its source, peer connections and
discoveries, and all peering failures with their cause.

## Troubleshooting

**Nix complains about a missing or invalid signature.** nix-cached
serves upstream signatures unmodified, so this points at a
misconfiguration elsewhere: another substituter answered, or the
`substituters` line has the wrong address.

**A path 404s locally but exists on cache.nixos.org.** It is not in any
configured catalog. Add the channel's `store-paths.xz` via
`-catalog-url` (repeatable). Nix falls through to upstream on its own,
so this is a cache miss, not an outage.

**`swarm_peers` stays 0.**
- *Firewall*: UDP port 8322 must be open on at least one side, or both
  sides need a common relay. Dials that reach nothing fail with
  `timeout: no recent network activity` in `peer connect ... err=...`.
  On some systems legacy `iptables` rules land in a table the active
  nftables ruleset never consults; verify with `nft list ruleset`.
- *Endpoint id mismatch*: the id in `-peer` must match the remote's
  current key. Wiping a node's state directory regenerates `p2p.key`
  and changes its id; peers pointing at the old id fail the handshake.
  Update the `-peer` entries.
- The connect loop retries every second and logs each failure, so the
  journal always names the actual reason.

**Peers are connected but pulls still come from upstream.** Check
`known_peers` vs `swarm_peers`: a connection alone does not make a
node a *content* peer — that happens via configuration, and its index
is pulled on the sync interval (`-sync-every`, default 5 min). Requests
that arrive before then are served from upstream.

**Misses answer 503 for a while.** After cache.nixos.org returns
429/503, the node backs off (honoring `Retry-After`, capped at 15 min)
and answers upstream misses with 503 + `Retry-After` itself so clients
do not pile up. Cached paths and peer-to-peer serving continue during
backoff. `nix_cached_backoff_total` counts these episodes.

**A transfer was aborted mid-download.** Transfers that stop making
byte progress are cancelled after `-stall-timeout` (default 1 min) and
logged; the next request retries. On a link where healthy transfers
pause longer than a minute, raise the timeout.

**The first request for a path is slow.** A cold miss holds the
narinfo response until the path is fully ingested. The narinfo
references the local NAR URL, so it cannot be answered earlier — and
answering 404 would make Nix negative-cache the path and skip the
local cache for it. Concurrent requests for the same path share one
download; everything after the first request is a local hit.

## Flags

```
--dir             state directory (required, created if missing)
--listen          substituter address        default 127.0.0.1:8321
--upstream        upstream cache             default https://cache.nixos.org
--catalog-url     store-paths list URL       repeatable
--catalog-ttl     drop paths this long after they leave the catalog
--budget-bytes    NAR-size budget, evicts rarely requested paths
--peer-byte-rate  peer-serving bandwidth cap, bytes/second
--seed            eagerly mirror the whole catalog
--peer            <endpointid>@host:port or ticket, repeatable
--p2p-port        swarm UDP port             default 8322 when peering
--relay           relay URL replacing the n0 defaults, repeatable
--no-relay        direct UDP only
--serve-relay     run a relay on this TCP address, e.g. :3340
--relay-url       external URL of --serve-relay
--relay-cert/-key TLS certificate for --serve-relay, enables QAD
--relay-ca        extra CA bundle trusted for relays
--sync-every      catalog and peer sync interval   default 5m
--stall-timeout   abort stalled upstream transfers default 1m
--trusted-key     accepted narinfo signing key, repeatable
```
