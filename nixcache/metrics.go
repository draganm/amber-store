package nixcache

import (
	"fmt"
	"io"
	"sync/atomic"
)

type metrics struct {
	upstreamIngests, upstreamNarBytes atomic.Uint64
	swarmIngests, swarmNarBytes       atomic.Uint64
	evictedPaths, evictedNarBytes     atomic.Uint64
	gcRuns, gcFreedBytes              atomic.Uint64
	seedFailures                      atomic.Uint64
	backoffs                          atomic.Uint64

	narinfoHit, narinfoFetched      atomic.Uint64
	narinfoNotFound, narinfoBackoff atomic.Uint64
	narinfoError                    atomic.Uint64
	narBytesServed                  atomic.Uint64
}

func (n *Node) writeMetrics(w io.Writer) {
	m := &n.metrics
	direct, relayed := n.swarmPeers()
	fmt.Fprintf(w, `# TYPE nix_cached_ingest_total counter
nix_cached_ingest_total{source="upstream"} %d
nix_cached_ingest_total{source="swarm"} %d
# TYPE nix_cached_ingest_nar_bytes_total counter
nix_cached_ingest_nar_bytes_total{source="upstream"} %d
nix_cached_ingest_nar_bytes_total{source="swarm"} %d
# TYPE nix_cached_evicted_paths_total counter
nix_cached_evicted_paths_total %d
# TYPE nix_cached_evicted_nar_bytes_total counter
nix_cached_evicted_nar_bytes_total %d
# TYPE nix_cached_gc_total counter
nix_cached_gc_total %d
# TYPE nix_cached_gc_freed_bytes_total counter
nix_cached_gc_freed_bytes_total %d
# TYPE nix_cached_seed_failures_total counter
nix_cached_seed_failures_total %d
# TYPE nix_cached_backoff_total counter
nix_cached_backoff_total %d
# TYPE nix_cached_narinfo_requests_total counter
nix_cached_narinfo_requests_total{result="hit"} %d
nix_cached_narinfo_requests_total{result="fetched"} %d
nix_cached_narinfo_requests_total{result="notfound"} %d
nix_cached_narinfo_requests_total{result="backoff"} %d
nix_cached_narinfo_requests_total{result="error"} %d
# TYPE nix_cached_nar_bytes_served_total counter
nix_cached_nar_bytes_served_total %d
# TYPE nix_cached_catalog_paths gauge
nix_cached_catalog_paths %d
# TYPE nix_cached_indexed_paths gauge
nix_cached_indexed_paths %d
# TYPE nix_cached_store_bytes gauge
nix_cached_store_bytes %d
# TYPE nix_cached_swarm_peers gauge
nix_cached_swarm_peers{path="direct"} %d
nix_cached_swarm_peers{path="relay"} %d
# TYPE nix_cached_known_peers gauge
nix_cached_known_peers %d
`,
		m.upstreamIngests.Load(), m.swarmIngests.Load(),
		m.upstreamNarBytes.Load(), m.swarmNarBytes.Load(),
		m.evictedPaths.Load(), m.evictedNarBytes.Load(),
		m.gcRuns.Load(), m.gcFreedBytes.Load(),
		m.seedFailures.Load(), m.backoffs.Load(),
		m.narinfoHit.Load(), m.narinfoFetched.Load(),
		m.narinfoNotFound.Load(), m.narinfoBackoff.Load(),
		m.narinfoError.Load(),
		m.narBytesServed.Load(),
		n.catalog.Len(), n.indexedPaths(), n.store.SizeBytes(), direct, relayed,
		len(n.peerIDs()))
}

func (n *Node) indexedPaths() int {
	count := 0
	for _, err := range indexEntries(n.indexRoot(), n.store.Get) {
		if err != nil {
			return count
		}
		count++
	}
	return count
}

func (n *Node) swarmPeers() (direct, relayed int) {
	if n.cfg.Swarm == nil {
		return 0, 0
	}
	return n.cfg.Swarm.NumConnected()
}
