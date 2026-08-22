package nixcache

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// seedJobs bounds concurrent seed fetches, leaving admission headroom for
// the peers the node serves while it seeds.
const seedJobs = 4

// seedSet returns the union of the last fetched catalog lists, or the
// full catalog while some URL has not been fetched yet.
func (n *Node) seedSet() iter.Seq[string] {
	n.catalogMu.RLock()
	cur := make([]*Catalog, 0, len(n.catalogCur))
	for _, c := range n.catalogCur {
		cur = append(cur, c)
	}
	n.catalogMu.RUnlock()
	if len(cur) < len(n.cfg.CatalogURLs) || len(cur) == 0 {
		return n.catalog.All()
	}
	return func(yield func(string) bool) {
		seen := map[string]struct{}{}
		for _, c := range cur {
			for hp := range c.All() {
				if _, ok := seen[hp]; ok {
					continue
				}
				seen[hp] = struct{}{}
				if !yield(hp) {
					return
				}
			}
		}
	}
}

// SeedPass ingests every catalogued path not yet held. Peers answer
// before upstream, so a second seeder pulls the delta from the first.
// Returns the number of failures so the caller can retry.
func (n *Node) SeedPass(ctx context.Context) int {
	start := time.Now()
	var scheduled int
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(seedJobs)
	var failed atomic.Int64
	for hp := range n.seedSet() {
		if _, err := Lookup(n.indexRoot(), hp, n.store.Get); err == nil {
			continue
		}
		scheduled++
		g.Go(func() error {
			if _, err := n.fetch(ctx, hp); err != nil && ctx.Err() == nil {
				failed.Add(1)
				var be *BackoffError
				if errors.As(err, &be) {
					return err
				}
				slog.Warn("seed fetch", "hashpart", hp, "err", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		slog.Warn("seed pass aborted", "err", err)
	}
	n.metrics.seedFailures.Add(uint64(failed.Load()))
	if scheduled > 0 {
		slog.Info("seed pass", "fetched", scheduled-int(failed.Load()),
			"failed", failed.Load(), "dur", time.Since(start).Round(time.Second))
	}
	return int(failed.Load())
}
