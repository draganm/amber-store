package nixcache

import (
	"context"
	"fmt"
	"iter"
	"os"
	"sync/atomic"

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

// SeedPass ingests every catalogued path the index does not hold yet, so
// the node carries the closure before anyone asks for it. Peers answer
// before upstream, so a second seeder pulls the delta from the first.
// Returns the number of failed paths so the caller can retry.
func (n *Node) SeedPass(ctx context.Context) int {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(seedJobs)
	var failed atomic.Int64
	for hp := range n.seedSet() {
		if _, err := Lookup(n.indexRoot(), hp, n.store.Get); err == nil {
			continue
		}
		g.Go(func() error {
			if _, err := n.fetch(ctx, hp); err != nil && ctx.Err() == nil {
				failed.Add(1)
				fmt.Fprintf(os.Stderr, "nixcache: seed %s: %v\n", hp, err)
			}
			return nil
		})
	}
	g.Wait()
	if f := failed.Load(); f > 0 {
		fmt.Fprintf(os.Stderr, "nixcache: seed pass: %d paths failed\n", f)
	}
	return int(failed.Load())
}
