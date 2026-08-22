package nixcache

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// seedJobs bounds concurrent seed fetches, leaving admission headroom for
// the peers the node serves while it seeds.
const seedJobs = 4

// SeedPass ingests every catalogued path the index does not hold yet, so
// the node carries the closure before anyone asks for it. Peers answer
// before upstream, so a second seeder pulls the delta from the first.
// Failed paths are logged and reported so the caller can retry.
func (n *Node) SeedPass(ctx context.Context) int {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(seedJobs)
	var failed atomic.Int64
	for hp := range n.catalog.All() {
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
