package nixcache

import (
	"context"
	"log/slog"
	"time"
)

// connectLoop keeps the static peers connected: fast retries while any is
// down, backing off to a slow liveness check once all are up.
func (n *Node) connectLoop(ctx context.Context) {
	d := time.Second
	wasUp := true
	for {
		allUp := true
		for _, a := range n.cfg.Peers {
			if n.cfg.Swarm.Connected(a.ID) {
				continue
			}
			allUp = false
			cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_, err := n.cfg.Swarm.Conn(cctx, a.ID)
			cancel()
			if err != nil {
				slog.Warn("peer connect", "peer", a.ID.Short(), "err", err, "retry_in", d)
				continue
			}
			slog.Info("peer connected", "peer", a.ID.Short())
			allUp = true
		}
		if !allUp && wasUp {
			d = time.Second
		} else {
			d = min(d*2, time.Minute)
		}
		wasUp = allUp
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}
