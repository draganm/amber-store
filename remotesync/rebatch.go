package remotesync

import (
	"context"

	"github.com/draganm/amber-store/key"
)

// rebatch drains in — slices of missing keys in arbitrary arrival order —
// and accumulates them into byte-balanced batches (target bytes, the
// maxBatchKeys cap, an oversized single key alone), sending each completed
// batch and the final partial one to out. It returns once in is closed and
// everything is flushed, or with ctx.Err() when ctx ends first. The caller
// owns closing out.
func rebatch(ctx context.Context, in <-chan []key.Key, out chan<- []key.Key, target uint64, size SizeOf) error {
	b := batcher{target: target, size: size}
	send := func(batch []key.Key) error {
		select {
		case out <- batch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		select {
		case keys, ok := <-in:
			if !ok {
				if last := b.flush(); last != nil {
					return send(last)
				}
				return nil
			}
			for _, k := range keys {
				if full := b.add(k); full != nil {
					if err := send(full); err != nil {
						return err
					}
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
