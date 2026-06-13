// Package remotesync implements the push/pull algorithms between a local
// packstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a pipelined have/want push (parallel
// negotiation, re-batching, parallel upload), and a round-based BFS pull.
// See architecture/remote.md.
package remotesync

import (
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/key"
)

// DefaultBatchBytes is the default per-batch payload target.
const DefaultBatchBytes = 8 << 20 // 8 MiB

// maxBatchKeys bounds a batch's key count so pathological trees of tiny
// objects cannot produce arbitrarily large key-list bodies.
const maxBatchKeys = 8192

// nominalNodeSize is the pull-side estimate for tree/file-node objects whose
// encoded size is unknown before fetching (their key lengths are logical,
// not encoded, sizes). It only affects batch balance, never correctness.
const nominalNodeSize = 4096

// SizeOf estimates the transfer size of the object behind a key.
type SizeOf func(k key.Key) uint64

// PushSizer sizes objects for pushing: a Blob's exact payload length comes
// from its key; everything else (FileNode/DirLeaf/DirNode/XattrSet key
// lengths are logical or cumulative) is measured from the local store, with
// a nominal fallback if the read fails (the push itself will surface the
// real error).
func PushSizer(store *packstore.Store) SizeOf {
	return func(k key.Key) uint64 {
		if k.Type() == key.Blob {
			return k.Length()
		}
		data, err := store.Get(k)
		if err != nil {
			return nominalNodeSize
		}
		return uint64(len(data))
	}
}

// PullSizer sizes objects for pulling, where only the key is known: a Blob's
// exact length from the key, a nominal estimate for everything else.
func PullSizer() SizeOf {
	return func(k key.Key) uint64 {
		if k.Type() == key.Blob {
			return k.Length()
		}
		return nominalNodeSize
	}
}

// batcher accumulates keys into byte-balanced batches: estimated payload
// sizes approach target without exceeding it, a batch never holds more than
// maxBatchKeys keys, and a single object larger than target gets its own
// batch.
type batcher struct {
	target uint64
	size   SizeOf
	cur    []key.Key
	bytes  uint64
}

// add appends k to the current batch, first returning the completed batch
// k would have overflowed (nil if k still fits).
func (b *batcher) add(k key.Key) []key.Key {
	s := b.size(k)
	var full []key.Key
	if len(b.cur) > 0 && (b.bytes+s > b.target || len(b.cur) >= maxBatchKeys) {
		full = b.cur
		b.cur, b.bytes = nil, 0
	}
	b.cur = append(b.cur, k)
	b.bytes += s
	return full
}

// flush returns the final partial batch, nil if empty.
func (b *batcher) flush() []key.Key {
	out := b.cur
	b.cur, b.bytes = nil, 0
	return out
}

// Batches bins keys, in order, into batches whose estimated payload sizes
// approach target without exceeding it (a single object larger than target
// gets its own batch).
func Batches(keys []key.Key, target uint64, size SizeOf) [][]key.Key {
	b := batcher{target: target, size: size}
	var out [][]key.Key
	for _, k := range keys {
		if full := b.add(k); full != nil {
			out = append(out, full)
		}
	}
	if last := b.flush(); last != nil {
		out = append(out, last)
	}
	return out
}
