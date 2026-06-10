// Package remotesync implements the push/pull algorithms between a local
// diskstore and a remote amber-store server: byte-balanced batching driven
// by the sizes encoded in keys, a two-round-trip have/want push, and a
// round-based BFS pull. See architecture/remote.md.
package remotesync

import (
	"github.com/draganm/amber-store/diskstore"
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
func PushSizer(store *diskstore.Store) SizeOf {
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

// Batches bins keys, in order, into batches whose estimated payload sizes
// approach target without exceeding it (a single object larger than target
// gets its own batch).
func Batches(keys []key.Key, target uint64, size SizeOf) [][]key.Key {
	var out [][]key.Key
	var cur []key.Key
	var curBytes uint64
	for _, k := range keys {
		s := size(k)
		if len(cur) > 0 && (curBytes+s > target || len(cur) >= maxBatchKeys) {
			out = append(out, cur)
			cur, curBytes = nil, 0
		}
		cur = append(cur, k)
		curBytes += s
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
