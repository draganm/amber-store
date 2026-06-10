// Package keylist encodes lists of store keys as raw concatenated 32-byte
// keys — the request/response body format of the remote server's
// have/want negotiation. Dense, order-preserving, zero parsing ambiguity:
// the byte length must be a multiple of key.Size and every key canonical.
package keylist

import (
	"fmt"

	"github.com/draganm/amber-store/key"
)

// Flatten concatenates keys in order.
func Flatten(keys []key.Key) []byte {
	out := make([]byte, 0, len(keys)*key.Size)
	for _, k := range keys {
		out = append(out, k[:]...)
	}
	return out
}

// Parse splits b into canonical keys. It rejects a length that is not a
// multiple of key.Size and any non-canonical key.
func Parse(b []byte) ([]key.Key, error) {
	if len(b)%key.Size != 0 {
		return nil, fmt.Errorf("key list length %d is not a multiple of %d", len(b), key.Size)
	}
	keys := make([]key.Key, 0, len(b)/key.Size)
	for i := 0; i < len(b); i += key.Size {
		k, err := key.Parse(b[i : i+key.Size])
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i/key.Size, err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}
