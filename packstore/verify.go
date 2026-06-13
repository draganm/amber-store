package packstore

import (
	"errors"
	"fmt"

	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// ErrVerify is returned (wrapped) when an object's key does not match its
// payload. Callers distinguish it with errors.Is to map to a client error.
var ErrVerify = errors.New("packstore: object verification failed")

// verifyObject recomputes o.Key from o.Data and reports ErrVerify on mismatch.
// For Blob and XattrSet — whose key length is the serialized byte length — it
// also checks the length field. Aggregate types (FileNode/DirLeaf/DirNode)
// carry a logical length the store cannot recompute without parsing, so only
// their hash is checked.
func verifyObject(o Object) error {
	sum := blake3.Sum256(o.Data)
	want, err := key.NewFromHash(o.Key.Type(), o.Key.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
	}
	if want != o.Key {
		return fmt.Errorf("%w: payload hashes to %s, not %s", ErrVerify, want, o.Key)
	}
	switch o.Key.Type() {
	case key.Blob, key.XattrSet:
		if o.Key.Length() != uint64(len(o.Data)) {
			return fmt.Errorf("%w: %s length field %d != payload %d", ErrVerify, o.Key, o.Key.Length(), len(o.Data))
		}
	}
	return nil
}
