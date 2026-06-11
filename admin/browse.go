package admin

import (
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/refstore"
)

// ObjectGetter is the read-only object-store view the ref browser needs;
// *diskstore.Store implements it.
type ObjectGetter interface {
	Get(k key.Key) ([]byte, error)
}

// RefStore is the read-only reference view the ref browser needs;
// *refstore.Store implements it.
type RefStore interface {
	Get(name string) ([]byte, error)
	All() ([]refstore.Record, error)
}
