// Package daemon serves the amber-store CAS over HTTP. The same http.Handler is
// transport-agnostic: a unix listener today, a TCP listener later. Routes are
// versioned under /v1.
package daemon

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/tarexport"
)

type handler struct {
	store *diskstore.Store
}

// New returns an http.Handler serving the store.
func New(store *diskstore.Store) http.Handler {
	h := &handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	return mux
}

type ingestResponse struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// postObjects decodes a pack-write stream, verifies and stores its objects, and
// returns store stats. Malformed-stream and verification failures are client
// errors (422); other failures are 500.
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request) {
	rd := amberpack.NewReader(r.Body)
	seq := func(yield func(diskstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(diskstore.Object{}, err)
				return
			}
			if !yield(diskstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	stats, err := h.store.WriteParallel(seq, diskstore.WriteOpts{Verify: true})
	if err != nil {
		if errors.Is(err, amberpack.ErrMalformed) || errors.Is(err, diskstore.ErrVerify) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ingestResponse{
		ObjectsStored:  stats.Stored,
		ObjectsDeduped: stats.Deduped,
		BytesStored:    stats.BytesStored,
	})
}

// getTar streams a PAX tar of the directory tree rooted at the {key} path value.
func (h *handler) getTar(w http.ResponseWriter, r *http.Request) {
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if k.Type() != key.DirLeaf && k.Type() != key.DirNode {
		http.Error(w, "key is not a directory object", http.StatusBadRequest)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "root object not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarexport.Write(w, k, h.store.Get); err != nil {
		// The 200 status and some bytes may already be in flight; we cannot change
		// the status now. Log and let the truncated archive surface as a tar read
		// error on the client.
		log.Printf("amber-store daemon: tar export of %s aborted: %v", k, err)
	}
}

// parseHexKey decodes a lowercase-hex key path segment into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return key.Parse(raw)
}
