// Package daemon serves the amber-store CAS over HTTP. The same http.Handler is
// transport-agnostic: a unix listener today, a TCP listener later. Routes are
// versioned under /v1.
package daemon

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/diskstore"
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

// getTar is implemented in Task 8.
func (h *handler) getTar(w http.ResponseWriter, r *http.Request) {
	_ = tarexport.Write // referenced fully in Task 8
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
