package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/keylist"
)

// uploadResponse mirrors the local daemon's ingest stats shape.
type uploadResponse struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// postMissing answers the have/want negotiation: of the keys in the request
// body, which does the server not have. Request and response bodies are raw
// concatenated 32-byte keys.
func (h *handler) postMissing(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	keys, err := keylist.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	var missing []byte
	for _, k := range keys {
		has, err := h.store.Has(k)
		if err != nil {
			h.log.Error("missing-check lookup failed", "key", k, "error", err)
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		if !has {
			missing = append(missing, k[:]...)
		}
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", missing)
}

// postObjects decodes an amberpack stream, verifies each object's payload
// against its key, and stores it — the same trust boundary as the local
// daemon: nothing unverified is ever persisted.
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	rd := amberpack.NewReader(bytes.NewReader(a.body))
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
			h.log.Warn("upload rejected", "error", err)
			h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.log.Error("upload failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("upload complete",
		"stored", stats.Stored,
		"deduped", stats.Deduped,
		"bytes_stored", stats.BytesStored,
	)
	body, err := json.Marshal(uploadResponse{
		ObjectsStored:  stats.Stored,
		ObjectsDeduped: stats.Deduped,
		BytesStored:    stats.BytesStored,
	})
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", body)
}

func (h *handler) postObjectsGet(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
