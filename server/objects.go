package server

import (
	"net/http"

	"github.com/draganm/amber-store/internal/keylist"
)

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

func (h *handler) postObjects(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}

func (h *handler) postObjectsGet(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.signError(w, a.nonce, http.StatusNotImplemented, "not implemented")
}
