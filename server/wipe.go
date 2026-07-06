package server

import (
	"net/http"
)

// postWipe destroys every object and reference — the factory-reset endpoint
// behind the wipe capability (allowlist-only; grants cannot carry it). The
// allowlist, server identity and nonce cache are untouched, so the caller
// keeps serving afterwards. wipeMu is held exclusively for the duration:
// mutating handlers (putRef, deleteRef, postObjects) hold it shared, so no
// write is in flight while the stores empty — a racing push can never land a
// reference whose objects just vanished, and the inbox is quiescent (pushes
// complete before the ingest returns).
func (h *handler) postWipe(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.wipeMu.Lock()
	defer h.wipeMu.Unlock()
	// Refs first: if the object wipe fails midway, the store holds dangling
	// (ref-less) objects — invisible garbage — rather than refs pointing at
	// deleted content.
	if err := h.refs.Wipe(); err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, "wiping references: "+err.Error())
		return
	}
	if err := h.store.Wipe(); err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, "wiping objects: "+err.Error())
		return
	}
	h.log.Warn("store wiped", "by", string(a.pubWire))
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", []byte(`{"wiped":true}`))
}
