package server

import (
	"fmt"
	"net/http"
)

// postWipe destroys every object and reference — the factory-reset endpoint
// behind the wipe capability (allowlist-only; grants cannot carry it). The
// allowlist, server identity and nonce cache are untouched, so the caller
// keeps serving afterwards. wipeMu is held exclusively for the duration:
// mutating handlers (putRef, deleteRef, postObjects' inbox commit) hold it
// shared, so no ref write races the reset — a reference can never point at
// objects the wipe just removed. Objects are weaker: inbox workers ingest
// committed packs asynchronously, so a pack accepted shortly before the wipe
// may land its objects afterwards. Those stragglers are unreferenced,
// content-addressed garbage — invisible and harmless, never wrong — and the
// JOBS engine quiesces its own pushers before calling this anyway.
func (h *handler) postWipe(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	h.wipeMu.Lock()
	defer h.wipeMu.Unlock()
	// The collector runs this once no cycle is marking. Refs first: if the
	// object wipe fails midway we keep garbage rather than dangling refs.
	err := h.coll.Wipe(func() error {
		if err := h.refs.Wipe(); err != nil {
			return fmt.Errorf("wiping references: %w", err)
		}
		if err := h.store.Wipe(); err != nil {
			return fmt.Errorf("wiping objects: %w", err)
		}
		return nil
	})
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Warn("store wiped", "by", string(a.pubWire))
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", []byte(`{"wiped":true}`))
}
