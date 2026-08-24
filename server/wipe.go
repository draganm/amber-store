package server

import (
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
	// Quiesce the collector before touching the stores: a cycle still
	// holding the pre-wipe union must not copy records into — or delete
	// packs out of — a store being emptied underneath it. wipeMu already
	// excludes route-triggered cycles (gcRun holds it shared); this stops
	// the background-interval loop too. The collector's own state (union,
	// closures) is deliberately left intact until after the store wipe: if
	// a refs/store wipe below fails, the stale over-full union only ever
	// overstates liveness — the safe direction.
	var release func()
	if h.gc != nil {
		release = h.gc.Quiesce()
		defer func() {
			if release != nil {
				release()
			}
		}()
	}
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
	// Closures last: they are derived state, and a re-walk of a surviving
	// reference could only fail against an already-wiped store anyway.
	// gc.Wipe takes the cycle lock itself, so end the quiescence first;
	// the window is benign — a cycle sneaking in scores an empty store,
	// and Wipe cancels and waits it out.
	if h.gc != nil {
		release()
		release = nil
		if err := h.gc.Wipe(); err != nil {
			h.signError(w, a.nonce, http.StatusInternalServerError, "wiping closures: "+err.Error())
			return
		}
	}
	h.log.Warn("store wiped", "by", string(a.pubWire))
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", []byte(`{"wiped":true}`))
}
