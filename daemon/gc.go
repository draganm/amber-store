// The daemon's GC surface: status (advisory mark + per-pack scores), run
// (one cycle, synchronous), why (the references holding a key live). The
// unix socket is the daemon's admin channel, so the routes carry no auth of
// their own. Client and daemon ship in one module, so the bodies are the gc
// package's own types marshalled as JSON.

package daemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/draganm/amber-store/gc"
)

// gcStatus reports per-pack liveness against a fresh advisory mark, totals,
// and the last cycle. The mark walks every live tree — on a big store this
// is a slow endpoint by design (no liveness state is kept between cycles).
func (h *handler) gcStatus(w http.ResponseWriter, r *http.Request) {
	st, err := h.coll.Status(r.Context())
	if err != nil {
		h.log.Error("gc status failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// gcRun runs one cycle synchronously and returns its stats. ?garbage= forces
// the selection line (a fraction; -1 or absent uses policy: 0.5, or 0.1
// under min-free pressure). An overlapping run is a 409.
func (h *handler) gcRun(w http.ResponseWriter, r *http.Request) {
	garbage := -1.0
	if s := r.URL.Query().Get("garbage"); s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || v > 1 {
			http.Error(w, "garbage must be a fraction <= 1", http.StatusUnprocessableEntity)
			return
		}
		garbage = v
	}
	stats, err := h.coll.Run(r.Context(), garbage)
	switch {
	case errors.Is(err, gc.ErrCycleRunning):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		h.log.Error("gc cycle failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("gc cycle complete", "marked", stats.Marked, "reaped", len(stats.Reaped),
		"copied_bytes", stats.CopiedBytes, "freed_bytes", stats.FreedBytes, "duration", stats.Duration)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// gcWhy lists the references whose tree reaches ?key= — why it is alive. An
// empty list means unreferenced: the next cycle may reap it.
func (h *handler) gcWhy(w http.ResponseWriter, r *http.Request) {
	k, err := parseHexKey(r.URL.Query().Get("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	names, err := h.coll.Why(k)
	if err != nil {
		h.log.Error("gc why failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if names == nil {
		names = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(names)
}
