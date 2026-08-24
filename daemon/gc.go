package daemon

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/draganm/amber-store/gc"
)

// gcEnabled writes a 503 and returns false when the daemon runs without a
// collector (--gc=false); the routes stay registered so the failure mode is
// explicit, not a 404.
func (h *handler) gcEnabled(w http.ResponseWriter) bool {
	if h.gc == nil {
		http.Error(w, "garbage collection is disabled (--gc=false)", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// gcStatus serves the full gc report — per-pack scores against the current
// union, totals, closures, last cycle — as JSON (gc.Status verbatim).
func (h *handler) gcStatus(w http.ResponseWriter, r *http.Request) {
	if !h.gcEnabled(w) {
		return
	}
	st, err := h.gc.Status(r.Context())
	if err != nil {
		h.log.Error("gc status failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// gcRun runs one cycle now and returns its gc.CycleStats as JSON. An
// optional ?garbage= query (a fraction) forces the selection line; absent
// means policy — 0.5, or 0.1 under min-free pressure. A cycle already in
// flight is a 409.
func (h *handler) gcRun(w http.ResponseWriter, r *http.Request) {
	if !h.gcEnabled(w) {
		return
	}
	garbage := -1.0
	if v := r.URL.Query().Get("garbage"); v != "" {
		g, err := strconv.ParseFloat(v, 64)
		if err != nil || g < 0 || g >= 1 {
			http.Error(w, "invalid garbage fraction "+strconv.Quote(v), http.StatusUnprocessableEntity)
			return
		}
		garbage = g
	}
	stats, err := h.gc.Run(r.Context(), garbage)
	if errors.Is(err, gc.ErrCycleRunning) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		h.log.Error("gc run failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("gc cycle complete", "scored", stats.Scored, "reaped", len(stats.Reaped),
		"copied_bytes", stats.CopiedBytes, "freed_bytes", stats.FreedBytes, "skipped", stats.Skipped)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// gcWhy serves the sorted reference names whose closure holds {key}'s tail
// — why the object is alive — as a JSON array (empty: unreferenced).
func (h *handler) gcWhy(w http.ResponseWriter, r *http.Request) {
	if !h.gcEnabled(w) {
		return
	}
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	names, err := h.gc.Why(k)
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
