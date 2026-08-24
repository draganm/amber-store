package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
)

// parseHexKey decodes a lowercase-hex key path segment into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return key.Parse(raw)
}

// The gc routes mirror the daemon's (GET /v1/gc, POST /v1/gc/run,
// GET /v1/gc/why/{key}) behind the signed transport: status and why need
// read, run needs admin (route table in server.go). Payloads are the gc
// package's types as JSON, verbatim.

// gcEnabled writes a signed 503 and returns false when the server runs
// without a collector (--gc=false).
func (h *handler) gcEnabled(w http.ResponseWriter, nonce []byte) bool {
	if h.gc == nil {
		h.signError(w, nonce, http.StatusServiceUnavailable, "garbage collection is disabled (--gc=false)")
		return false
	}
	return true
}

// gcStatus serves the full gc report — per-pack scores against the current
// union, totals, closures, last cycle.
func (h *handler) gcStatus(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if !h.gcEnabled(w, a.nonce) {
		return
	}
	st, err := h.gc.Status(r.Context())
	if err != nil {
		h.log.Error("gc status failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	body, err := json.Marshal(st)
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", body)
}

// gcRun runs one cycle now and returns its gc.CycleStats. An optional
// ?garbage= query (a fraction) forces the selection line; absent means
// policy. A cycle already in flight is a 409.
func (h *handler) gcRun(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if !h.gcEnabled(w, a.nonce) {
		return
	}
	garbage := -1.0
	if v := r.URL.Query().Get("garbage"); v != "" {
		g, err := strconv.ParseFloat(v, 64)
		if err != nil || g < 0 || g >= 1 {
			h.signError(w, a.nonce, http.StatusUnprocessableEntity, "invalid garbage fraction "+strconv.Quote(v))
			return
		}
		garbage = g
	}
	stats, err := h.gc.Run(r.Context(), garbage)
	if errors.Is(err, gc.ErrCycleRunning) {
		h.signError(w, a.nonce, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		h.log.Error("gc run failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("gc cycle complete", "scored", stats.Scored, "reaped", len(stats.Reaped),
		"copied_bytes", stats.CopiedBytes, "freed_bytes", stats.FreedBytes, "skipped", stats.Skipped)
	body, err := json.Marshal(stats)
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", body)
}

// gcWhy serves the sorted reference names whose closure holds {key}'s tail
// as a JSON array (empty: unreferenced).
func (h *handler) gcWhy(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if !h.gcEnabled(w, a.nonce) {
		return
	}
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		h.signError(w, a.nonce, http.StatusBadRequest, err.Error())
		return
	}
	names, err := h.gc.Why(k)
	if err != nil {
		h.log.Error("gc why failed", "key", k, "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if names == nil {
		names = []string{}
	}
	body, err := json.Marshal(names)
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/json", body)
}
