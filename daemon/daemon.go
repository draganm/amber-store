// Package daemon serves the amber-store CAS over HTTP. The same http.Handler is
// transport-agnostic: a unix listener today, a TCP listener later. Routes are
// versioned under /v1.
package daemon

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/tarexport"
)

type handler struct {
	store *diskstore.Store
	log   *slog.Logger
}

// New returns an http.Handler serving the store. Every request is logged to
// logger (method, path, status, duration), as are per-operation outcomes:
// rejected uploads at Warn, store failures at Error, completed ingests at Info.
// A nil logger discards all logging.
func New(store *diskstore.Store, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{store: store, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	return logRequests(logger, mux)
}

// statusWriter records the status code a handler sends so the request log can
// include it. Unwrap keeps http.ResponseController working through the wrapper.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// logRequests logs one line per completed request.
func logRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start),
		)
	})
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
	start := time.Now()
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
			h.log.Warn("ingest rejected", "error", err)
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		h.log.Error("ingest failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("ingest complete",
		"stored", stats.Stored,
		"deduped", stats.Deduped,
		"bytes_stored", stats.BytesStored,
		"duration", time.Since(start),
	)
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
		h.log.Error("tar root lookup failed", "key", k, "error", err)
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
		h.log.Error("tar export aborted", "key", k, "error", err)
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
