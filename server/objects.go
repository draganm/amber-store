package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/zeebo/blake3"
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
	missing, err := h.store.Missing(keys)
	if err != nil {
		h.log.Error("missing-check lookup failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", keylist.Flatten(missing))
}

// postObjects decodes an amberpack stream, verifies each object's payload
// against its key, and stores it — the same trust boundary as the local
// daemon: nothing unverified is ever persisted.
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	rd := amberpack.NewReader(bytes.NewReader(a.body))
	seq := func(yield func(packstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(packstore.Object{}, err)
				return
			}
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	stats, err := h.store.WriteParallel(seq, packstore.WriteOpts{Verify: true})
	if err != nil {
		if errors.Is(err, amberpack.ErrMalformed) || errors.Is(err, packstore.ErrVerify) {
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

// postObjectsGet streams an amberpack of the requested keys. Existence is
// checked for every key before the first body byte (the project-wide
// do-the-work-before-streaming convention), so an absent key is a clean 404
// naming the missing keys. The response signature travels in an HTTP trailer
// because the body hash is only known at the end; a mid-stream failure cuts
// the stream, which clients surface as a missing/invalid trailer signature.
func (h *handler) postObjectsGet(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	keys, err := keylist.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	missing, err := h.store.Missing(keys)
	if err != nil {
		h.log.Error("objects/get lookup failed", "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if len(missing) > 0 {
		names := make([]string, len(missing))
		for i, k := range missing {
			names[i] = k.String()
		}
		h.signError(w, a.nonce, http.StatusNotFound, "objects not found:\n"+strings.Join(names, "\n"))
		return
	}
	// Declare the trailer name up-front so HTTP/1.1 chunked encoding includes
	// it; http.TrailerPrefix alone is only sufficient for HTTP/2.
	w.Header().Set("Trailer", httpsig.HeaderSignature)
	w.Header().Set("Content-Type", "application/octet-stream")
	hasher := blake3.New()
	pw := amberpack.NewWriter(io.MultiWriter(w, hasher))
	for _, k := range keys {
		data, err := h.store.Get(k)
		if err != nil {
			// Bytes are already in flight; cut the stream without a trailer.
			h.log.Error("objects/get stream aborted", "key", k, "error", err)
			return
		}
		if err := pw.Add(fstree.Object{Key: k, Bytes: data}); err != nil {
			h.log.Error("objects/get stream aborted", "key", k, "error", err)
			return
		}
	}
	if err := pw.Close(); err != nil {
		h.log.Error("objects/get stream aborted", "error", err)
		return
	}
	sig, err := httpsig.SignResponse(h.identity, a.nonce, http.StatusOK, hasher.Sum(nil))
	if err != nil {
		h.log.Error("signing objects/get trailer failed", "error", err)
		return
	}
	w.Header().Set(http.TrailerPrefix+httpsig.HeaderSignature, sig)
}
