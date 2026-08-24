package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// maxRefRecord bounds a PUT /v1/refs body: a record is a 1 KiB name plus a
// few small fields and a signature; 1 MiB is generous.
const maxRefRecord = 1 << 20

// refName extracts and validates the ?name= query parameter; on failure it
// writes a 422 and returns false.
func refName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return "", false
	}
	if err := reference.ValidateName(name); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return "", false
	}
	return name, true
}

// putRef stores the CBOR reference record from the body under ?name=,
// overwriting unconditionally. The record must decode, match the query name,
// and carry a canonical key. With the collector running, the pointed-to
// content must be complete — every object reachable from the key exists — and
// the write lands under the removal lock (the optimistic reference PUT: a 404
// names a missing object; the caller re-sends it and retries). Without it
// (--gc=false) only the key itself is checked, as before.
func (h *handler) putRef(w http.ResponseWriter, r *http.Request) {
	name, ok := refName(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRefRecord+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(body) > maxRefRecord {
		http.Error(w, "reference record too large", http.StatusUnprocessableEntity)
		return
	}
	rec, err := reference.Decode(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if rec.Name != name {
		http.Error(w, "record name does not match query name", http.StatusUnprocessableEntity)
		return
	}
	k, err := key.Parse(rec.Key) // canonical: Decode validated it
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	// The body is stored verbatim: it is the canonical encoding (Decode
	// rejects non-canonical bytes) and preserves the signature bytes
	// untouched.
	if h.gc != nil {
		err := h.gc.PutRef(name, k, body)
		var miss *fstree.MissingObjectError
		switch {
		case errors.As(err, &miss):
			http.Error(w, "referenced content is incomplete: "+miss.Key.String()+
				" is missing — store the objects, then retry", http.StatusNotFound)
			return
		case errors.Is(err, packstore.ErrNotFound):
			http.Error(w, "referenced content is incomplete: "+err.Error(), http.StatusNotFound)
			return
		case err != nil:
			h.log.Error("ref put failed", "name", name, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		h.log.Info("reference stored", "name", name, "key", k)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		h.log.Error("ref key lookup failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "referenced key not found in store", http.StatusNotFound)
		return
	}
	if err := h.refs.Put(name, body); err != nil {
		h.log.Error("ref put failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("reference stored", "name", name, "key", k)
	w.WriteHeader(http.StatusNoContent)
}

// refLine is one NDJSON line of the GET /v1/refs listing.
type refLine struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"` // RFC 3339
	Signed    bool   `json:"signed"`
}

// getRefs serves a single record (?name=, application/cbor) or, without a
// name parameter, the NDJSON listing of all references in name order.
func (h *handler) getRefs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("name") == "" {
		h.listRefs(w)
		return
	}
	name, ok := refName(w, r)
	if !ok {
		return
	}
	data, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.log.Error("ref get failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/cbor")
	_, _ = w.Write(data)
}

// listRefs writes the NDJSON listing. Records are decoded fully before the
// first byte so a malformed record surfaces as a 500, not a truncated 200.
func (h *handler) listRefs(w http.ResponseWriter) {
	recs, err := h.refs.All()
	if err != nil {
		h.log.Error("ref list failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lines := make([]refLine, len(recs))
	for i, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			h.log.Error("stored reference is malformed", "name", rec.Name, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			h.log.Error("stored reference has invalid key", "name", rec.Name, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		lines[i] = refLine{
			Name:      ref.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Signed:    len(ref.Signature) > 0,
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, l := range lines {
		if err := enc.Encode(l); err != nil {
			h.log.Error("ref list stream aborted", "error", err)
			return
		}
	}
}

// deleteRef removes the reference named by ?name=. With the collector
// running the root is released: its tails leave the union, its closure file
// goes if no other name shares it.
func (h *handler) deleteRef(w http.ResponseWriter, r *http.Request) {
	name, ok := refName(w, r)
	if !ok {
		return
	}
	var err error
	if h.gc != nil {
		err = h.gc.DeleteRef(name)
	} else {
		err = h.refs.Delete(name)
	}
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.log.Error("ref delete failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("reference deleted", "name", name)
	w.WriteHeader(http.StatusNoContent)
}
