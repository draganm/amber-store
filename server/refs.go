package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// refName extracts and validates the ?name= query parameter.
func refName(r *http.Request) (string, error) {
	name := r.URL.Query().Get("name")
	if name == "" {
		return "", errors.New("missing name query parameter")
	}
	if err := reference.ValidateName(name); err != nil {
		return "", err
	}
	return name, nil
}

// putRef stores a reference record after the full validation chain:
// canonical record matching the query name; a verifying signature (the
// record MUST be signed — the signer key owns the name); ownership — an
// existing name may only be overwritten by the same signer key, unless the
// transport key is an admin; and the pointed-to content must be complete —
// every object reachable from the key exists (push objects before the ref).
func (h *handler) putRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rec, err := reference.Decode(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if rec.Name != name {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "record name does not match query name")
		return
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "reference record is not signed; the server only accepts signed references")
		return
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := sshsign.Verify(payload, rec.Signature, rec.PublicKey); err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, "reference signature does not verify: "+err.Error())
		return
	}
	existing, err := h.refs.Get(name)
	switch {
	case err == nil:
		old, derr := reference.Decode(existing)
		if derr != nil {
			h.log.Error("stored reference is malformed", "name", name, "error", derr)
			h.signError(w, a.nonce, http.StatusInternalServerError, derr.Error())
			return
		}
		if !bytes.Equal(old.PublicKey, rec.PublicKey) && !a.admin {
			h.signError(w, a.nonce, http.StatusForbidden, "reference is owned by a different signer key")
			return
		}
	case errors.Is(err, refstore.ErrNotFound):
		// fresh name
	default:
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	// Uploads are acked as soon as they are durably staged, not stored. Wait for
	// any packs tagged with this root to finish processing before checking
	// completeness; a pack that failed to process was quarantined, so its
	// objects stay absent and CheckComplete reports the ref incomplete.
	h.inbox.WaitFor(k)
	// The referenced content must be complete: every object reachable from
	// the key must exist in the store. The walk runs parallel lookups —
	// referenced trees can be large.
	err = fstree.CheckComplete(k, h.store.Get, h.store.Has, 0)
	var miss *fstree.MissingObjectError
	switch {
	case errors.As(err, &miss):
		h.signError(w, a.nonce, http.StatusNotFound, "referenced content is incomplete: "+miss.Key.String()+" is missing — push objects before the ref")
		return
	case errors.Is(err, packstore.ErrNotFound):
		h.signError(w, a.nonce, http.StatusNotFound, "referenced content is incomplete: "+err.Error()+" — push objects before the ref")
		return
	case err != nil:
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.refs.Put(name, a.body); err != nil {
		h.log.Error("ref put failed", "name", name, "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("reference stored", "name", name, "key", k)
	h.signAndWrite(w, a.nonce, http.StatusNoContent, "", nil)
}

// refLine mirrors the local daemon's listing shape.
type refLine struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signed    bool   `json:"signed"`
}

// getRefs serves a single record (?name=) or the NDJSON listing.
func (h *handler) getRefs(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if r.URL.Query().Get("name") == "" {
		h.listRefs(w, a)
		return
	}
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	data, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		h.signError(w, a.nonce, http.StatusNotFound, "reference not found")
		return
	}
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/cbor", data)
}

// listRefs builds the full NDJSON listing in memory (records are small and
// the body must be hashed for the response signature anyway).
func (h *handler) listRefs(w http.ResponseWriter, a *authedRequest) {
	recs, err := h.refs.All()
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			h.log.Error("stored reference is malformed", "name", rec.Name, "error", err)
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
		if err := enc.Encode(refLine{
			Name:      ref.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Signed:    len(ref.Signature) > 0,
		}); err != nil {
			h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
			return
		}
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/x-ndjson", buf.Bytes())
}

// deleteRef removes a reference. Ownership lives in the ref signature, but a
// DELETE carries no record to sign, so deletion is restricted to admin
// transport keys (spec §4).
func (h *handler) deleteRef(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	if !a.admin {
		h.signError(w, a.nonce, http.StatusForbidden, "reference deletion requires an admin key")
		return
	}
	name, err := refName(r)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	err = h.refs.Delete(name)
	if errors.Is(err, refstore.ErrNotFound) {
		h.signError(w, a.nonce, http.StatusNotFound, "reference not found")
		return
	}
	if err != nil {
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("reference deleted", "name", name)
	h.signAndWrite(w, a.nonce, http.StatusNoContent, "", nil)
}
