package daemon

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

// preflightResponse is what `remote add` shows the user for confirmation.
type preflightResponse struct {
	PublicKey   string `json:"public_key"` // SSH wire format, base64
	Fingerprint string `json:"fingerprint"`
	KeyType     string `json:"key_type"`
}

// remotePreflight fetches and self-verifies a prospective remote's identity
// so the CLI can show its fingerprint for trust-on-first-use confirmation.
// Upstream failures are 502: the daemon is fine, the remote is not.
func (h *handler) remotePreflight(w http.ResponseWriter, r *http.Request) {
	u := r.URL.Query().Get("url")
	if u == "" {
		http.Error(w, "missing url query parameter", http.StatusUnprocessableEntity)
		return
	}
	pubWire, err := remoteclient.FetchIdentity(r.Context(), u)
	if err != nil {
		h.log.Warn("remote preflight failed", "url", u, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	pub, err := ssh.ParsePublicKey(pubWire)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(preflightResponse{
		PublicKey:   base64.StdEncoding.EncodeToString(pubWire),
		Fingerprint: ssh.FingerprintSHA256(pub),
		KeyType:     pub.Type(),
	})
}

// putRemote persists a confirmed remote: name and URL from the query, the
// user-confirmed public key in the JSON body.
func (h *handler) putRemote(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	u := r.URL.Query().Get("url")
	if name == "" || u == "" {
		http.Error(w, "missing name or url query parameter", http.StatusUnprocessableEntity)
		return
	}
	var body struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	pubWire, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if _, err := ssh.ParsePublicKey(pubWire); err != nil {
		http.Error(w, "public_key is not a valid SSH key: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	err = h.remotes.Registry.Add(name, remotes.Remote{URL: u, ServerKey: pubWire})
	switch {
	case errors.Is(err, remotes.ErrExists):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, remotes.ErrInvalid):
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	case err != nil:
		// Not a client mistake — persisting the registry failed.
		h.log.Error("remote add failed", "name", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.log.Info("remote added", "name", name, "url", u)
	w.WriteHeader(http.StatusNoContent)
}

// remoteLine is one NDJSON line of the GET /v1/remotes listing.
type remoteLine struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
}

// listRemotes writes the NDJSON listing of registered remotes, name order.
func (h *handler) listRemotes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, n := range h.remotes.Registry.All() {
		fp := "(invalid key)"
		if pub, err := ssh.ParsePublicKey(n.ServerKey); err == nil {
			fp = ssh.FingerprintSHA256(pub)
		}
		if err := enc.Encode(remoteLine{Name: n.Name, URL: n.URL, Fingerprint: fp}); err != nil {
			h.log.Error("remote list stream aborted", "error", err)
			return
		}
	}
}

// deleteRemote unregisters the remote named by ?name=.
func (h *handler) deleteRemote(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return
	}
	err := h.remotes.Registry.Remove(name)
	switch {
	case errors.Is(err, remotes.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		h.log.Info("remote removed", "name", name)
		w.WriteHeader(http.StatusNoContent)
	}
}

// Sync-route stubs, filled in by the next task.
func (h *handler) remotePushObjects(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePullObjects(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePushRef(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remotePullRef(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
func (h *handler) remoteLsRefs(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
