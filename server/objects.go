package server

import (
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
	"golang.org/x/crypto/ssh"
)

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

// postObjects receives a pushed pack. It streams the body into the inbox while
// hashing it, authenticates the request against that hash (the body is never
// fully buffered, so a pack is bounded by disk, not memory), and — once the
// pack is durably staged — returns 200. Processing into the store happens
// asynchronously; setting the ref waits for it (refs.go).
func (h *handler) postObjects(w http.ResponseWriter, r *http.Request) {
	rootHex := r.URL.Query().Get("root")
	rootBytes, err := hex.DecodeString(rootHex)
	if err != nil {
		h.signError(w, nil, http.StatusUnprocessableEntity, "invalid root query parameter: "+err.Error())
		return
	}
	root, err := key.Parse(rootBytes)
	if err != nil {
		h.signError(w, nil, http.StatusUnprocessableEntity, "invalid root key: "+err.Error())
		return
	}
	meta := inbox.Meta{Ref: r.URL.Query().Get("ref"), Root: root[:], ReceivedAt: time.Now().UnixNano()}
	tmp, bodyHash, n, err := h.inbox.Stage(meta, io.LimitReader(r.Body, h.maxBody+1))
	if err != nil {
		h.signError(w, nil, http.StatusInternalServerError, err.Error())
		return
	}
	if n > h.maxBody {
		h.inbox.Discard(tmp)
		h.signError(w, nil, http.StatusRequestEntityTooLarge, "request body exceeds the server limit")
		return
	}
	now := time.Now()
	pub, nonce, err := httpsig.VerifyRequestHash(r, bodyHash, now, h.window)
	if err != nil {
		h.inbox.Discard(tmp)
		h.log.Warn("request authentication failed", "error", err)
		h.signError(w, nonce, http.StatusUnauthorized, err.Error())
		return
	}
	if h.nonces.SeenBefore(ssh.FingerprintSHA256(pub), nonce, now) {
		h.inbox.Discard(tmp)
		h.log.Warn("replayed nonce", "key", ssh.FingerprintSHA256(pub))
		h.signError(w, nonce, http.StatusUnauthorized, "replayed nonce")
		return
	}
	if _, ok := h.allow().Lookup(pub.Marshal()); !ok {
		h.inbox.Discard(tmp)
		h.log.Warn("key not allowed", "key", ssh.FingerprintSHA256(pub))
		h.signError(w, nonce, http.StatusForbidden, "public key is not in the server allowlist")
		return
	}
	if _, err := h.inbox.Commit(tmp, bodyHash, root); err != nil {
		h.inbox.Discard(tmp)
		h.log.Error("inbox commit failed", "error", err)
		h.signError(w, nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.log.Info("pack accepted", "root", root, "bytes", n)
	h.signAndWrite(w, nonce, http.StatusOK, "application/json", []byte(`{"accepted":true}`))
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

// postObjectsReachable walks the tree under the requested root key and returns
// the full set of reachable keys as raw concatenated 32-byte keys. The request
// body is the 32-byte root key. The walk runs to completion before any byte is
// written, so an incomplete tree (a reachable object missing from the store)
// surfaces as a clean 500 rather than a truncated body. The response is
// header-signed; pull uses it to learn the whole key set before fetching.
func (h *handler) postObjectsReachable(w http.ResponseWriter, r *http.Request, a *authedRequest) {
	root, err := key.Parse(a.body)
	if err != nil {
		h.signError(w, a.nonce, http.StatusUnprocessableEntity, err.Error())
		return
	}
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		h.log.Error("reachable walk failed", "root", root, "error", err)
		h.signError(w, a.nonce, http.StatusInternalServerError, err.Error())
		return
	}
	h.signAndWrite(w, a.nonce, http.StatusOK, "application/octet-stream", keylist.Flatten(keys))
}
