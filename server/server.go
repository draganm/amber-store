// Package server implements the amber-store remote server: a TCP HTTP(S)
// sibling of the local daemon that other amber daemons push objects and
// references to and pull them from. Every request must carry a valid
// signature by an allowed SSH key (internal/httpsig); every response is
// signed with the server's identity key so clients can enforce their pinned
// key. See architecture/remote.md.
package server

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/nonces"
	"github.com/draganm/amber-store/refstore"
	"golang.org/x/crypto/ssh"
)

// DefaultMaxBody caps a request body; batches are sized well below it.
const DefaultMaxBody = 64 << 20 // 64 MiB

// Config assembles a server handler.
type Config struct {
	Store    *packstore.Store
	Refs     *refstore.Store
	Allow    func() *allowlist.List // called per request, enabling hot reload
	Identity ssh.Signer
	Log      *slog.Logger  // nil discards
	Window   time.Duration // timestamp validity window; 0 = httpsig.DefaultWindow
	MaxBody  int64         // request body cap; 0 = DefaultMaxBody
	Inbox    *inbox.Inbox  // receives pushed packs; required
}

type handler struct {
	store    *packstore.Store
	refs     *refstore.Store
	allow    func() *allowlist.List
	identity ssh.Signer
	log      *slog.Logger
	window   time.Duration
	maxBody  int64
	inbox    *inbox.Inbox
	nonces   *nonces.Cache
}

// New returns the remote server's http.Handler.
func New(cfg Config) http.Handler {
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	window := cfg.Window
	if window == 0 {
		window = httpsig.DefaultWindow
	}
	maxBody := cfg.MaxBody
	if maxBody == 0 {
		maxBody = DefaultMaxBody
	}
	h := &handler{
		store:    cfg.Store,
		refs:     cfg.Refs,
		allow:    cfg.Allow,
		identity: cfg.Identity,
		log:      log,
		window:   window,
		maxBody:  maxBody,
		inbox:    cfg.Inbox,
		nonces:   nonces.New(window),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", h.getIdentity)
	mux.HandleFunc("POST /v1/objects/missing", h.auth(h.postMissing))
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("POST /v1/objects/get", h.auth(h.postObjectsGet))
	mux.HandleFunc("POST /v1/objects/reachable", h.auth(h.postObjectsReachable))
	mux.HandleFunc("PUT /v1/refs", h.auth(h.putRef))
	mux.HandleFunc("GET /v1/refs", h.auth(h.getRefs))
	mux.HandleFunc("DELETE /v1/refs", h.auth(h.deleteRef))
	return logRequests(log, mux)
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

// authedRequest is what the middleware hands an authenticated handler.
type authedRequest struct {
	pubWire []byte // the client's key, SSH wire format
	admin   bool
	nonce   []byte // the request nonce; responses sign over it
	body    []byte // the fully-read request body
}

type authedHandler func(w http.ResponseWriter, r *http.Request, a *authedRequest)

// auth reads the (size-capped) body, verifies the request signature, checks
// the nonce for replay and the key against the allowlist — all before the
// wrapped handler can cause any side effect. Bad signature/timestamp/replay
// are 401; a valid signature by an unlisted key is 403.
func (h *handler) auth(next authedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBody+1))
		if err != nil {
			h.signError(w, nil, http.StatusInternalServerError, err.Error())
			return
		}
		if int64(len(body)) > h.maxBody {
			h.signError(w, nil, http.StatusRequestEntityTooLarge, "request body exceeds the server limit")
			return
		}
		now := time.Now()
		pub, nonce, err := httpsig.VerifyRequest(r, body, now, h.window)
		if err != nil {
			h.log.Warn("request authentication failed", "error", err)
			h.signError(w, nonce, http.StatusUnauthorized, err.Error())
			return
		}
		// Replay check after signature verification so unauthenticated junk
		// cannot grow the nonce cache.
		if h.nonces.SeenBefore(ssh.FingerprintSHA256(pub), nonce, now) {
			h.log.Warn("replayed nonce", "key", ssh.FingerprintSHA256(pub))
			h.signError(w, nonce, http.StatusUnauthorized, "replayed nonce")
			return
		}
		ent, ok := h.allow().Lookup(pub.Marshal())
		if !ok {
			h.log.Warn("key not allowed", "key", ssh.FingerprintSHA256(pub))
			h.signError(w, nonce, http.StatusForbidden, "public key is not in the server allowlist")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r, &authedRequest{pubWire: pub.Marshal(), admin: ent.Admin, nonce: nonce, body: body})
	}
}

// getIdentity serves the server's public key (SSH wire format),
// self-signed: trust comes from the user confirming the fingerprint at
// `remote add`, not from this signature.
func (h *handler) getIdentity(w http.ResponseWriter, r *http.Request) {
	h.signAndWrite(w, nil, http.StatusOK, "application/octet-stream", h.identity.PublicKey().Marshal())
}
