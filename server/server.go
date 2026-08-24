// Package server implements the amber-store remote server: a TCP HTTP(S)
// sibling of the local daemon that other amber daemons push objects and
// references to and pull them from. Every request must carry a valid
// signature by an allowed SSH key (internal/httpsig); requests are then
// authorized against the server's allowlist capabilities or a delegate-issued
// capability grant (package grant); every response is signed with the
// server's identity key so clients can enforce their pinned key. See
// architecture/remote.md.
package server

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/httpsig"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/nonces"
	"github.com/draganm/amber-store/packstore"
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
	// GC, when non-nil, wires the garbage collector: reference writes go
	// through the removal lock and the completeness walk, deletes release
	// their root, pushed roots hold upload leases, a wipe empties the
	// closures, and the /v1/gc routes are live. Must sit over the same
	// Store and Refs and outlive the handler, and Inbox must have been
	// opened with inbox.WithLeaser(inbox.LeaserOf(GC.Lease)) — New panics
	// otherwise.
	GC *gc.Collector
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
	gc       *gc.Collector // nil: gc disabled
	nonces   *nonces.Cache

	// wipeMu serializes the wipe endpoint against every mutating handler:
	// writers hold it shared, postWipe exclusively.
	wipeMu sync.RWMutex
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
		gc:       cfg.GC,
		nonces:   nonces.New(window),
	}
	if h.gc != nil && !h.inbox.Leased() {
		// Not recoverable here: leases for entries recovered at inbox.Open
		// are taken inside Open, before its workers start; wiring them now
		// would leave those roots covered by grace alone (the safety
		// argument of architecture/simple-gc.md would not hold).
		panic("server: Config.GC is set but Config.Inbox was opened without inbox.WithLeaser(inbox.LeaserOf(GC.Lease))")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", h.getIdentity)
	mux.HandleFunc("POST /v1/objects/missing", h.auth(allowlist.CapRead, h.postMissing))
	mux.HandleFunc("POST /v1/objects", h.postObjects) // streams; authorizes inline, needs push-objects
	mux.HandleFunc("POST /v1/objects/get", h.auth(allowlist.CapRead, h.postObjectsGet))
	mux.HandleFunc("POST /v1/objects/reachable", h.auth(allowlist.CapRead, h.postObjectsReachable))
	mux.HandleFunc("PUT /v1/refs", h.auth(allowlist.CapWriteRefs, h.putRef))
	mux.HandleFunc("GET /v1/refs", h.auth(allowlist.CapRead, h.getRefs))
	mux.HandleFunc("DELETE /v1/refs", h.auth(allowlist.CapAdmin, h.deleteRef))
	mux.HandleFunc("POST /v1/wipe", h.auth(allowlist.CapWipe, h.postWipe))
	mux.HandleFunc("GET /v1/gc", h.auth(allowlist.CapRead, h.gcStatus))
	mux.HandleFunc("POST /v1/gc/run", h.auth(allowlist.CapAdmin, h.gcRun))
	mux.HandleFunc("GET /v1/gc/why/{key}", h.auth(allowlist.CapRead, h.gcWhy))
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
	pubWire []byte          // the client's key, SSH wire format
	entry   allowlist.Entry // effective capabilities (allowlist entry or grant)
	nonce   []byte          // the request nonce; responses sign over it
	body    []byte          // the fully-read request body
}

type authedHandler func(w http.ResponseWriter, r *http.Request, a *authedRequest)

// authorize resolves the request key to its effective capabilities: an
// allowlisted key uses its entry; an unlisted key may present a capability
// grant (Amber-Grant header) minted by an allowlisted delegate. Anything else
// is a 403-worthy error.
func (h *handler) authorize(pub ssh.PublicKey, r *http.Request, now time.Time) (allowlist.Entry, error) {
	if ent, ok := h.allow().Lookup(pub.Marshal()); ok {
		return ent, nil
	}
	gB64 := r.Header.Get(grant.Header)
	if gB64 == "" {
		return allowlist.Entry{}, errors.New("public key is not in the server allowlist")
	}
	raw, err := base64.StdEncoding.DecodeString(gB64)
	if err != nil {
		return allowlist.Entry{}, fmt.Errorf("decoding %s: %w", grant.Header, err)
	}
	g, issuerWire, err := grant.Verify(raw, pub.Marshal(), now, h.window)
	if err != nil {
		return allowlist.Entry{}, fmt.Errorf("capability grant: %w", err)
	}
	issuer, ok := h.allow().Lookup(issuerWire)
	if !ok || !issuer.Allows(allowlist.CapDelegate) {
		return allowlist.Entry{}, errors.New("grant issuer is not an allowlisted delegate")
	}
	return allowlist.ParseCaps(g.Caps)
}

// auth reads the (size-capped) body, verifies the request signature, checks
// the nonce for replay and resolves the key's effective capabilities
// (allowlist or grant) against the route's required capability — all before
// the wrapped handler can cause any side effect. Bad signature/timestamp/
// replay are 401; a valid signature that is not authorized, or lacks the
// required capability, is 403.
func (h *handler) auth(need string, next authedHandler) http.HandlerFunc {
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
		ent, err := h.authorize(pub, r, now)
		if err != nil {
			h.log.Warn("key not authorized", "key", ssh.FingerprintSHA256(pub), "error", err)
			h.signError(w, nonce, http.StatusForbidden, err.Error())
			return
		}
		if !ent.Allows(need) {
			h.signError(w, nonce, http.StatusForbidden, "key lacks the "+need+" capability")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r, &authedRequest{pubWire: pub.Marshal(), entry: ent, nonce: nonce, body: body})
	}
}

// getIdentity serves the server's public key (SSH wire format),
// self-signed: trust comes from the user confirming the fingerprint at
// `remote add`, not from this signature.
func (h *handler) getIdentity(w http.ResponseWriter, r *http.Request) {
	h.signAndWrite(w, nil, http.StatusOK, "application/octet-stream", h.identity.PublicKey().Marshal())
}
