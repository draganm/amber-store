// Package admin serves the server's admin SPA and its JSON API: a
// password-protected browser UI (under /admin/) where an operator
// inspects, adds and removes the SSH keys allowed to talk to the
// server, and browses the server's references and their file trees.
// The password comes from the environment; sessions live in
// memory, so a restart logs every admin out.
package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/draganm/amber-store/internal/allowstore"
)

// SessionCookie is the name of the admin session cookie.
const SessionCookie = "amber_admin_session"

const (
	sessionTTL       = 12 * time.Hour
	defaultFailDelay = 500 * time.Millisecond
	maxBody          = 64 << 10 // an authorized_keys line fits comfortably
)

// Config assembles the admin handler.
type Config struct {
	Password  string            // required; the UI is only mounted when set
	Keys      *allowstore.Store // the live allowed-keys store
	Objects   ObjectGetter      // required; read-only object access for the ref browser
	Refs      RefStore          // required; read-only reference access for the ref browser
	UI        fs.FS             // built SPA; index.html at the root
	Log       *slog.Logger      // nil discards
	Secure    bool              // mark the session cookie Secure (serving TLS)
	FailDelay time.Duration     // pause after a failed login; 0 = 500ms
}

type handler struct {
	password  []byte // sha256 of the password, for constant-time compare
	keys      *allowstore.Store
	objects   ObjectGetter
	refs      RefStore
	ui        fs.FS
	log       *slog.Logger
	secure    bool
	failDelay time.Duration

	mu       sync.Mutex
	sessions map[string]time.Time // token -> expiry
}

// New returns the admin http.Handler; its routes all live under /admin/.
func New(cfg Config) (http.Handler, error) {
	if cfg.Password == "" {
		return nil, errors.New("admin UI requires a password")
	}
	if cfg.Objects == nil || cfg.Refs == nil {
		return nil, errors.New("admin UI requires the object and reference stores")
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	failDelay := cfg.FailDelay
	if failDelay == 0 {
		failDelay = defaultFailDelay
	}
	pw := sha256.Sum256([]byte(cfg.Password))
	h := &handler{
		password:  pw[:],
		keys:      cfg.Keys,
		objects:   cfg.Objects,
		refs:      cfg.Refs,
		ui:        cfg.UI,
		log:       log,
		secure:    cfg.Secure,
		failDelay: failDelay,
		sessions:  map[string]time.Time{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/api/login", h.sameOrigin(h.login))
	mux.HandleFunc("POST /admin/api/logout", h.sameOrigin(h.logout))
	mux.HandleFunc("GET /admin/api/session", h.authed(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("GET /admin/api/keys", h.authed(h.listKeys))
	mux.HandleFunc("POST /admin/api/keys", h.sameOrigin(h.authed(h.addKey)))
	mux.HandleFunc("DELETE /admin/api/keys", h.sameOrigin(h.authed(h.removeKey)))
	mux.HandleFunc("GET /admin/", h.serveUI)
	mux.Handle("GET /admin", http.RedirectHandler("/admin/", http.StatusMovedPermanently))
	return mux, nil
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// sameOrigin rejects state-changing requests whose Origin header names
// another site — defense in depth on top of the SameSite cookie.
func (h *handler) sameOrigin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && o != "null" {
			u, err := url.Parse(o)
			if err != nil || u.Host != r.Host {
				jsonError(w, http.StatusForbidden, "cross-origin request rejected")
				return
			}
		}
		next(w, r)
	}
}

// authed admits only requests carrying a live session cookie.
func (h *handler) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(SessionCookie)
		if err != nil || !h.sessionValid(c.Value) {
			jsonError(w, http.StatusUnauthorized, "not logged in")
			return
		}
		next(w, r)
	}
}

func (h *handler) sessionValid(token string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	exp, ok := h.sessions[token]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(h.sessions, token)
		return false
	}
	return true
}

func (h *handler) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Password string `json:"password"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil || json.Unmarshal(body, &req) != nil {
		jsonError(w, http.StatusBadRequest, "malformed login request")
		return
	}
	got := sha256.Sum256([]byte(req.Password))
	if subtle.ConstantTimeCompare(got[:], h.password) != 1 {
		h.log.Warn("admin login failed")
		time.Sleep(h.failDelay) // blunt brute-force damper
		jsonError(w, http.StatusUnauthorized, "wrong password")
		return
	}
	tok := make([]byte, 32)
	if _, err := rand.Read(tok); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	token := hex.EncodeToString(tok)
	now := time.Now()
	h.mu.Lock()
	for t, exp := range h.sessions {
		if now.After(exp) {
			delete(h.sessions, t)
		}
	}
	h.sessions[token] = now.Add(sessionTTL)
	h.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookie,
		Value:    token,
		Path:     "/admin",
		MaxAge:   int(sessionTTL / time.Second),
		HttpOnly: true,
		Secure:   h.secure,
		SameSite: http.SameSiteStrictMode,
	})
	h.log.Info("admin logged in")
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(SessionCookie); err == nil {
		h.mu.Lock()
		delete(h.sessions, c.Value)
		h.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookie,
		Path:     "/admin",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.secure,
		SameSite: http.SameSiteStrictMode,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) listKeys(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": h.keys.List()})
}

func (h *handler) addKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Line  string `json:"line"`
		Admin bool   `json:"admin"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil || json.Unmarshal(body, &req) != nil {
		jsonError(w, http.StatusBadRequest, "malformed request")
		return
	}
	if err := h.keys.Add(req.Line, req.Admin); err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.log.Info("admin added a key")
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) removeKey(w http.ResponseWriter, r *http.Request) {
	fp := r.URL.Query().Get("fingerprint")
	if fp == "" {
		jsonError(w, http.StatusBadRequest, "missing fingerprint")
		return
	}
	if err := h.keys.Remove(fp); err != nil {
		jsonError(w, http.StatusNotFound, err.Error())
		return
	}
	h.log.Info("admin removed a key", "fingerprint", fp)
	w.WriteHeader(http.StatusNoContent)
}

// serveUI serves the embedded SPA: real files as-is, everything else
// falls back to index.html so client-side routes deep-link.
func (h *handler) serveUI(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/admin/")
	if p != "" {
		if f, err := h.ui.Open(p); err == nil {
			f.Close()
			http.ServeFileFS(w, r, h.ui, p)
			return
		}
	}
	// Serve the index without a redirect dance: ServeFileFS on
	// "index.html" would 301 to "./"; serve its content directly.
	b, err := fs.ReadFile(h.ui, "index.html")
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "admin UI is missing from this build")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}
