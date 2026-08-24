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
	"strconv"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/remotes"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/tarexport"
	"golang.org/x/crypto/ssh"
)

type handler struct {
	store   *packstore.Store
	refs    *refstore.Store
	log     *slog.Logger
	remotes *RemoteConfig
	gc      *gc.Collector // nil: --gc=false — no walk on ref writes, gc routes 503
}

// An Option tweaks the daemon handler.
type Option func(*handler)

// WithCollector wires the garbage collector: PUT /v1/refs gains the
// completeness walk (a missing object fails the write with a 404 naming
// it), DELETE releases the root, pulls hold upload leases, and the /v1/gc
// routes serve status, cycles and liveness queries. The collector must sit
// over the same store and refs, and outlive the handler.
func WithCollector(c *gc.Collector) Option {
	return func(h *handler) { h.gc = c }
}

// RemoteConfig wires the daemon's remote-sync support: the persistent remote
// registry and the SSH identities (held in memory, resolved at daemon start)
// used to sign requests to remote servers.
type RemoteConfig struct {
	Registry      *remotes.Registry
	DefaultSigner ssh.Signer            // used for remotes without an override; may be nil
	Signers       map[string]ssh.Signer // per-remote overrides by remote name
}

// signerFor picks the identity for a remote: the per-remote override, else
// the default, else an error.
func (rc *RemoteConfig) signerFor(name string) (ssh.Signer, error) {
	if s, ok := rc.Signers[name]; ok {
		return s, nil
	}
	if rc.DefaultSigner != nil {
		return rc.DefaultSigner, nil
	}
	return nil, errors.New("no remote signing key configured")
}

// New returns an http.Handler serving the store. Every request is logged to
// logger (method, path, status, duration), as are per-operation outcomes:
// rejected uploads at Warn, store failures at Error, completed ingests at Info.
// A nil logger discards all logging.
func New(store *packstore.Store, refs *refstore.Store, logger *slog.Logger, opts ...Option) http.Handler {
	return NewWithRemotes(store, refs, logger, nil, opts...)
}

// NewWithRemotes additionally registers the /v1/remotes and /v1/remote/*
// routes backed by rc.
func NewWithRemotes(store *packstore.Store, refs *refstore.Store, logger *slog.Logger, rc *RemoteConfig, opts ...Option) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{store: store, refs: refs, log: logger, remotes: rc}
	for _, opt := range opts {
		opt(h)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/objects", h.postObjects)
	mux.HandleFunc("GET /v1/tar/{key}", h.getTar)
	mux.HandleFunc("GET /v1/file/{key}", h.getFile)
	mux.HandleFunc("GET /v1/ls/{key}", h.getLs)
	mux.HandleFunc("GET /v1/content-keys/{key}", h.getContentKeys)
	mux.HandleFunc("PUT /v1/refs", h.putRef)
	mux.HandleFunc("GET /v1/refs", h.getRefs)
	mux.HandleFunc("DELETE /v1/refs", h.deleteRef)
	mux.HandleFunc("GET /v1/gc", h.gcStatus)
	mux.HandleFunc("POST /v1/gc/run", h.gcRun)
	mux.HandleFunc("GET /v1/gc/why/{key}", h.gcWhy)
	if rc != nil {
		mux.HandleFunc("POST /v1/remotes/preflight", h.remotePreflight)
		mux.HandleFunc("PUT /v1/remotes", h.putRemote)
		mux.HandleFunc("GET /v1/remotes", h.listRemotes)
		mux.HandleFunc("DELETE /v1/remotes", h.deleteRemote)
		mux.HandleFunc("POST /v1/remote/push", h.remotePush)
		mux.HandleFunc("POST /v1/remote/pull", h.remotePull)
		mux.HandleFunc("GET /v1/remote/refs", h.remoteLsRefs)
	}
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

// getTar streams a PAX tar of the directory tree rooted at the {key} path
// value. An optional ?path= query names a subdirectory of {key}
// (slash-separated) to tar instead of {key} itself.
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
	if path := r.URL.Query().Get("path"); path != "" {
		k, err = fstree.ResolvePath(k, path, h.store.Get)
		switch {
		case errors.Is(err, fstree.ErrNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, fstree.ErrNotDir):
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		case err != nil:
			h.log.Error("tar path resolution failed", "key", k, "path", path, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarexport.Write(w, k, h.store.Get); err != nil {
		// The 200 status and some bytes may already be in flight; we cannot change
		// the status now. Log and let the truncated archive surface as a tar read
		// error on the client.
		h.log.Error("tar export aborted", "key", k, "error", err)
	}
}

// getContentKeys lists the keys of every object reachable from {key} — the set
// a client must fetch to hold the whole content — as plain text, one
// lowercase-hex key per line, root first, each key once. {key} may be any
// object type. An optional ?path= query names a subdirectory of {key}
// (slash-separated) to walk instead of {key} itself.
func (h *handler) getContentKeys(w http.ResponseWriter, r *http.Request) {
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		h.log.Error("content-keys root lookup failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "root object not found", http.StatusNotFound)
		return
	}
	if path := r.URL.Query().Get("path"); path != "" {
		if k.Type() != key.DirLeaf && k.Type() != key.DirNode {
			http.Error(w, "key is not a directory object", http.StatusBadRequest)
			return
		}
		k, err = fstree.ResolvePath(k, path, h.store.Get)
		switch {
		case errors.Is(err, fstree.ErrNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, fstree.ErrNotDir):
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		case err != nil:
			h.log.Error("content-keys path resolution failed", "key", k, "path", path, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// Walk fully before writing so a failure surfaces as a 500, not a truncated
	// 200 stream.
	keys, err := fstree.ReachableKeys(k, h.store.Get)
	if err != nil {
		h.log.Error("content-keys walk failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, rk := range keys {
		if _, err := fmt.Fprintln(w, rk.String()); err != nil {
			// Bytes are already in flight; log and stop, like getTar.
			h.log.Error("content-keys stream aborted", "key", k, "error", err)
			return
		}
	}
}

// lsEntry is one NDJSON line of the GET /v1/ls/{key} response. Size is the
// logical content length for regular files and directories (from the content
// key) and the target length for symlinks. Key is the hex content key of
// regular-file and directory entries, usable for further /v1/ls or /v1/tar
// requests.
type lsEntry struct {
	Name       string   `json:"name"`
	Mode       uint64   `json:"mode"`
	UID        uint64   `json:"uid"`
	GID        uint64   `json:"gid"`
	MtimeNs    int64    `json:"mtime_ns"`
	Size       uint64   `json:"size"`
	Key        string   `json:"key,omitempty"`
	LinkTarget string   `json:"link_target,omitempty"`
	Rdev       []uint64 `json:"rdev,omitempty"`
}

// getLs streams the entries of the directory object {key} as NDJSON, one JSON
// object per line. An optional ?path= query names a subdirectory of {key}
// (slash-separated) to list instead of {key} itself. DirNode index levels are
// flattened; the response lists the directory's immediate entries in name
// order.
func (h *handler) getLs(w http.ResponseWriter, r *http.Request) {
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
		h.log.Error("ls root lookup failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "directory object not found", http.StatusNotFound)
		return
	}
	if path := r.URL.Query().Get("path"); path != "" {
		k, err = fstree.ResolvePath(k, path, h.store.Get)
		switch {
		case errors.Is(err, fstree.ErrNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, fstree.ErrNotDir):
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		case err != nil:
			h.log.Error("ls path resolution failed", "key", k, "path", path, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	entries, err := fstree.CollectEntries(k, h.store.Get)
	if err != nil {
		h.log.Error("ls failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Convert before writing so a malformed entry surfaces as a 500, not a
	// truncated 200 stream.
	lines := make([]lsEntry, len(entries))
	for i, ent := range entries {
		le := lsEntry{
			Name:    string(ent.Name),
			Mode:    ent.Mode,
			UID:     ent.UID,
			GID:     ent.GID,
			MtimeNs: ent.Mtime,
			Rdev:    ent.Rdev,
		}
		if len(ent.ContentKey) > 0 {
			ck, err := key.Parse(ent.ContentKey)
			if err != nil {
				h.log.Error("ls entry has invalid content key", "key", k, "name", le.Name, "error", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			le.Key = ck.String()
			le.Size = ck.Length()
		}
		if len(ent.LinkTarget) > 0 {
			le.LinkTarget = string(ent.LinkTarget)
			le.Size = uint64(len(ent.LinkTarget))
		}
		lines[i] = le
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, le := range lines {
		if err := enc.Encode(le); err != nil {
			// Bytes are already in flight; log and stop, like getTar.
			h.log.Error("ls stream aborted", "key", k, "error", err)
			return
		}
	}
}

// parseHexKey decodes a lowercase-hex key path segment into a validated key.
// getFile streams the reconstructed bytes of a single regular-file content
// object (Blob or FileNode) named by its content key. The content key uniquely
// identifies the bytes, so no path resolution is needed; callers obtain it from
// an ls entry. Content-Length is set from the key's encoded length, so a
// truncated body (mid-stream store failure) surfaces as a read error on the
// client rather than a silent short read.
func (h *handler) getFile(w http.ResponseWriter, r *http.Request) {
	k, err := parseHexKey(r.PathValue("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if k.Type() != key.Blob && k.Type() != key.FileNode {
		http.Error(w, "key is not a file-content object", http.StatusBadRequest)
		return
	}
	has, err := h.store.Has(k)
	if err != nil {
		h.log.Error("file root lookup failed", "key", k, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !has {
		http.Error(w, "file object not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatUint(k.Length(), 10))
	if err := fstree.WriteContent(w, k, h.store.Get); err != nil {
		// The 200 status and some bytes may already be in flight; we cannot change
		// the status now. Log and let the truncated body surface as a read error
		// on the client, like getTar.
		h.log.Error("file export aborted", "key", k, "error", err)
	}
}

func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return key.Parse(raw)
}
