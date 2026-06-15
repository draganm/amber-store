package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/remotesync"
)

// remoteFor resolves the ?remote= query (empty selects the sole remote) into
// a connected client.
func (h *handler) remoteFor(r *http.Request) (*remoteclient.Client, int, error) {
	name, rem, err := h.remotes.Registry.Resolve(r.URL.Query().Get("remote"))
	if err != nil {
		status := http.StatusUnprocessableEntity
		if errors.Is(err, remotes.ErrNotFound) {
			status = http.StatusNotFound
		}
		return nil, status, err
	}
	signer, err := h.remotes.signerFor(name)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}
	rc, err := remoteclient.New(rem.URL, signer, rem.ServerKey)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}
	return rc, 0, nil
}

// syncOpts reads optional ?jobs= and ?batch_bytes= query parameters.
func syncOpts(r *http.Request) (remotesync.Opts, error) {
	var opts remotesync.Opts
	if v := r.URL.Query().Get("jobs"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return opts, fmt.Errorf("invalid jobs %q", v)
		}
		opts.Jobs = n
	}
	if v := r.URL.Query().Get("batch_bytes"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return opts, fmt.Errorf("invalid batch_bytes %q", v)
		}
		opts.BatchBytes = n
	}
	return opts, nil
}

// syncEvent is one NDJSON line of a push/pull response. Progress
// lines carry done/total; the final line carries stats or an error. The 200
// status is committed before the work runs, so failures surface here.
type syncEvent struct {
	Done      int            `json:"done,omitempty"`
	Total     int            `json:"total,omitempty"`
	PushStats *pushStatsLine `json:"push_stats,omitempty"`
	PullStats *pullStatsLine `json:"pull_stats,omitempty"`
	Key       string         `json:"key,omitempty"` // pull: the resolved root
	Error     string         `json:"error,omitempty"`
}

type pushStatsLine struct {
	ObjectsTotal  int   `json:"objects_total"`
	ObjectsPushed int   `json:"objects_pushed"`
	BytesPushed   int64 `json:"bytes_pushed"`
}

type pullStatsLine struct {
	ObjectsFetched int   `json:"objects_fetched"`
	BytesFetched   int64 `json:"bytes_fetched"`
}

// eventStream serializes NDJSON events and flushes each one so the CLI can
// render live progress. Safe for concurrent Progress callbacks.
type eventStream struct {
	mu  sync.Mutex
	enc *json.Encoder
	rc  *http.ResponseController
}

func newEventStream(w http.ResponseWriter) *eventStream {
	w.Header().Set("Content-Type", "application/x-ndjson")
	return &eventStream{enc: json.NewEncoder(w), rc: http.NewResponseController(w)}
}

func (s *eventStream) send(ev syncEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(ev)
	_ = s.rc.Flush()
}

// remotePush pushes everything reachable from the local ref ?name= to the
// remote and then uploads the signed reference — one operation, streaming
// NDJSON progress. The signed record is read and validated up front, so a
// missing or unsigned reference is a clean 404/422 before any object moves.
// Once the stream is open the 200 is committed, so a later failure — including a
// remote PutRef rejection such as an ownership 403 — is delivered as a
// syncEvent{Error} line, not an HTTP status.
func (h *handler) remotePush(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query parameter", http.StatusUnprocessableEntity)
		return
	}
	raw, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		http.Error(w, "reference not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		http.Error(w, "reference is not signed — configure a signing key and re-create it", http.StatusUnprocessableEntity)
		return
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Push(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("push failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	// Objects are up; push the signed reference. The server re-verifies it and
	// the no-dangling rule, which now holds.
	if err := rc.PutRef(r.Context(), name, raw); err != nil {
		h.log.Warn("push reference failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("push complete", "name", name, "pushed", stats.ObjectsPushed)
	stream.send(syncEvent{PushStats: &pushStatsLine{
		ObjectsTotal:  stats.ObjectsTotal,
		ObjectsPushed: stats.ObjectsPushed,
		BytesPushed:   stats.BytesPushed,
	}})
}

// fetchAndVerifyRemoteRef gets ?name= from the remote and fully validates
// the record: canonical decode, present signature, verifying signature.
func fetchAndVerifyRemoteRef(r *http.Request, rc *remoteclient.Client, name string) ([]byte, reference.Reference, int, error) {
	if name == "" {
		return nil, reference.Reference{}, http.StatusUnprocessableEntity, errors.New("missing name query parameter")
	}
	raw, err := rc.GetRef(r.Context(), name)
	if errors.Is(err, remoteclient.ErrRefNotFound) {
		return nil, reference.Reference{}, http.StatusNotFound, err
	}
	if err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, err
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, fmt.Errorf("remote returned an invalid reference: %w", err)
	}
	if len(rec.Signature) == 0 || len(rec.PublicKey) == 0 {
		return nil, reference.Reference{}, http.StatusBadGateway, errors.New("remote returned an unsigned reference")
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		return nil, reference.Reference{}, http.StatusInternalServerError, err
	}
	if _, err := sshsign.Verify(payload, rec.Signature, rec.PublicKey); err != nil {
		return nil, reference.Reference{}, http.StatusBadGateway, fmt.Errorf("remote reference signature does not verify: %w", err)
	}
	return raw, rec, 0, nil
}

// remotePull resolves ?name= on the REMOTE (so it works before any local ref
// exists), pulls everything reachable from its key, and then writes the
// verified reference into the local refstore — one operation, streaming NDJSON
// progress. Pull's completeness gate guarantees the whole tree is local before
// the reference is written (the no-dangling rule).
func (h *handler) remotePull(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	opts, err := syncOpts(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	name := r.URL.Query().Get("name")
	raw, rec, status, err := fetchAndVerifyRemoteRef(r, rc, name)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	stream := newEventStream(w)
	opts.Progress = func(done, total int) { stream.send(syncEvent{Done: done, Total: total}) }
	stats, err := remotesync.Pull(r.Context(), h.store, rc, root, opts)
	if err != nil {
		h.log.Warn("pull failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	if err := h.refs.Put(name, raw); err != nil {
		h.log.Warn("pull reference write failed", "name", name, "error", err)
		stream.send(syncEvent{Error: err.Error()})
		return
	}
	h.log.Info("pull complete", "name", name, "fetched", stats.ObjectsFetched, "key", root)
	stream.send(syncEvent{Key: root.String(), PullStats: &pullStatsLine{
		ObjectsFetched: stats.ObjectsFetched,
		BytesFetched:   stats.BytesFetched,
	}})
}

// remoteLsRefs proxies the remote's reference listing as NDJSON.
func (h *handler) remoteLsRefs(w http.ResponseWriter, r *http.Request) {
	rc, status, err := h.remoteFor(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	infos, err := rc.ListRefs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, info := range infos {
		if err := enc.Encode(info); err != nil {
			h.log.Error("remote ls-refs stream aborted", "error", err)
			return
		}
	}
}
