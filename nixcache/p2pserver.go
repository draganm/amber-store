package nixcache

import (
	"cmp"
	"context"
	"errors"
	"io"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/keylist"
	"github.com/tmc/go-iroh/iroh"
)

// Attach serves the swarm protocol on sw from the Server's store and
// index. Reads need no authentication: content is addressed by hash,
// narinfo records carry upstream signatures.
func (s *Server) Attach(sw *Swarm) {
	h := s.handleStream
	sw.handle.Store(&h)
}

// handleStream wraps request dispatch with admission control and the
// request/status framing.
func (s *Server) handleStream(st *iroh.Stream) {
	release, ok := s.admit(context.Background())
	if !ok {
		resetStream(st)
		return
	}
	defer release()
	req, err := readRequest(st)
	if err != nil {
		resetStream(st)
		return
	}
	w := &deadlineWriter{st: st, idle: cmp.Or(s.PeerWriteTimeout, 30*time.Second)}
	switch req.kind {
	case reqObjects:
		err = s.serveObjects(req.keys, w)
	case reqKeys:
		err = s.serveKeys(req.root, w)
	case reqIndex:
		err = s.serveIndex(w)
	case reqNarinfo:
		err = s.serveNarinfo(req.hashpart, w)
	}
	if err != nil {
		resetStream(st)
		return
	}
	st.Close()
}

// admit bounds abuse: a concurrency cap with a short queue plus a shared
// token bucket over served bytes. A legitimate burst waits briefly.
func (s *Server) admit(ctx context.Context) (release func(), ok bool) {
	s.peerOnce.Do(func() {
		s.peerSem = make(chan struct{}, cmp.Or(s.PeerConcurrency, 4))
		if s.PeerByteRate > 0 {
			s.peerRate = newByteLimiter(s.PeerByteRate)
		}
	})
	select {
	case s.peerSem <- struct{}{}:
		return func() { <-s.peerSem }, true
	case <-ctx.Done():
		return nil, false
	case <-time.After(time.Second):
		return nil, false
	}
}

// deadlineWriter renews an idle write deadline per write, so a peer that
// stops reading frees its admission slot instead of holding it forever.
type deadlineWriter struct {
	st   *iroh.Stream
	idle time.Duration
}

func (d *deadlineWriter) Write(b []byte) (int, error) {
	d.st.SetWriteDeadline(time.Now().Add(d.idle))
	return d.st.Write(b)
}

func (s *Server) peerWriter(w io.Writer) io.Writer {
	if s.peerRate == nil {
		return w
	}
	return &limitedWriter{w: w, l: s.peerRate}
}

func writeStatus(w io.Writer, status byte) error {
	_, err := w.Write([]byte{status})
	return err
}

// serveObjects streams the requested records as one amberpack, coalesced
// into a few large writes copied span by span out of the segments. Objects
// self-verify against their keys on the puller's side.
func (s *Server) serveObjects(req []byte, w io.Writer) error {
	keys, err := keylist.Parse(req)
	if err != nil || len(keys) > maxPeerKeys {
		return writeStatus(w, statusError)
	}
	if err := writeStatus(w, statusOK); err != nil {
		return err
	}
	pw := amberpack.NewWriter(s.peerWriter(w))
	// Bytes may be in flight on error. Cutting the stream fails the
	// client's pack parse, and the puller falls back.
	if err := s.Store.ViewRecordSpans(keys, maxServeSpan, pw.AddRecord); err != nil {
		return err
	}
	return pw.Close()
}

// serveKeys serves the reachable key set under a root: the list half of
// the chunk-sync protocol peers pull trees through.
func (s *Server) serveKeys(rootBytes [32]byte, w io.Writer) error {
	root, err := key.Parse(rootBytes[:])
	if err != nil {
		return writeStatus(w, statusError)
	}
	keys, err := fstree.ReachableKeys(root, s.Store.Get)
	switch {
	case errors.Is(err, fstree.ErrNotFound):
		return writeStatus(w, statusNotFound)
	case err != nil:
		return writeStatus(w, statusError)
	}
	if err := writeStatus(w, statusOK); err != nil {
		return err
	}
	_, err = s.peerWriter(w).Write(keylist.Flatten(keys))
	return err
}

// serveIndex serves this node's index root for peer sync.
func (s *Server) serveIndex(w io.Writer) error {
	root := s.Index()
	if err := writeStatus(w, statusOK); err != nil {
		return err
	}
	_, err := w.Write(root[:])
	return err
}

// serveNarinfo answers a peer's probe from the index alone — never the
// upstream fetch path, so probes cannot cascade into fetches or peer
// cycles.
func (s *Server) serveNarinfo(hp string, w io.Writer) error {
	if !validHashPart(hp) {
		return writeStatus(w, statusError)
	}
	pi, err := Lookup(s.Index(), hp, s.Store.Get)
	switch {
	case errors.Is(err, fstree.ErrNotFound):
		return writeStatus(w, statusNotFound)
	case err != nil:
		return writeStatus(w, statusError)
	}
	if s.Touch != nil {
		s.Touch(hp)
	}
	if err := writeStatus(w, statusOK); err != nil {
		return err
	}
	_, err = w.Write(FormatNarinfo(pi))
	return err
}
