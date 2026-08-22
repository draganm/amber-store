package nixcache

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	ikey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"golang.org/x/sync/singleflight"
)

// SwarmOpts configures NewSwarm.
type SwarmOpts struct {
	KeyPath string         // 32-byte identity seed, created if missing
	Bind    netip.AddrPort // UDP bind address
	Relay   relay.Mode
	Extra   []iroh.Option
}

// Swarm keeps one QUIC connection per peer over an iroh endpoint and
// serves the swarm protocol on every connection, dialed or accepted, so a
// peer that reached us through NAT is usable as a source too.
type Swarm struct {
	ep     *iroh.Endpoint
	router *iroh.Router
	handle atomic.Pointer[func(*iroh.Stream)] // nil until a Server attaches

	mu    sync.Mutex
	conns map[ikey.EndpointID]*iroh.Conn
	dial  singleflight.Group

	ctx    context.Context
	cancel context.CancelFunc
}

// NewSwarm binds the swarm endpoint with a persistent identity and starts
// accepting connections.
func NewSwarm(ctx context.Context, o SwarmOpts) (*Swarm, error) {
	sk, err := loadIdentity(o.KeyPath)
	if err != nil {
		return nil, err
	}
	s := &Swarm{conns: map[ikey.EndpointID]*iroh.Conn{}}
	opts := append([]iroh.Option{
		iroh.WithSecretKey(sk),
		iroh.WithALPNs(swarmALPN),
		iroh.WithBindAddr(o.Bind),
		iroh.WithRelayMode(o.Relay),
		iroh.WithNetReport(),
		iroh.WithTransportConfig(&iroh.QUICTransportConfig{MaxIncomingStreams: 256}),
	}, o.Extra...)
	s.ep, err = iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, err
	}
	s.router, err = iroh.NewRouter(s.ep, map[string]iroh.ProtocolHandler{
		swarmALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, c *iroh.Conn) error {
			s.serveConn(c)
			return nil
		}),
	}, nil)
	if err != nil {
		s.ep.Shutdown(ctx)
		return nil, err
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s, nil
}

func (s *Swarm) Endpoint() *iroh.Endpoint   { return s.ep }
func (s *Swarm) ID() ikey.EndpointID        { return s.ep.ID() }
func (s *Swarm) Addr() netaddr.EndpointAddr { return s.ep.Addr() }
func (s *Swarm) done() <-chan struct{}      { return s.ctx.Done() }

// LogAddr logs the ticket now and whenever the advertised addresses change.
func (s *Swarm) LogAddr() {
	w := s.ep.WatchAddr()
	go func() {
		last := ""
		for a := w.Current(); ; {
			if t := endpointticket.Encode(a); t != last {
				last = t
				slog.Info("swarm address", "ticket", t)
			}
			var err error
			if a, err = w.Updated(s.ctx); err != nil {
				return
			}
		}
	}()
}

// Close shuts the endpoint down.
func (s *Swarm) Close() error {
	s.cancel()
	err := s.router.Shutdown(context.Background())
	if errors.Is(err, iroh.ErrEndpointClosed) {
		return nil
	}
	return err
}

// AddAddr records how to reach a peer.
func (s *Swarm) AddAddr(a netaddr.EndpointAddr) { s.ep.AddEndpointAddr(a) }

// Connected reports whether a live connection to id is cached.
func (s *Swarm) Connected(id ikey.EndpointID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.conns[id]
	return ok && c.Context().Err() == nil
}

// NumConnected counts live cached connections by whether their selected
// path is direct or relayed.
func (s *Swarm) NumConnected() (direct, relayed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		if c.Context().Err() != nil {
			continue
		}
		if slices.ContainsFunc(c.Paths(), func(p iroh.PathInfo) bool { return p.Selected && !p.Relayed }) {
			direct++
		} else {
			relayed++
		}
	}
	return direct, relayed
}

// ClosePeer drops the connection to id, if any.
func (s *Swarm) ClosePeer(id ikey.EndpointID) {
	s.mu.Lock()
	c := s.conns[id]
	delete(s.conns, id)
	s.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// Conn returns the cached connection to id or dials one.
func (s *Swarm) Conn(ctx context.Context, id ikey.EndpointID) (*iroh.Conn, error) {
	s.mu.Lock()
	if c, ok := s.conns[id]; ok && c.Context().Err() == nil {
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()
	ch := s.dial.DoChan(string(id.String()), func() (any, error) {
		c, err := s.ep.Connect(s.ctx, netaddr.NewEndpointAddr(id), swarmALPN)
		if err != nil {
			return nil, err
		}
		go s.serveConn(c)
		return c, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*iroh.Conn), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// serveConn caches c (unless another live conn to that peer is cached) and
// serves requests on it until it ends.
func (s *Swarm) serveConn(c *iroh.Conn) {
	id := c.RemoteID()
	s.mu.Lock()
	if old, ok := s.conns[id]; !ok || old.Context().Err() != nil {
		s.conns[id] = c
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.conns[id] == c {
			delete(s.conns, id)
		}
		s.mu.Unlock()
	}()
	for {
		st, err := c.AcceptStream(s.ctx)
		if err != nil {
			return
		}
		h := s.handle.Load()
		if h == nil {
			resetStream(st)
			continue
		}
		go (*h)(st)
	}
}

// open sends req on a new stream to id and consumes the response status.
// The caller reads the body and closes the stream. ctx aborts the stream.
func (s *Swarm) open(ctx context.Context, id ikey.EndpointID, req request) (*iroh.Stream, error) {
	c, err := s.Conn(ctx, id)
	if err != nil {
		return nil, err
	}
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	// Stream reads block regardless of ctx. A cancelled fetch (scheduler
	// abort, pull teardown) must tear the stream down to unblock them.
	context.AfterFunc(ctx, func() { resetStream(st) })
	if _, err := st.Write(req.encode()); err != nil {
		resetStream(st)
		return nil, err
	}
	if err := st.Close(); err != nil {
		resetStream(st)
		return nil, err
	}
	if err := readStatus(st); err != nil {
		st.CancelRead(0)
		return nil, err
	}
	return st, nil
}

func resetStream(st *iroh.Stream) {
	st.CancelRead(0)
	st.CancelWrite(0)
}
