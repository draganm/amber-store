package nixcache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

// RelayOpts configures ServeRelay.
type RelayOpts struct {
	Listen      string // TCP listen address, and UDP for QAD when TLS is set
	ExternalURL string // what peers put in --relay; empty derives it
	CertFile    string // PEM certificate; enables HTTPS and QAD
	KeyFile     string
}

// RelayHost is an iroh relay embedded in the node. With a certificate it
// also answers QUIC address discovery so NATed peers learn their public
// address and can go direct; without one they stay relayed.
type RelayHost struct {
	srv    *relayserver.Server
	hs     *http.Server
	qad    net.PacketConn
	cancel context.CancelFunc
	url    netaddr.RelayURL
}

func ServeRelay(o RelayOpts) (*RelayHost, error) {
	var tc *tls.Config
	if o.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, err
		}
		tc = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}}
	}
	l, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return nil, err
	}
	u, err := relayURL(l.Addr().(*net.TCPAddr), o.ExternalURL, tc != nil)
	if err != nil {
		l.Close()
		return nil, err
	}
	rs := relayserver.New()
	mux := http.NewServeMux()
	mux.Handle("/", rs)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	ctx, cancel := context.WithCancel(context.Background())
	r := &RelayHost{srv: rs, url: u, cancel: cancel,
		hs: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, TLSConfig: tc}}
	if tc == nil {
		go r.hs.Serve(l)
		return r, nil
	}
	r.qad, err = net.ListenPacket("udp", l.Addr().String())
	if err != nil {
		l.Close()
		return nil, err
	}
	go r.hs.ServeTLS(l, "", "")
	go rs.ServeQAD(ctx, r.qad, tc)
	return r, nil
}

func relayURL(bound *net.TCPAddr, external string, https bool) (netaddr.RelayURL, error) {
	if external != "" {
		return parseRelayURL(external)
	}
	if https {
		return netaddr.RelayURL{}, errors.New("--relay-cert needs --relay-url with the certificate's host name")
	}
	host, _ := netip.AddrFromSlice(bound.IP)
	host = host.Unmap()
	if host.IsUnspecified() {
		var ok bool
		if host, ok = defaultRouteAddr(); !ok {
			return netaddr.RelayURL{}, errors.New("no interface address for the relay URL, pass --relay-url")
		}
	}
	return netaddr.RelayURLFromURL(&url.URL{Scheme: "http", Host: netip.AddrPortFrom(host, uint16(bound.Port)).String()}), nil
}

// defaultRouteAddr is the source address the kernel picks for outbound
// traffic. Dialing UDP sends nothing.
func defaultRouteAddr() (netip.Addr, bool) {
	for _, dst := range []string{"192.0.2.1:9", "[2001:db8::1]:9"} {
		c, err := net.Dial("udp", dst)
		if err != nil {
			continue
		}
		a := c.LocalAddr().(*net.UDPAddr).AddrPort().Addr().Unmap()
		c.Close()
		return a, true
	}
	return netip.Addr{}, false
}

func (r *RelayHost) URL() netaddr.RelayURL { return r.url }

// config points QAD at the HTTPS port, iroh relays use a separate one.
func (r *RelayHost) config() relay.Config {
	c := relay.NewConfig(r.url, nil)
	if r.qad != nil {
		c.QUIC = &relay.QUICConfig{Port: uint16(r.qad.LocalAddr().(*net.UDPAddr).Port)}
	}
	return c
}

// Mode is a relay mode with only this relay.
func (r *RelayHost) Mode() relay.Mode { return relay.ModeCustom(relay.NewMap(r.config())) }

func (r *RelayHost) Close() error {
	r.cancel()
	if r.qad != nil {
		r.qad.Close()
	}
	return r.hs.Close()
}

func (r *RelayHost) Snapshot() map[string]uint64 { return r.srv.Snapshot() }

// Metrics renders the relay counters in Prometheus text format.
func (r *RelayHost) Metrics() string {
	snap := r.srv.Snapshot()
	names := make([]string, 0, len(snap))
	for k := range snap {
		names = append(names, k)
	}
	slices.Sort(names)
	var b strings.Builder
	for _, k := range names {
		fmt.Fprintf(&b, "# TYPE nix_cached_relay_%s_total counter\nnix_cached_relay_%s_total %d\n", k, k, snap[k])
	}
	return b.String()
}
