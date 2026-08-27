package nixcache_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/nixcache"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

func relayOnlySwarm(t *testing.T, mode relay.Mode) *nixcache.Swarm {
	t.Helper()
	sw, err := nixcache.NewSwarm(t.Context(), nixcache.SwarmOpts{
		KeyPath: t.TempDir() + "/p2p.key",
		Relay:   mode,
		Extra:   []iroh.Option{iroh.WithoutIPTransports()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	return sw
}

// Two leaves without UDP reach each other through the embedded relay.
func TestEmbeddedRelay(t *testing.T) {
	rh, err := nixcache.ServeRelay(nixcache.RelayOpts{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer rh.Close()
	if !strings.HasPrefix(rh.URL().String(), "http://127.0.0.1:") {
		t.Fatalf("url = %s", rh.URL())
	}
	mode := relay.ModeCustomURLs(rh.URL())

	a, b := relayOnlySwarm(t, mode), relayOnlySwarm(t, mode)
	st := newRecStore()
	want := buildTree(t, st, []byte("relay"))
	(&nixcache.Server{Store: st, Index: func() key.Key { return want }}).Attach(b)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := b.Endpoint().Online(ctx); err != nil {
		t.Fatal("b online:", err)
	}
	if len(b.Addr().IPAddrs()) != 0 {
		t.Fatalf("b advertises IPs %v", b.Addr().IPAddrs())
	}
	a.AddAddr(b.Addr())
	src := &nixcache.PeerSource{Swarm: a, ID: b.ID()}
	if got, err := nixcache.IndexRoot(src, ctx); err != nil || got != want {
		t.Fatalf("index root via relay: %v, %v", got, err)
	}
	if rh.Snapshot()["datagrams_forwarded"] == 0 {
		t.Fatal("relay forwarded nothing")
	}
	if !strings.Contains(rh.Metrics(), "nix_cached_relay_datagrams_forwarded_total ") {
		t.Fatalf("metrics:\n%s", rh.Metrics())
	}
}

// With a dead direct address the dial must still succeed through the relay.
func TestRelayFallbackWithDeadDirectAddr(t *testing.T) {
	rh, err := nixcache.ServeRelay(nixcache.RelayOpts{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer rh.Close()
	mode := relay.ModeCustomURLs(rh.URL())
	mk := func() *nixcache.Swarm {
		sw, err := nixcache.NewSwarm(t.Context(), nixcache.SwarmOpts{
			KeyPath: t.TempDir() + "/p2p.key",
			Bind:    netip.MustParseAddrPort("127.0.0.1:0"),
			Relay:   mode,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sw.Close() })
		return sw
	}
	a, b := mk(), mk()
	st := newRecStore()
	want := buildTree(t, st, []byte("relay"))
	(&nixcache.Server{Store: st, Index: func() key.Key { return want }}).Attach(b)
	// blackhole UDP port instead of b's real one
	dead := netaddr.NewEndpointAddr(b.ID()).WithIP(netip.MustParseAddrPort("127.0.0.1:9")).WithRelayURL(rh.URL())
	a.AddAddr(dead)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	src := &nixcache.PeerSource{Swarm: a, ID: b.ID()}
	if got, err := nixcache.IndexRoot(src, ctx); err != nil || got != want {
		t.Fatalf("index root via relay fallback: %v, %v", got, err)
	}
}

// With a certificate the embedded relay answers QAD, so two wildcard-bound
// peers that advertise nothing themselves learn their address from it and
// end up on a direct path.
func TestEmbeddedRelayQADGivesDirectPath(t *testing.T) {
	certFile, keyFile, pool := selfSignedCert(t)
	rh, err := nixcache.ServeRelay(nixcache.RelayOpts{Listen: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile})
	if err == nil {
		t.Fatal("https relay without --relay-url accepted")
	}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	rh, err = nixcache.ServeRelay(nixcache.RelayOpts{Listen: fmt.Sprintf("127.0.0.1:%d", port), CertFile: certFile, KeyFile: keyFile,
		ExternalURL: fmt.Sprintf("https://127.0.0.1:%d", port)})
	if err != nil {
		t.Fatal(err)
	}
	defer rh.Close()
	caFile := t.TempDir() + "/ca.pem"
	os.WriteFile(caFile, pool, 0o600)
	mk := func() *nixcache.Swarm {
		sw, err := nixcache.NewSwarm(t.Context(), nixcache.SwarmOpts{
			KeyPath: t.TempDir() + "/p2p.key",
			Bind:    netip.MustParseAddrPort("0.0.0.0:0"),
			Relay:   rh.Mode(),
			RelayCA: caFile,
			Extra:   []iroh.Option{iroh.WithoutInterfaceAddrs()},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sw.Close() })
		return sw
	}
	a, b := mk(), mk()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for _, sw := range []*nixcache.Swarm{a, b} {
		for len(sw.Addr().IPAddrs()) == 0 {
			select {
			case <-ctx.Done():
				t.Fatalf("no QAD address: %v", sw.Addr())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	a.AddAddr(b.Addr())
	c, err := a.Conn(ctx, b.ID())
	if err != nil {
		t.Fatal(err)
	}
	for {
		if slices.ContainsFunc(c.Paths(), func(p iroh.PathInfo) bool { return p.HasAddr && !p.Relayed }) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no direct path: %+v", c.Paths())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func selfSignedCert(t *testing.T) (certFile, keyFile string, pemCert []byte) {
	t.Helper()
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(priv)
	pemCert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	dir := t.TempDir()
	certFile, keyFile = dir+"/cert.pem", dir+"/key.pem"
	os.WriteFile(certFile, pemCert, 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	return certFile, keyFile, pemCert
}
