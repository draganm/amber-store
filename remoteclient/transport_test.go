package remoteclient

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/ssh"
)

func newTestSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fetchProto(t *testing.T, hc *http.Client, url string) string {
	t.Helper()
	resp, err := hc.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The client must stay on HTTP/1.1 even when the server offers HTTP/2:
// HTTP/2 multiplexes every worker onto one TCP connection whose upload
// flow-control window caps push throughput at roughly window/RTT on
// high-latency links, while parallel HTTP/1.1 connections get kernel-tuned
// TCP windows.
func TestClientSpeaksHTTP1ToHTTP2CapableServer(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// Sanity: an h2-willing client does negotiate HTTP/2 with this server.
	if got := fetchProto(t, srv.Client(), srv.URL); got != "HTTP/2.0" {
		t.Fatalf("test server negotiated %s with an h2-willing client, cannot exercise the downgrade", got)
	}

	c, err := New(srv.URL, newTestSigner(t), []byte("pinned-key"))
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport is %T, want *http.Transport", c.hc.Transport)
	}
	// Add trust for the test certificate WITHOUT replacing the TLS config:
	// Transport.Clone() h2-initializes http.DefaultTransport and copies its
	// TLSClientConfig (NextProtos and all), and a fresh config here would
	// mask whatever ALPN list production really advertises.
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.RootCAs = pool
	if got := fetchProto(t, c.hc, srv.URL); got != "HTTP/1.1" {
		t.Fatalf("client negotiated %s, want HTTP/1.1", got)
	}
}

// With one HTTP/1.1 connection per in-flight request, the idle pool must
// hold at least as many connections as sync workers (--jobs), or workers
// re-handshake between batches. The stock default of 2 is too small.
func TestTransportKeepsConnectionsForParallelWorkers(t *testing.T) {
	tr := newTransport()
	if tr.MaxIdleConnsPerHost < 32 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want at least 32", tr.MaxIdleConnsPerHost)
	}
}
