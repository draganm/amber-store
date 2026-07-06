// Package remoteclient is the signed HTTP client the local daemon uses to
// talk to a remote amber-store server: every request is signed with the
// daemon's SSH identity (internal/httpsig) and every response must verify
// against the pinned server key recorded at `remote add` — a mismatch aborts
// the operation.
package remoteclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/httpsig"
	"golang.org/x/crypto/ssh"
)

// maxResponse bounds a buffered (non-streaming) response body.
const maxResponse = 256 << 20 // 256 MiB

// StatusError is a non-2xx server response: the HTTP status plus the
// server's (signed) message body.
type StatusError struct {
	Code int
	Msg  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server responded %d: %s", e.Code, strings.TrimSpace(e.Msg))
}

// Option configures a Client.
type Option func(*Client)

// WithGrant attaches a delegated capability grant (package grant) to every
// request: provider is consulted per request — so a caller can swap in a
// refreshed grant at any time — and may return nil to send none.
func WithGrant(provider func() []byte) Option {
	return func(c *Client) { c.grant = provider }
}

// Client talks to one remote server with one client identity and one pinned
// server key. Safe for concurrent use.
type Client struct {
	hc            *http.Client
	base          string
	signer        ssh.Signer
	serverPubWire []byte
	grant         func() []byte
}

// newTransport returns the transport every Client uses: the default
// transport with HTTP/2 disabled. HTTP/2 would multiplex all sync workers
// onto a single TCP connection whose 1 MiB upload flow-control window caps
// push throughput at roughly 1 MiB per round trip regardless of bandwidth;
// HTTP/1.1 gives each worker its own connection with kernel-tuned TCP
// windows.
func newTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ForceAttemptHTTP2 = false
	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	// Clone() h2-initializes DefaultTransport first, so the cloned
	// TLSClientConfig still ADVERTISES h2 (NextProtos = ["h2","http/1.1"])
	// even though the empty TLSNextProto map above removed the h2 handler.
	// Left as is, a server that picks h2 from that offer breaks the
	// connection ("malformed HTTP response \x00\x00..."), so advertise
	// HTTP/1.1 only.
	if tr.TLSClientConfig != nil {
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	// A Client only ever talks to one host; let the whole idle pool serve it
	// so every sync worker (--jobs) keeps its connection between batches.
	tr.MaxIdleConnsPerHost = tr.MaxIdleConns
	return tr
}

// New validates the base URL (http or https) and returns a Client. The
// pinned server key is the SSH wire-format public key confirmed at
// `remote add`.
func New(baseURL string, signer ssh.Signer, serverPubWire []byte, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("remote URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("remote URL %q must be http(s)://host[:port][/path]", baseURL)
	}
	if len(serverPubWire) == 0 {
		return nil, fmt.Errorf("no pinned server key for %s", baseURL)
	}
	c := &Client{
		hc:            &http.Client{Transport: newTransport()},
		base:          strings.TrimRight(baseURL, "/"),
		signer:        signer,
		serverPubWire: serverPubWire,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

func newNonce() ([]byte, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}
	return b, nil
}

// attachGrant adds the capability-grant header when a provider is configured.
// The grant rides outside the request signature: the server binds it to the
// signer key via the grant's subject, so it needs no coverage by httpsig.
func (c *Client) attachGrant(req *http.Request) {
	if c.grant == nil {
		return
	}
	if g := c.grant(); len(g) > 0 {
		req.Header.Set(grant.Header, base64.StdEncoding.EncodeToString(g))
	}
}

// signedRequest builds and signs a request; the returned nonce is what the
// response signature must cover.
func (c *Client) signedRequest(ctx context.Context, method, pathQuery, contentType string, body []byte) (*http.Request, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+pathQuery, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	nonce, err := newNonce()
	if err != nil {
		return nil, nil, err
	}
	if err := httpsig.SignRequest(req, c.signer, time.Now().UnixNano(), nonce, body); err != nil {
		return nil, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.attachGrant(req)
	return req, nonce, nil
}

// do sends one signed request with fully-buffered request and response
// bodies, verifies the response signature against the pinned key, and maps
// non-2xx statuses to StatusError.
func (c *Client) do(ctx context.Context, method, pathQuery, contentType string, body []byte) (int, []byte, error) {
	req, nonce, err := c.signedRequest(ctx, method, pathQuery, contentType, body)
	if err != nil {
		return 0, nil, err
	}
	return c.send(req, nonce)
}

// Wipe asks the server to destroy every object and reference (POST /v1/wipe,
// the factory-reset endpoint). The signing key must carry the wipe capability
// on the server's allowlist — a grant can never convey it.
func (c *Client) Wipe(ctx context.Context) error {
	if _, _, err := c.do(ctx, http.MethodPost, "/v1/wipe", "", nil); err != nil {
		return fmt.Errorf("wiping remote %s: %w", c.base, err)
	}
	return nil
}

// doStreaming sends a signed request whose body is streamed from body (never
// held whole), so the caller must pass the body's precomputed blake3 as
// bodyHash — the signature header is set before the body is read. body is taken
// as a ReadCloser so the transport closes it on completion or error, unblocking
// a producer goroutine writing into a pipe.
func (c *Client) doStreaming(ctx context.Context, method, pathQuery, contentType string, bodyHash []byte, body io.ReadCloser) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+pathQuery, body)
	if err != nil {
		return 0, nil, err
	}
	nonce, err := newNonce()
	if err != nil {
		return 0, nil, err
	}
	if err := httpsig.SignRequestHash(req, c.signer, time.Now().UnixNano(), nonce, bodyHash); err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.attachGrant(req)
	return c.send(req, nonce)
}

// send performs req, buffers and verifies the response against the pinned key,
// and maps non-2xx statuses to StatusError. nonce is what the response
// signature must cover.
func (c *Client) send(req *http.Request, nonce []byte) (int, []byte, error) {
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("contacting remote %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return 0, nil, fmt.Errorf("reading response: %w", err)
	}
	sig := resp.Header.Get(httpsig.HeaderSignature)
	if sig == "" {
		sig = resp.Trailer.Get(httpsig.HeaderSignature)
	}
	if err := httpsig.VerifyResponse(c.serverPubWire, nonce, resp.StatusCode,
		httpsig.HashBody(respBody), sig); err != nil {
		return 0, nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, respBody, &StatusError{Code: resp.StatusCode, Msg: string(respBody)}
	}
	return resp.StatusCode, respBody, nil
}

// FetchIdentity fetches the server's public key (SSH wire format) from the
// unauthenticated identity endpoint. The response is self-signed — the
// returned key verifies its own signature — so trust must come from the user
// confirming the fingerprint.
func FetchIdentity(ctx context.Context, baseURL string) ([]byte, error) {
	base := strings.TrimRight(baseURL, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/identity", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting remote %s: %w", base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading identity: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{Code: resp.StatusCode, Msg: string(body)}
	}
	if _, err := ssh.ParsePublicKey(body); err != nil {
		return nil, fmt.Errorf("server identity is not a valid SSH public key: %w", err)
	}
	if err := httpsig.VerifyResponse(body, nil, resp.StatusCode, httpsig.HashBody(body),
		resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		return nil, fmt.Errorf("server identity self-signature: %w", err)
	}
	return body, nil
}
