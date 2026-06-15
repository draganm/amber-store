package server_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func testSigner(t *testing.T) ssh.Signer {
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

// testServer is the shared harness: a server over fresh stores with one
// allowed client key and one allowed admin key, returning everything a
// test needs to issue signed requests.
type testServer struct {
	srv      *httptest.Server
	store    *packstore.Store
	refs     *refstore.Store
	inbox    *inbox.Inbox
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	store, err := packstore.Open(filepath.Join(dir, "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ib.Close() })
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
		Inbox:    ib,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testServer{srv: srv, store: store, refs: refs, inbox: ib, identity: identity, client: client, admin: admin}
}

// signedDo sends one signed request and returns status + body, verifying the
// response signature against the server identity along the way.
func (ts *testServer) signedDo(t *testing.T, signer ssh.Signer, method, pathQuery string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ts.srv.URL+pathQuery, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, signer, time.Now().UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	sig := resp.Header.Get(httpsig.HeaderSignature)
	if sig == "" {
		sig = resp.Trailer.Get(httpsig.HeaderSignature)
	}
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nonce, resp.StatusCode,
		httpsig.HashBody(respBody), sig); err != nil {
		t.Fatalf("response signature: %v", err)
	}
	return resp.StatusCode, respBody
}

func TestIdentityIsServedAndSelfSigned(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.srv.URL + "/v1/identity")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !bytes.Equal(body, ts.identity.PublicKey().Marshal()) {
		t.Fatal("identity body is not the server public key")
	}
	// self-signed: the body itself is the verification key, nonce empty
	if err := httpsig.VerifyResponse(body, nil, 200, httpsig.HashBody(body),
		resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		t.Fatalf("identity self-signature: %v", err)
	}
}

func TestAuthRejections(t *testing.T) {
	ts := newTestServer(t)

	// unsigned request → 401, still signed by the server (nil nonce)
	resp, err := http.Post(ts.srv.URL+"/v1/objects/missing", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 401 {
		t.Fatalf("unsigned status = %d, want 401", resp.StatusCode)
	}
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nil, resp.StatusCode,
		httpsig.HashBody(respBody), resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		t.Fatalf("unsigned-401 response signature: %v", err)
	}

	// valid signature, unlisted key → 403
	stranger := testSigner(t)
	if code, _ := ts.signedDo(t, stranger, "POST", "/v1/objects/missing", nil); code != 403 {
		t.Fatalf("unlisted key status = %d, want 403", code)
	}

	// allowed key → 200
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", nil); code != 200 {
		t.Fatalf("allowed key status = %d, want 200", code)
	}
}

func TestReplayedNonceRejected(t *testing.T) {
	ts := newTestServer(t)
	req, err := http.NewRequest("POST", ts.srv.URL+"/v1/objects/missing", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), []byte("fixed-nonce-0123"), nil); err != nil {
		t.Fatal(err)
	}
	send := func() int {
		r2 := req.Clone(req.Context())
		r2.Body = io.NopCloser(bytes.NewReader(nil))
		resp, err := http.DefaultClient.Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := send(); code != 200 {
		t.Fatalf("first send = %d, want 200", code)
	}
	if code := send(); code != 401 {
		t.Fatalf("replay = %d, want 401", code)
	}
}

func TestBodyOverCapRejected(t *testing.T) {
	ts := newTestServer(t)
	allow, err := allowlist.Parse(ssh.MarshalAuthorizedKey(ts.client.PublicKey()))
	if err != nil {
		t.Fatal(err)
	}
	small := server.New(server.Config{
		Store:    ts.store,
		Refs:     ts.refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: ts.identity,
		MaxBody:  1024,
	})
	srv := httptest.NewServer(small)
	defer srv.Close()
	body := make([]byte, 2048)
	req, err := http.NewRequest("POST", srv.URL+"/v1/objects/missing", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), []byte("nonce-16-bytes!!"), body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	// The body was never authenticated, so the server signs over a nil nonce.
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nil, resp.StatusCode,
		httpsig.HashBody(respBody), resp.Header.Get(httpsig.HeaderSignature)); err != nil {
		t.Fatalf("413 response signature: %v", err)
	}
}
