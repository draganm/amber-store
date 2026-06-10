package httpsig_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/internal/httpsig"
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

// signedRequest builds a client request and the equivalent server-side view.
func signedRequest(t *testing.T, signer ssh.Signer, ts int64, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest("POST", "http://server/v1/objects/missing?x=1", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, signer, ts, []byte("nonce-16-bytes!!"), body); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestRequestRoundTrip(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	req := signedRequest(t, signer, now.UnixNano(), body)
	pub, nonce, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if string(nonce) != "nonce-16-bytes!!" {
		t.Fatalf("nonce = %q", nonce)
	}
	if pub.Type() != signer.PublicKey().Type() {
		t.Fatalf("pub type = %s", pub.Type())
	}
}

func TestRequestRejectsTamperedBody(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	req := signedRequest(t, signer, now.UnixNano(), []byte("the body"))
	if _, _, err := httpsig.VerifyRequest(req, []byte("evil body"), now, httpsig.DefaultWindow); err == nil {
		t.Fatal("tampered body verified")
	}
}

func TestRequestRejectsTamperedPath(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	req := signedRequest(t, signer, now.UnixNano(), body)
	req.URL.Path = "/v1/refs"
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("tampered path verified")
	}
}

func TestRequestRejectsStaleTimestamp(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("b")
	req := signedRequest(t, signer, now.Add(-10*time.Minute).UnixNano(), body)
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("stale timestamp verified")
	}
	// future timestamps beyond the window are rejected too
	req = signedRequest(t, signer, now.Add(10*time.Minute).UnixNano(), body)
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("future timestamp verified")
	}
}

func TestRequestRejectsMissingHeaders(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://server/v1/refs", nil)
	if _, _, err := httpsig.VerifyRequest(req, nil, time.Now(), httpsig.DefaultWindow); err == nil {
		t.Fatal("unsigned request verified")
	}
}

func TestRequestRejectsTamperedMethod(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	req := signedRequest(t, signer, now.UnixNano(), body)
	req.Method = "GET"
	if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
		t.Fatal("tampered method verified")
	}
}

func TestRequestRejectsPartialHeaders(t *testing.T) {
	signer := testSigner(t)
	now := time.Now()
	body := []byte("the body")
	for _, h := range []string{httpsig.HeaderPublicKey, httpsig.HeaderTimestamp, httpsig.HeaderNonce, httpsig.HeaderSignature} {
		req := signedRequest(t, signer, now.UnixNano(), body)
		req.Header.Del(h)
		if _, _, err := httpsig.VerifyRequest(req, body, now, httpsig.DefaultWindow); err == nil {
			t.Fatalf("request without %s verified", h)
		}
	}
}

func TestResponseRoundTrip(t *testing.T) {
	signer := testSigner(t)
	body := []byte("response body")
	nonce := []byte("nonce-16-bytes!!")
	sig, err := httpsig.SignResponse(signer, nonce, 200, httpsig.HashBody(body))
	if err != nil {
		t.Fatal(err)
	}
	pubWire := signer.PublicKey().Marshal()
	if err := httpsig.VerifyResponse(pubWire, nonce, 200, httpsig.HashBody(body), sig); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := httpsig.VerifyResponse(pubWire, nonce, 404, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong status verified")
	}
	if err := httpsig.VerifyResponse(pubWire, []byte("other nonce!!!!!"), 200, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong nonce verified")
	}
	if err := httpsig.VerifyResponse(pubWire, nonce, 200, httpsig.HashBody([]byte("evil")), sig); err == nil {
		t.Fatal("wrong body hash verified")
	}
	other := testSigner(t).PublicKey().Marshal()
	if err := httpsig.VerifyResponse(other, nonce, 200, httpsig.HashBody(body), sig); err == nil {
		t.Fatal("wrong key verified")
	}
}

func TestResponseNilNonceEqualsEmpty(t *testing.T) {
	signer := testSigner(t)
	sig, err := httpsig.SignResponse(signer, nil, 200, httpsig.HashBody([]byte("b")))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.VerifyResponse(signer.PublicKey().Marshal(), []byte{}, 200, httpsig.HashBody([]byte("b")), sig); err != nil {
		t.Fatalf("nil-signed nonce did not verify as empty: %v", err)
	}
}
