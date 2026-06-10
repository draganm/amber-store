// Package httpsig signs and verifies amber-store remote-protocol HTTP
// messages. A request signature covers a canonical CBOR map of
// {method, path+query, timestamp, nonce, blake3(body)}; a response signature
// covers {request nonce, status, blake3(body)}. Both are SSHSIG blobs in the
// amber-store-http namespace, base64 in Amber-* headers. The expensive part —
// hashing a multi-megabyte body — is blake3; SSHSIG's internal SHA-512 only
// covers the ~100-byte canonical payload.
package httpsig

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/fxamacker/cbor/v2"
	"github.com/zeebo/blake3"
	"golang.org/x/crypto/ssh"
)

// Header names of the four signature components.
const (
	HeaderPublicKey = "Amber-Public-Key" // signer's public key, SSH wire format, base64
	HeaderTimestamp = "Amber-Timestamp"  // ns since the Unix epoch, decimal
	HeaderNonce     = "Amber-Nonce"      // random bytes, base64
	HeaderSignature = "Amber-Signature"  // raw SSHSIG blob, base64
)

// Namespace is the SSHSIG namespace for amber-store HTTP signatures.
const Namespace = "amber-store-http"

// DefaultWindow is the default timestamp validity window (each side of now).
const DefaultWindow = 5 * time.Minute

// encMode is the shared deterministic encoder, mirroring the reference and
// fstree conventions.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("httpsig: building CBOR enc mode: %v", err))
	}
	encMode = m
}

type requestPayload struct {
	Method    string `cbor:"0,keyasint"`
	PathQuery string `cbor:"1,keyasint"`
	Timestamp int64  `cbor:"2,keyasint"`
	Nonce     []byte `cbor:"3,keyasint"`
	BodyHash  []byte `cbor:"4,keyasint"`
}

type responsePayload struct {
	Nonce    []byte `cbor:"0,keyasint"`
	Status   int64  `cbor:"1,keyasint"`
	BodyHash []byte `cbor:"2,keyasint"`
}

// HashBody returns the blake3-256 hash of body.
func HashBody(body []byte) []byte {
	h := blake3.Sum256(body)
	return h[:]
}

// notNil maps nil to an empty slice so a nil and an empty nonce/hash encode
// identically.
func notNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

func requestSigPayload(method, pathQuery string, timestamp int64, nonce, bodyHash []byte) ([]byte, error) {
	return encMode.Marshal(requestPayload{
		Method:    method,
		PathQuery: pathQuery,
		Timestamp: timestamp,
		Nonce:     notNil(nonce),
		BodyHash:  notNil(bodyHash),
	})
}

// SignRequest signs req's method, path+query, timestamp, nonce and body, and
// sets the four Amber-* headers. body must be the exact bytes the request
// will send.
func SignRequest(req *http.Request, signer ssh.Signer, timestamp int64, nonce, body []byte) error {
	payload, err := requestSigPayload(req.Method, req.URL.RequestURI(), timestamp, nonce, HashBody(body))
	if err != nil {
		return fmt.Errorf("encoding request signature payload: %w", err)
	}
	sig, err := sshsign.SignNamespace(signer, payload, Namespace)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderPublicKey, base64.StdEncoding.EncodeToString(signer.PublicKey().Marshal()))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(timestamp, 10))
	req.Header.Set(HeaderNonce, base64.StdEncoding.EncodeToString(nonce))
	req.Header.Set(HeaderSignature, base64.StdEncoding.EncodeToString(sig))
	return nil
}

// VerifyRequest checks r's Amber-* headers against body: the timestamp must
// be within window of now and the signature must verify over the
// reconstructed payload with the claimed public key. It returns the claimed
// key and the nonce; the caller still must check the nonce for replay and
// the key against an allowlist. Callers should map any non-nil error to a 401
// response. The nonce is returned even on failure so error responses can be
// signed over it.
func VerifyRequest(r *http.Request, body []byte, now time.Time, window time.Duration) (ssh.PublicKey, []byte, error) {
	pubB64 := r.Header.Get(HeaderPublicKey)
	tsStr := r.Header.Get(HeaderTimestamp)
	nonceB64 := r.Header.Get(HeaderNonce)
	sigB64 := r.Header.Get(HeaderSignature)
	if pubB64 == "" || tsStr == "" || nonceB64 == "" || sigB64 == "" {
		return nil, nil, errors.New("request is not signed (missing Amber-* headers)")
	}
	pubWire, err := base64.StdEncoding.DecodeString(pubB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderPublicKey, err)
	}
	pub, err := ssh.ParsePublicKey(pubWire)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing public key: %w", err)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing %s: %w", HeaderTimestamp, err)
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderNonce, err)
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", HeaderSignature, err)
	}
	d := now.Sub(time.Unix(0, ts))
	if d < -window || d > window {
		return nil, nonce, fmt.Errorf("request timestamp outside the ±%s window", window)
	}
	payload, err := requestSigPayload(r.Method, r.URL.RequestURI(), ts, nonce, HashBody(body))
	if err != nil {
		return nil, nonce, fmt.Errorf("encoding request signature payload: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, sig, pubWire, Namespace); err != nil {
		return nil, nonce, fmt.Errorf("request signature: %w", err)
	}
	return pub, nonce, nil
}

// SignResponse signs {nonce, status, bodyHash} with the server identity,
// returning the base64 header/trailer value. nonce is the request's nonce,
// binding the response to its request.
func SignResponse(signer ssh.Signer, nonce []byte, status int, bodyHash []byte) (string, error) {
	payload, err := encMode.Marshal(responsePayload{
		Nonce:    notNil(nonce),
		Status:   int64(status),
		BodyHash: notNil(bodyHash),
	})
	if err != nil {
		return "", fmt.Errorf("encoding response signature payload: %w", err)
	}
	sig, err := sshsign.SignNamespace(signer, payload, Namespace)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// VerifyResponse checks a response signature against the pinned server key
// (SSH wire format), the request nonce, the response status and body hash.
func VerifyResponse(serverPubWire, nonce []byte, status int, bodyHash []byte, sigB64 string) error {
	if sigB64 == "" {
		return errors.New("response is not signed (missing Amber-Signature)")
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("decoding response signature: %w", err)
	}
	payload, err := encMode.Marshal(responsePayload{
		Nonce:    notNil(nonce),
		Status:   int64(status),
		BodyHash: notNil(bodyHash),
	})
	if err != nil {
		return fmt.Errorf("encoding response signature payload: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(payload, sig, serverPubWire, Namespace); err != nil {
		return fmt.Errorf("response signature: %w", err)
	}
	return nil
}
