// Package grant implements delegated capability grants: a short-lived,
// SSHSIG-signed statement by an allowlisted "delegate" key that another key
// (the subject) may exercise a capability subset against the remote server —
// the mechanism that lets a build runner push objects and read references
// without ever being allowlisted. Grants are stateless: expiry lives in the
// token and the server stores nothing per subject. A grant can only convey
// read and push-objects; reference writes always require an allowlisted key.
package grant

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/sshsign"
	"github.com/fxamacker/cbor/v2"
	"golang.org/x/crypto/ssh"
)

// Namespace is the SSHSIG namespace for capability grants.
const Namespace = "amber-store-grant"

// Header carries the base64-encoded signed grant on remote-protocol requests.
const Header = "Amber-Grant"

// AllowedCaps is every capability a grant may convey.
var AllowedCaps = []string{allowlist.CapRead, allowlist.CapPushObjects}

// Grant is the signed statement: subject may exercise caps until ExpiresAt.
type Grant struct {
	Subject   []byte   `cbor:"0,keyasint"` // bearer's public key, SSH wire format
	Caps      []string `cbor:"1,keyasint"` // subset of AllowedCaps
	IssuedAt  int64    `cbor:"2,keyasint"` // ns since the Unix epoch
	ExpiresAt int64    `cbor:"3,keyasint"` // ns since the Unix epoch
}

// envelope is the wire form: the exact signed payload bytes, the SSHSIG over
// them, and the issuer's public key.
type envelope struct {
	Payload   []byte `cbor:"0,keyasint"`
	Signature []byte `cbor:"1,keyasint"`
	IssuerKey []byte `cbor:"2,keyasint"`
}

// encMode is the shared deterministic encoder, mirroring httpsig.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("grant: building CBOR enc mode: %v", err))
	}
	encMode = m
}

func validateCaps(caps []string) error {
	if len(caps) == 0 {
		return errors.New("grant carries no capabilities")
	}
	for _, c := range caps {
		if !slices.Contains(AllowedCaps, c) {
			return fmt.Errorf("capability %q cannot be delegated by a grant", c)
		}
	}
	return nil
}

// Sign encodes and signs g with the issuer key, returning the raw envelope
// (callers base64 it into the Header). Grants conveying anything outside
// AllowedCaps are refused.
func Sign(g Grant, issuer ssh.Signer) ([]byte, error) {
	if err := validateCaps(g.Caps); err != nil {
		return nil, err
	}
	if len(g.Subject) == 0 {
		return nil, errors.New("grant has no subject key")
	}
	slices.Sort(g.Caps)
	payload, err := encMode.Marshal(g)
	if err != nil {
		return nil, fmt.Errorf("encoding grant: %w", err)
	}
	sig, err := sshsign.SignNamespace(issuer, payload, Namespace)
	if err != nil {
		return nil, err
	}
	return encMode.Marshal(envelope{Payload: payload, Signature: sig, IssuerKey: issuer.PublicKey().Marshal()})
}

// Verify checks raw: the envelope decodes, its signature verifies against the
// embedded issuer key, the caps are delegable, the subject matches the
// presenting key, and now is within [IssuedAt-window, ExpiresAt+window] (the
// same clock tolerance as request timestamps). It returns the grant and the
// issuer's wire-format key; THE CALLER must still check that key against the
// allowlist for the delegate capability — this function alone establishes
// only integrity, not authority.
func Verify(raw, subjectPubWire []byte, now time.Time, window time.Duration) (Grant, []byte, error) {
	var env envelope
	if err := cbor.Unmarshal(raw, &env); err != nil {
		return Grant{}, nil, fmt.Errorf("decoding grant envelope: %w", err)
	}
	if _, err := sshsign.VerifyNamespace(env.Payload, env.Signature, env.IssuerKey, Namespace); err != nil {
		return Grant{}, nil, fmt.Errorf("grant signature: %w", err)
	}
	var g Grant
	if err := cbor.Unmarshal(env.Payload, &g); err != nil {
		return Grant{}, nil, fmt.Errorf("decoding grant payload: %w", err)
	}
	if err := validateCaps(g.Caps); err != nil {
		return Grant{}, nil, err
	}
	if !bytes.Equal(g.Subject, subjectPubWire) {
		return Grant{}, nil, errors.New("grant subject does not match the request key")
	}
	n := now.UnixNano()
	if n < g.IssuedAt-int64(window) {
		return Grant{}, nil, errors.New("grant is not valid yet")
	}
	if n > g.ExpiresAt+int64(window) {
		return Grant{}, nil, errors.New("grant is expired")
	}
	return g, env.IssuerKey, nil
}
