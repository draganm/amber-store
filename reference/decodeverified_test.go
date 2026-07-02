package reference_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/sshsign"
	"golang.org/x/crypto/ssh"
)

func TestDecodeVerified(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := fstree.EncodeBlob([]byte("payload")) // a real canonical key
	if err != nil {
		t.Fatal(err)
	}
	rec := reference.Reference{Name: "a:1", Key: blob.Key[:], User: "u", CreatedAt: 42}
	// PublicKey must be set before SignaturePayload is computed: the payload
	// binds the signer's key (see Reference.SignaturePayload), matching
	// signReference in cmd/amber-store/sign.go and every other call site.
	rec.PublicKey = signer.PublicKey().Marshal()
	payload, err := rec.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := sshsign.SignWith(signer, payload)
	if err != nil {
		t.Fatal(err)
	}
	rec.Signature = sig
	raw, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.DecodeVerified(raw); err != nil {
		t.Fatal(err)
	}

	t.Run("unsigned record is rejected", func(t *testing.T) {
		u := rec
		u.Signature, u.PublicKey = nil, nil
		rawUnsigned, err := u.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reference.DecodeVerified(rawUnsigned); err == nil {
			t.Fatal("unsigned record must be rejected")
		}
	})
	t.Run("tampered signature is rejected", func(t *testing.T) {
		bad := rec
		bad.Signature = append([]byte(nil), rec.Signature...)
		bad.Signature[len(bad.Signature)/2] ^= 0xff
		rawBad, err := bad.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reference.DecodeVerified(rawBad); err == nil {
			t.Fatal("tampered record must be rejected")
		}
	})
}
