package grant

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/sshsign"
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

func TestSignVerifyRoundTrip(t *testing.T) {
	issuer, subject := testSigner(t), testSigner(t)
	now := time.Now()
	g := Grant{
		Subject:   subject.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead, allowlist.CapPushObjects},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}
	raw, err := Sign(g, issuer)
	if err != nil {
		t.Fatal(err)
	}
	got, issuerWire, err := Verify(raw, subject.PublicKey().Marshal(), now, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if string(issuerWire) != string(issuer.PublicKey().Marshal()) {
		t.Fatal("issuer key mismatch")
	}
	if len(got.Caps) != 2 {
		t.Fatalf("caps: %v", got.Caps)
	}
}

func TestVerifyRejections(t *testing.T) {
	issuer, subject, other := testSigner(t), testSigner(t), testSigner(t)
	now := time.Now()
	window := 5 * time.Minute
	fresh := func(expiresIn time.Duration) []byte {
		t.Helper()
		raw, err := Sign(Grant{
			Subject:   subject.PublicKey().Marshal(),
			Caps:      []string{allowlist.CapRead},
			IssuedAt:  now.UnixNano(),
			ExpiresAt: now.Add(expiresIn).UnixNano(),
		}, issuer)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	t.Run("wrong subject", func(t *testing.T) {
		if _, _, err := Verify(fresh(15*time.Minute), other.PublicKey().Marshal(), now, window); err == nil {
			t.Fatal("expected subject mismatch error")
		}
	})
	t.Run("expired beyond window", func(t *testing.T) {
		raw := fresh(time.Minute)
		late := now.Add(time.Minute + window + time.Second)
		if _, _, err := Verify(raw, subject.PublicKey().Marshal(), late, window); err == nil {
			t.Fatal("expected expiry error")
		}
	})
	t.Run("expired within window still passes", func(t *testing.T) {
		raw := fresh(time.Minute)
		slightlyLate := now.Add(time.Minute + window - time.Second)
		if _, _, err := Verify(raw, subject.PublicKey().Marshal(), slightlyLate, window); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("not yet valid", func(t *testing.T) {
		raw := fresh(15 * time.Minute)
		early := now.Add(-window - time.Second)
		if _, _, err := Verify(raw, subject.PublicKey().Marshal(), early, window); err == nil {
			t.Fatal("expected not-yet-valid error")
		}
	})
	t.Run("tampered payload", func(t *testing.T) {
		raw := fresh(15 * time.Minute)
		raw[len(raw)/2] ^= 0xff
		if _, _, err := Verify(raw, subject.PublicKey().Marshal(), now, window); err == nil {
			t.Fatal("expected verification error")
		}
	})
	t.Run("sign refuses privileged caps", func(t *testing.T) {
		for _, c := range []string{allowlist.CapWriteRefs, allowlist.CapDelegate, allowlist.CapAdmin} {
			_, err := Sign(Grant{
				Subject:   subject.PublicKey().Marshal(),
				Caps:      []string{c},
				IssuedAt:  now.UnixNano(),
				ExpiresAt: now.Add(time.Minute).UnixNano(),
			}, issuer)
			if err == nil {
				t.Fatalf("Sign accepted cap %q", c)
			}
		}
	})
	t.Run("verify refuses privileged caps in a hand-rolled envelope", func(t *testing.T) {
		// Bypass Sign to forge a write-refs grant; Verify must still reject it.
		payload, err := encMode.Marshal(Grant{
			Subject:   subject.PublicKey().Marshal(),
			Caps:      []string{allowlist.CapWriteRefs},
			IssuedAt:  now.UnixNano(),
			ExpiresAt: now.Add(time.Minute).UnixNano(),
		})
		if err != nil {
			t.Fatal(err)
		}
		sig, err := sshsign.SignNamespace(issuer, payload, Namespace)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := encMode.Marshal(envelope{Payload: payload, Signature: sig, IssuerKey: issuer.PublicKey().Marshal()})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := Verify(raw, subject.PublicKey().Marshal(), now, window); err == nil {
			t.Fatal("Verify accepted a forged write-refs grant")
		}
	})
	t.Run("garbage", func(t *testing.T) {
		if _, _, err := Verify([]byte("junk"), subject.PublicKey().Marshal(), now, window); err == nil {
			t.Fatal("expected decode error")
		}
	})
}
