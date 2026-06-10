package sshsign_test

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
)

// noPrompt fails the test if the passphrase prompt is invoked.
func noPrompt(t *testing.T) sshsign.PassphrasePrompt {
	return func(path string) ([]byte, error) {
		t.Fatalf("passphrase prompt invoked for %s", path)
		return nil, nil
	}
}

// writeKeyFiles writes an OpenSSH private key (encrypted when passphrase is
// non-empty) plus its .pub next to it, returning both paths and the public key.
func writeKeyFiles(t *testing.T, priv crypto.PrivateKey, passphrase string) (privPath, pubPath string, pub ssh.PublicKey) {
	t.Helper()
	var block *pem.Block
	var err error
	if passphrase == "" {
		block, err = ssh.MarshalPrivateKey(priv, "")
	} else {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte(passphrase))
	}
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	pub = signer.PublicKey()
	dir := t.TempDir()
	privPath = filepath.Join(dir, "key")
	pubPath = filepath.Join(dir, "key.pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubPath, ssh.MarshalAuthorizedKey(pub), 0o644); err != nil {
		t.Fatal(err)
	}
	return privPath, pubPath, pub
}

// mustVerify checks blob is a valid SSHSIG over payload by pub in our namespace.
func mustVerify(t *testing.T, blob, payload []byte, pub ssh.PublicKey) {
	t.Helper()
	sig, err := sshsig.ParseSignature(blob)
	if err != nil {
		t.Fatalf("parsing produced signature: %v", err)
	}
	if err := sshsig.Verify(bytes.NewReader(payload), sig, pub, sshsig.HashSHA512, sshsign.Namespace); err != nil {
		t.Fatalf("verifying produced signature: %v", err)
	}
}

func TestSignWithEd25519File(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(privPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignWithRSAFile(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, err := sshsign.Sign(privPath, payload, noPrompt(t))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignMissingKeyFile(t *testing.T) {
	_, err := sshsign.Sign(filepath.Join(t.TempDir(), "absent"), []byte("p"), noPrompt(t))
	if err == nil {
		t.Fatal("expected error for missing key file")
	}
}

func TestSignWithEncryptedKey(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "letmein")
	payload := []byte("payload bytes")
	calls := 0
	blob, err := sshsign.Sign(privPath, payload, func(path string) ([]byte, error) {
		calls++
		if path != privPath {
			t.Fatalf("prompt path = %q, want %q", path, privPath)
		}
		return []byte("letmein"), nil
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if calls != 1 {
		t.Fatalf("prompt called %d times, want 1", calls)
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignWithWrongPassphrase(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, _ := writeKeyFiles(t, priv, "letmein")
	_, err = sshsign.Sign(privPath, []byte("p"), func(string) ([]byte, error) {
		return []byte("wrong"), nil
	})
	if err == nil {
		t.Fatal("expected error for wrong passphrase")
	}
}
