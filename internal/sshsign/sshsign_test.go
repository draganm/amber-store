package sshsign_test

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
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

// mustSign resolves the key at keyPath, signs payload, and returns the blob
// together with the resolved signer's public key.
func mustSign(t *testing.T, keyPath string, payload []byte, prompt sshsign.PassphrasePrompt) ([]byte, ssh.PublicKey) {
	t.Helper()
	signer, closeSigner, err := sshsign.Signer(keyPath, prompt)
	if err != nil {
		t.Fatalf("Signer: %v", err)
	}
	defer closeSigner()
	blob, err := sshsign.SignWith(signer, payload)
	if err != nil {
		t.Fatalf("SignWith: %v", err)
	}
	return blob, signer.PublicKey()
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
	blob, got := mustSign(t, privPath, payload, noPrompt(t))
	if !bytes.Equal(got.Marshal(), pub.Marshal()) {
		t.Fatal("resolved signer public key differs from the key file's")
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
	blob, got := mustSign(t, privPath, payload, noPrompt(t))
	if !bytes.Equal(got.Marshal(), pub.Marshal()) {
		t.Fatal("resolved signer public key differs from the key file's")
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignMissingKeyFile(t *testing.T) {
	_, _, err := sshsign.Signer(filepath.Join(t.TempDir(), "absent"), noPrompt(t))
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
	blob, _ := mustSign(t, privPath, payload, func(path string) ([]byte, error) {
		calls++
		if path != privPath {
			t.Fatalf("prompt path = %q, want %q", path, privPath)
		}
		return []byte("letmein"), nil
	})
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
	_, _, err = sshsign.Signer(privPath, func(string) ([]byte, error) {
		return []byte("wrong"), nil
	})
	if err == nil {
		t.Fatal("expected error for wrong passphrase")
	}
}

// startTestAgent serves an in-memory ssh-agent holding keys over a unix
// socket and points $SSH_AUTH_SOCK at it.
func startTestAgent(t *testing.T, keys ...crypto.PrivateKey) {
	t.Helper()
	kr := agent.NewKeyring()
	for _, k := range keys {
		if err := kr.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			t.Fatal(err)
		}
	}
	// Short dir: unix sun_path is capped at ~104 bytes on macOS/BSD and
	// t.TempDir() embeds the long test name.
	dir, err := os.MkdirTemp("", "amber-agent-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go agent.ServeAgent(kr, conn)
		}
	}()
	t.Setenv("SSH_AUTH_SOCK", sock)
}

func TestSignWithAgentKey(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, pub := writeKeyFiles(t, priv, "")
	startTestAgent(t, priv)
	payload := []byte("payload bytes")
	blob, got := mustSign(t, pubPath, payload, noPrompt(t))
	if !bytes.Equal(got.Marshal(), pub.Marshal()) {
		t.Fatal("resolved agent public key differs from the .pub file's")
	}
	mustVerify(t, blob, payload, pub)
}

func TestSignAgentKeyNotLoaded(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, pub := writeKeyFiles(t, priv, "")
	startTestAgent(t, otherPriv) // agent holds a different key
	_, _, err = sshsign.Signer(pubPath, noPrompt(t))
	if err == nil {
		t.Fatal("expected error when key absent from agent")
	}
	if want := ssh.FingerprintSHA256(pub); !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name fingerprint %s", err, want)
	}
}

func TestSignAgentUnavailable(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, pubPath, _ := writeKeyFiles(t, priv, "")
	t.Setenv("SSH_AUTH_SOCK", "")
	_, _, err = sshsign.Signer(pubPath, noPrompt(t))
	if err == nil || !strings.Contains(err.Error(), "SSH_AUTH_SOCK") {
		t.Fatalf("error = %v, want mention of SSH_AUTH_SOCK", err)
	}
}

// fakeSKKeyPEM builds an openssh-key-v1 container whose embedded public key
// announces a FIDO2 sk- type. Only the cleartext header matters for the
// detection under test; no private section is needed.
func fakeSKKeyPEM(t *testing.T) []byte {
	t.Helper()
	pubBlob := ssh.Marshal(struct {
		Algo  string
		Bytes []byte
	}{"sk-ssh-ed25519@openssh.com", make([]byte, 32)})
	body := append([]byte("openssh-key-v1\x00"), ssh.Marshal(struct {
		CipherName, KdfName, KdfOpts string
		NumKeys                      uint32
		PubKey                       []byte
	}{"none", "none", "", 1, pubBlob})...)
	return pem.EncodeToMemory(&pem.Block{Type: "OPENSSH PRIVATE KEY", Bytes: body})
}

func TestSignRejectsSKKeyFileWithHint(t *testing.T) {
	p := filepath.Join(t.TempDir(), "id_ed25519_sk")
	if err := os.WriteFile(p, fakeSKKeyPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := sshsign.Signer(p, noPrompt(t))
	if err == nil || !strings.Contains(err.Error(), ".pub") || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("error = %v, want sk- hint mentioning agent and .pub", err)
	}
}

// TestSignatureVerifiesWithOpenSSH proves "verify later" works with stock
// tooling: armor a produced signature and check ssh-keygen -Y verify accepts
// it against an allowed_signers entry.
func TestSignatureVerifiesWithOpenSSH(t *testing.T) {
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("ssh-keygen not on PATH")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privPath, _, pub := writeKeyFiles(t, priv, "")
	payload := []byte("payload bytes")
	blob, _ := mustSign(t, privPath, payload, noPrompt(t))
	sig, err := sshsig.ParseSignature(blob)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	sigPath := filepath.Join(dir, "ref.sig")
	if err := os.WriteFile(sigPath, sshsig.Armor(sig), 0o644); err != nil {
		t.Fatal(err)
	}
	signersPath := filepath.Join(dir, "allowed_signers")
	line := "tester@amber " + string(ssh.MarshalAuthorizedKey(pub))
	if err := os.WriteFile(signersPath, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("ssh-keygen", "-Y", "verify",
		"-f", signersPath, "-I", "tester@amber",
		"-n", sshsign.Namespace, "-s", sigPath)
	cmd.Stdin = bytes.NewReader(payload)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh-keygen -Y verify failed: %v\n%s", err, out)
	}
}

func TestCheckKeyFile(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plainPath, pubPath, _ := writeKeyFiles(t, priv, "")
	_, encPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encPath, _, _ := writeKeyFiles(t, encPriv, "letmein")
	skPath := filepath.Join(t.TempDir(), "sk")
	if err := os.WriteFile(skPath, fakeSKKeyPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	garbagePath := filepath.Join(t.TempDir(), "garbage")
	if err := os.WriteFile(garbagePath, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, path string
		wantErr    bool
	}{
		{"private key", plainPath, false},
		{"public key", pubPath, false},
		{"encrypted private key", encPath, false},
		{"sk private key", skPath, true},
		{"garbage", garbagePath, true},
		{"missing", filepath.Join(t.TempDir(), "absent"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sshsign.CheckKeyFile(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckKeyFile(%s) = %v, wantErr %v", tc.path, err, tc.wantErr)
			}
		})
	}
}
