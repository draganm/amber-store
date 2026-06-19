// Package identity manages a store's own SSH identity: an ed25519 keypair
// generated on first use and persisted in the store directory, used by the
// remote server and by the daemon's remote sync when no explicit key is
// configured.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
)

// Comment is the comment embedded in generated keys.
const Comment = "amber-store"

// LoadOrCreate returns the store's own SSH identity, generating an ed25519
// keypair on first use. The private key lives at <storeDir>/identity (0600)
// and its public half at <storeDir>/identity.pub (0644) — the latter is what
// an operator copies into a server's allowed-keys file. An existing identity
// that cannot be parsed (including passphrase-protected keys) is an error;
// the file is never overwritten, because the key may already be trusted
// elsewhere.
func LoadOrCreate(storeDir string) (ssh.Signer, error) {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating store directory: %w", err)
	}
	keyPath := filepath.Join(storeDir, "identity")
	b, err := os.ReadFile(keyPath)
	switch {
	case err == nil:
		return load(keyPath, b)
	case os.IsNotExist(err):
		return create(keyPath)
	default:
		return nil, fmt.Errorf("reading identity %s: %w", keyPath, err)
	}
}

// load parses an existing identity and self-heals a missing .pub so the
// public key is always available for copy-paste.
func load(keyPath string, b []byte) (ssh.Signer, error) {
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		var miss *ssh.PassphraseMissingError
		if errors.As(err, &miss) {
			return nil, fmt.Errorf("identity %s is passphrase-protected; auto-managed store identities must be unencrypted — configure the key explicitly instead", keyPath)
		}
		return nil, fmt.Errorf("parsing identity %s: %w", keyPath, err)
	}
	pubPath := keyPath + ".pub"
	if _, err := os.Stat(pubPath); os.IsNotExist(err) {
		if err := writePub(pubPath, signer.PublicKey()); err != nil {
			return nil, err
		}
	}
	return signer, nil
}

func create(keyPath string) (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating identity: %w", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, Comment)
	if err != nil {
		return nil, fmt.Errorf("encoding identity: %w", err)
	}
	// Temp-file + rename so a crash cannot leave a half-written key behind.
	tmp := keyPath + ".tmp"
	if err := os.WriteFile(tmp, pem.EncodeToMemory(block), 0o600); err != nil {
		return nil, fmt.Errorf("writing identity: %w", err)
	}
	if err := os.Rename(tmp, keyPath); err != nil {
		return nil, fmt.Errorf("writing identity: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, fmt.Errorf("building signer: %w", err)
	}
	if err := writePub(keyPath+".pub", signer.PublicKey()); err != nil {
		return nil, err
	}
	return signer, nil
}

// writePub writes an authorized_keys-format line with the package comment.
func writePub(pubPath string, pub ssh.PublicKey) error {
	line := bytes.TrimRight(ssh.MarshalAuthorizedKey(pub), "\n")
	line = append(line, ' ')
	line = append(line, Comment...)
	line = append(line, '\n')
	if err := os.WriteFile(pubPath, line, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", pubPath, err)
	}
	return nil
}
