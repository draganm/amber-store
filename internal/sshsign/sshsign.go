// Package sshsign produces SSHSIG signatures (the ssh-keygen -Y / git SSH
// signing format) over reference signature payloads, using either a private
// key file or a key held by the ssh-agent (selected by a .pub file).
package sshsign

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
)

// Namespace is the SSHSIG namespace for amber-store reference signatures;
// namespaces prevent a signature from being replayed in another protocol.
const Namespace = "amber-store-ref"

// PassphrasePrompt obtains the passphrase for the encrypted key at path.
type PassphrasePrompt func(path string) ([]byte, error)

// Sign signs payload with the key at keyPath and returns a raw (un-armored)
// SSHSIG blob. A public-key file selects the matching ssh-agent key; a
// private-key file is parsed directly, calling prompt at most once if it is
// encrypted.
func Sign(keyPath string, payload []byte, prompt PassphrasePrompt) ([]byte, error) {
	b, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("reading signing key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		var miss *ssh.PassphraseMissingError
		if !errors.As(err, &miss) {
			return nil, fmt.Errorf("parsing signing key %s: %w", keyPath, err)
		}
		pass, perr := prompt(keyPath)
		if perr != nil {
			return nil, fmt.Errorf("reading passphrase for %s: %w", keyPath, perr)
		}
		signer, err = ssh.ParsePrivateKeyWithPassphrase(b, pass)
		if err != nil {
			return nil, fmt.Errorf("decrypting signing key %s: %w", keyPath, err)
		}
	}
	return rawSign(signer, payload)
}

// rawSign wraps a one-shot SSHSIG signing, returning the binary blob.
func rawSign(signer ssh.Signer, payload []byte) ([]byte, error) {
	sig, err := sshsig.Sign(bytes.NewReader(payload), signer, sshsig.HashSHA512, Namespace)
	if err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}
	return sig.Marshal(), nil
}
