// Package sshsign produces SSHSIG signatures (the ssh-keygen -Y / git SSH
// signing format) over reference signature payloads, using either a private
// key file or a key held by the ssh-agent (selected by a .pub file).
package sshsign

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/hiddeco/sshsig"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/term"
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
	if pub, _, _, _, perr := ssh.ParseAuthorizedKey(b); perr == nil {
		return signWithAgent(pub, payload)
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

// TTYPrompt reads a passphrase from the controlling terminal without echo.
// It is the PassphrasePrompt used outside tests.
func TTYPrompt(path string) ([]byte, error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("opening terminal to prompt for passphrase (key %s): %w", path, err)
	}
	defer tty.Close()
	fmt.Fprintf(tty, "Enter passphrase for %s: ", path)
	defer fmt.Fprintln(tty)
	return term.ReadPassword(int(tty.Fd()))
}

// rawSign wraps a one-shot SSHSIG signing, returning the binary blob.
func rawSign(signer ssh.Signer, payload []byte) ([]byte, error) {
	sig, err := sshsig.Sign(bytes.NewReader(payload), signer, sshsig.HashSHA512, Namespace)
	if err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}
	return sig.Marshal(), nil
}

// signWithAgent signs with the agent key matching pub.
func signWithAgent(pub ssh.PublicKey, payload []byte) ([]byte, error) {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil, errors.New("signing key is a public key, but no ssh-agent is available ($SSH_AUTH_SOCK is not set)")
	}
	conn, err := net.Dial("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("connecting to ssh-agent: %w", err)
	}
	defer conn.Close()
	signers, err := agent.NewClient(conn).Signers()
	if err != nil {
		return nil, fmt.Errorf("listing ssh-agent keys: %w", err)
	}
	want := pub.Marshal()
	for _, s := range signers {
		if bytes.Equal(s.PublicKey().Marshal(), want) {
			return rawSign(s, payload)
		}
	}
	return nil, fmt.Errorf("signing key %s is not loaded in the ssh-agent", ssh.FingerprintSHA256(pub))
}
