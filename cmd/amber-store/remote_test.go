package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/remotes"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

// startRemoteServer runs an in-process remote server allowing clientPub.
func startRemoteServer(t *testing.T, clientPub ssh.PublicKey) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	store, err := packstore.Open(filepath.Join(dir, "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ib.Close() })
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	allow, err := allowlist.Parse(ssh.MarshalAuthorizedKey(clientPub))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs, Collector: openTestCollector(t, store, refs),
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startDaemonWithRemotes mirrors startDaemon but enables remote support with
// the given default signer (the daemon handler is built directly, like
// startDaemon, to avoid racing on urfave/cli globals).
func startDaemonWithRemotes(t *testing.T, signer ssh.Signer) string {
	t.Helper()
	store, err := packstore.Open(t.TempDir(), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(t.TempDir(), "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	registry, err := remotes.Open(filepath.Join(t.TempDir(), "remotes"))
	if err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("", "amber-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.NewWithRemotes(store, refs, openTestCollector(t, store, refs), nil, &daemon.RemoteConfig{
		Registry:      registry,
		DefaultSigner: signer,
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// signerFromKeyPath loads the unencrypted test key written by writeSigningKey.
func signerFromKeyPath(t *testing.T, path string) ssh.Signer {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

// runApp runs the CLI with stdin/stdout wired, returning stdout.
func runApp(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	app := newApp()
	var out bytes.Buffer
	app.Reader = strings.NewReader(stdin)
	app.Writer = &out
	err := app.Run(append([]string{"amber-store"}, args...))
	return out.String(), err
}

func TestRemoteAddConfirmAndLs(t *testing.T) {
	keyPath, pub := writeSigningKey(t)
	signer := signerFromKeyPath(t, keyPath)
	sock := startDaemonWithRemotes(t, signer)
	srv := startRemoteServer(t, pub)

	// declining the fingerprint aborts
	if _, err := runApp(t, "n\n", "remote", "add", "--socket", sock, "origin", srv.URL); err == nil {
		t.Fatal("declined add succeeded")
	}
	out, err := runApp(t, "", "remote", "ls", "--socket", sock)
	if err != nil || strings.Contains(out, "origin") {
		t.Fatalf("remote listed after declined add: %q, %v", out, err)
	}

	// confirming records it
	out, err = runApp(t, "y\n", "remote", "add", "--socket", sock, "origin", srv.URL)
	if err != nil {
		t.Fatalf("remote add: %v", err)
	}
	if !strings.Contains(out, "SHA256:") {
		t.Fatalf("add output shows no fingerprint: %q", out)
	}
	out, err = runApp(t, "", "remote", "ls", "--socket", sock)
	if err != nil || !strings.Contains(out, "origin") || !strings.Contains(out, srv.URL) {
		t.Fatalf("remote ls = %q, %v", out, err)
	}

	// --yes skips the prompt
	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "--yes", "second", srv.URL); err != nil {
		t.Fatalf("add --yes: %v", err)
	}

	// rm removes
	if _, err := runApp(t, "", "remote", "rm", "--socket", sock, "second"); err != nil {
		t.Fatalf("remote rm: %v", err)
	}

	// piped confirmation without trailing newline is accepted
	if _, err := runApp(t, "y", "remote", "add", "--socket", sock, "third", srv.URL); err != nil {
		t.Fatalf("add with newline-less confirmation: %v", err)
	}
}

func TestRemotePushPullCycle(t *testing.T) {
	keyPath, pub := writeSigningKey(t)
	signer := signerFromKeyPath(t, keyPath)
	configureTestUserWithKey(t, "tester", keyPath) // signing key → signed refs
	sock := startDaemonWithRemotes(t, signer)
	srv := startRemoteServer(t, pub)
	if _, err := runApp(t, "y\n", "remote", "add", "--socket", sock, "origin", srv.URL); err != nil {
		t.Fatal(err)
	}

	// ingest a directory and push it
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("push me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, "", "ingest", "--no-progress", "--socket", sock, "snap", dir); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	out, err := runApp(t, "", "remote", "push", "--socket", sock, "origin", "snap")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if !strings.Contains(out, "pushed") {
		t.Fatalf("push output: %q", out)
	}
	out, err = runApp(t, "", "remote", "ls-refs", "--socket", sock, "origin")
	if err != nil || !strings.Contains(out, "snap") {
		t.Fatalf("ls-refs = %q, %v", out, err)
	}

	// pull into a second daemon (single remote → REMOTE arg omitted)
	sock2 := startDaemonWithRemotes(t, signer)
	if _, err := runApp(t, "y\n", "remote", "add", "--socket", sock2, "origin", srv.URL); err != nil {
		t.Fatal(err)
	}
	out, err = runApp(t, "", "remote", "pull", "--socket", sock2, "snap")
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if !strings.Contains(out, "root ") {
		t.Fatalf("pull output shows no root key: %q", out)
	}
	// the content round-trips
	restoreDir := t.TempDir()
	if _, err := runApp(t, "", "restore", "--socket", sock2, "ref:snap", restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restoreDir, "f.txt"))
	if err != nil || string(got) != "push me" {
		t.Fatalf("restored content = %q, %v", got, err)
	}
}

func TestRemoteArgParsing(t *testing.T) {
	keyPath, _ := writeSigningKey(t)
	signer := signerFromKeyPath(t, keyPath)
	sock := startDaemonWithRemotes(t, signer)
	if _, err := runApp(t, "", "remote", "push", "--socket", sock); err == nil {
		t.Fatal("no-arg push succeeded")
	}
	if _, err := runApp(t, "", "remote", "push", "--socket", sock, "a", "b", "c"); err == nil {
		t.Fatal("3-arg push succeeded")
	}
	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "only-name"); err == nil {
		t.Fatal("add without URL succeeded")
	}
}

// TestRemotePushWithAutoIdentity pushes through a daemon whose transport
// signer is the auto-generated store identity, against a server whose
// allowlist holds the generated identity.pub.
func TestRemotePushWithAutoIdentity(t *testing.T) {
	storeDir := t.TempDir()
	signer, err := defaultRemoteSigner(nil, storeDir) // what runDaemon does without --remote-key
	if err != nil {
		t.Fatal(err)
	}

	pubBytes, err := os.ReadFile(filepath.Join(storeDir, "identity.pub"))
	if err != nil {
		t.Fatal(err)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(pubBytes)
	if err != nil {
		t.Fatal(err)
	}
	srv := startRemoteServer(t, pub)
	sock := startDaemonWithRemotes(t, signer)

	refKeyPath, _ := writeSigningKey(t)
	configureTestUserWithKey(t, "tester", refKeyPath)

	if _, err := runApp(t, "", "remote", "add", "--socket", sock, "--yes", "origin", srv.URL); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("auto identity"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runApp(t, "", "ingest", "--no-progress", "--socket", sock, "snap", dir); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	out, err := runApp(t, "", "remote", "push", "--socket", sock, "origin", "snap")
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if !strings.Contains(out, "pushed") {
		t.Fatalf("push output: %q", out)
	}
}
