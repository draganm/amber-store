package daemon_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func testSignerD(t *testing.T) ssh.Signer {
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

// remoteHarness wires a local daemon (with remotes support) and a remote
// server together.
type remoteHarness struct {
	daemonSrv    *httptest.Server
	serverSrv    *httptest.Server
	store        *packstore.Store // local daemon's store
	refs         *refstore.Store  // local daemon's refs
	srvStore     *packstore.Store // remote server's store
	srvRefs      *refstore.Store
	identity     ssh.Signer
	clientSigner ssh.Signer // the SSH signer allowed by the remote server
	registry     *remotes.Registry
}

// newRemoteServer starts an in-process remote server allowing clientPub.
func newRemoteServer(t *testing.T, identity ssh.Signer, clientPub ssh.PublicKey) (*httptest.Server, *packstore.Store, *refstore.Store) {
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
	allow, err := allowlist.Parse(ssh.MarshalAuthorizedKey(clientPub))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return srv, store, refs
}

// newDaemonFor starts a local daemon with remote support against serverSrv.
func newDaemonFor(t *testing.T, serverSrv *httptest.Server, identity, client ssh.Signer, srvStore *packstore.Store, srvRefs *refstore.Store) *remoteHarness {
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
	registry, err := remotes.Open(filepath.Join(dir, "remotes"))
	if err != nil {
		t.Fatal(err)
	}
	h := daemon.NewWithRemotes(store, refs, slog.New(slog.DiscardHandler), &daemon.RemoteConfig{
		Registry:      registry,
		DefaultSigner: client,
	})
	daemonSrv := httptest.NewServer(h)
	t.Cleanup(daemonSrv.Close)
	return &remoteHarness{
		daemonSrv: daemonSrv, serverSrv: serverSrv,
		store: store, refs: refs, srvStore: srvStore, srvRefs: srvRefs,
		identity: identity, clientSigner: client, registry: registry,
	}
}

func newRemoteHarness(t *testing.T) *remoteHarness {
	t.Helper()
	identity, client := testSignerD(t), testSignerD(t)
	srv, srvStore, srvRefs := newRemoteServer(t, identity, client.PublicKey())
	h := newDaemonFor(t, srv, identity, client, srvStore, srvRefs)
	return h
}

func (h *remoteHarness) doReq(t *testing.T, method, pathQuery string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, h.daemonSrv.URL+pathQuery, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// addRemote registers the harness server under name via the daemon routes.
func (h *remoteHarness) addRemote(t *testing.T, name string) {
	t.Helper()
	code, body := h.doReq(t, "POST", "/v1/remotes/preflight?url="+url.QueryEscape(h.serverSrv.URL), nil)
	if code != 200 {
		t.Fatalf("preflight = %d: %s", code, body)
	}
	var pf struct {
		PublicKey   string `json:"public_key"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.Unmarshal(body, &pf); err != nil {
		t.Fatal(err)
	}
	if pf.Fingerprint != ssh.FingerprintSHA256(h.identity.PublicKey()) {
		t.Fatalf("fingerprint = %s", pf.Fingerprint)
	}
	addBody, err := json.Marshal(map[string]string{"public_key": pf.PublicKey})
	if err != nil {
		t.Fatal(err)
	}
	code, body = h.doReq(t, "PUT",
		"/v1/remotes?name="+name+"&url="+url.QueryEscape(h.serverSrv.URL), addBody)
	if code != 204 {
		t.Fatalf("add = %d: %s", code, body)
	}
}

// newRemoteHarnessWithServer attaches a fresh daemon to an existing harness's
// server, reusing the same allowed client signer so the new daemon can auth.
func newRemoteHarnessWithServer(t *testing.T, h *remoteHarness, client ssh.Signer) *remoteHarness {
	t.Helper()
	return newDaemonFor(t, h.serverSrv, h.identity, client, h.srvStore, h.srvRefs)
}

// putLocalRef stores a signed ref in the local daemon's refstore, pointing at
// root, signed by signer. Returns the canonical record bytes.
func (h *remoteHarness) putLocalRef(t *testing.T, name string, root key.Key, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name:      name,
		Key:       root[:],
		User:      "u@example.com",
		CreatedAt: time.Now().UnixNano(),
		PublicKey: signer.PublicKey().Marshal(),
	}
	payload, err := rec.SignaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := sshsign.SignWith(signer, payload)
	if err != nil {
		t.Fatal(err)
	}
	rec.Signature = sig
	b, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.refs.Put(name, b); err != nil {
		t.Fatal(err)
	}
	return b
}

// buildTree stores a small two-level tree in store and returns its root:
// DirLeaf{ file "f" → FileNode → [blob1, blob2] }.
func buildTree(t *testing.T, store *packstore.Store) key.Key {
	t.Helper()
	b1, err := fstree.EncodeBlob([]byte("blob one payload"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fstree.EncodeBlob([]byte("blob two payload"))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: fn.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{b1, b2, fn, leaf} {
		if err := store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return leaf.Key
}

func TestRemoteAddListRemove(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")

	// duplicate add → 409
	pub := base64.StdEncoding.EncodeToString(h.identity.PublicKey().Marshal())
	addBody, err := json.Marshal(map[string]string{"public_key": pub})
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := h.doReq(t, "PUT", "/v1/remotes?name=origin&url="+url.QueryEscape(h.serverSrv.URL), addBody); code != 409 {
		t.Fatalf("duplicate add = %d, want 409", code)
	}

	code, body := h.doReq(t, "GET", "/v1/remotes", nil)
	if code != 200 || !strings.Contains(string(body), `"origin"`) {
		t.Fatalf("list = %d: %s", code, body)
	}

	if code, _ := h.doReq(t, "DELETE", "/v1/remotes?name=origin", nil); code != 204 {
		t.Fatal("remove failed")
	}
	if code, _ := h.doReq(t, "DELETE", "/v1/remotes?name=origin", nil); code != 404 {
		t.Fatal("second remove should 404")
	}
}

func TestPreflightBadURL(t *testing.T) {
	h := newRemoteHarness(t)
	if code, _ := h.doReq(t, "POST", "/v1/remotes/preflight?url="+url.QueryEscape("http://127.0.0.1:1"), nil); code != 502 {
		t.Fatalf("unreachable preflight = %d, want 502", code)
	}
	if code, _ := h.doReq(t, "POST", "/v1/remotes/preflight", nil); code != 422 {
		t.Fatal("missing url should 422")
	}
}

func TestRemoteRoutesAbsentWithoutConfig(t *testing.T) {
	// daemon.New (no RemoteConfig) must not register the remote routes
	dir := t.TempDir()
	store, err := packstore.Open(filepath.Join(dir, "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer refs.Close()
	srv := httptest.NewServer(daemon.New(store, refs, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/remotes")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
