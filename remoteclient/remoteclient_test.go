package remoteclient_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowlist"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/server"
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

type harness struct {
	srv      *httptest.Server
	store    *diskstore.Store
	refs     *refstore.Store
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	store, err := diskstore.Open(filepath.Join(dir, "store"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, refs: refs, identity: identity, client: client, admin: admin}
}

// rc is used by the object/ref method tests added in the next task.
func (h *harness) rc(t *testing.T) *remoteclient.Client {
	t.Helper()
	c, err := remoteclient.New(h.srv.URL, h.client, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFetchIdentity(t *testing.T) {
	h := newHarness(t)
	pubWire, err := remoteclient.FetchIdentity(context.Background(), h.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if string(pubWire) != string(h.identity.PublicKey().Marshal()) {
		t.Fatal("fetched identity differs from the server key")
	}
}

func TestPinnedKeyMismatchAborts(t *testing.T) {
	h := newHarness(t)
	wrongPin := testSigner(t).PublicKey().Marshal()
	c, err := remoteclient.New(h.srv.URL, h.client, wrongPin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Missing(context.Background(), nil); err == nil {
		t.Fatal("request against a mismatched pinned key succeeded")
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	if _, err := remoteclient.New("ftp://nope", testSigner(t), []byte("k")); err == nil {
		t.Fatal("want error for non-http(s) URL")
	}
}

func TestStatusErrorsCarryServerMessage(t *testing.T) {
	h := newHarness(t)
	stranger := testSigner(t)
	c, err := remoteclient.New(h.srv.URL, stranger, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Missing(context.Background(), nil)
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusForbidden {
		t.Fatalf("err = %v, want StatusError 403", err)
	}
}
