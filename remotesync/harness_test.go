package remotesync_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/key"
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
	store    *packstore.Store
	inbox    *inbox.Inbox
	refs     *refstore.Store
	identity ssh.Signer
	client   ssh.Signer
	admin    ssh.Signer
}

func newHarness(t *testing.T) *harness {
	return newHarnessMW(t, nil)
}

// newHarnessMW is newHarness with the server handler wrapped by mw (nil =
// unwrapped), so tests can observe or fail raw requests.
func newHarnessMW(t *testing.T, mw func(http.Handler) http.Handler) *harness {
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
	identity, client, admin := testSigner(t), testSigner(t), testSigner(t)
	content := string(ssh.MarshalAuthorizedKey(client.PublicKey())) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	var handler http.Handler = server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	})
	if mw != nil {
		handler = mw(handler)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, inbox: ib, refs: refs, identity: identity, client: client, admin: admin}
}

func (h *harness) rc(t *testing.T) *remoteclient.Client {
	t.Helper()
	c, err := remoteclient.New(h.srv.URL, h.client, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newLocalStore(t *testing.T) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(filepath.Join(t.TempDir(), "local"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
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
