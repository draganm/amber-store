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

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
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
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs, Collector: openTestCollector(t, store, refs),
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, inbox: ib, refs: refs, identity: identity, client: client, admin: admin}
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

func TestReachableKeys_RoundTrip(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)

	// Build and store a small tree: a directory leaf referencing one blob.
	blob, err := fstree.EncodeBlob([]byte("hello reachable"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{blob, leaf} {
		if err := h.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}

	got, err := c.ReachableKeys(context.Background(), leaf.Key)
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	want, err := fstree.ReachableKeys(leaf.Key, h.store.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	if len(got) == 0 || got[0] != leaf.Key {
		t.Fatalf("first key = %v, want the root %s", got, leaf.Key)
	}
	gotSet := map[key.Key]bool{}
	for _, k := range got {
		gotSet[k] = true
	}
	for _, k := range want {
		if !gotSet[k] {
			t.Errorf("missing reachable key %s", k)
		}
	}
}

func TestReachableKeys_IncompleteTreeErrors(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)

	// Build a DirLeaf (non-leaf interior node) but do NOT store it.
	// Then build a DirNode that references it and store only the DirNode.
	// When the server walks the DirNode it will try to fetch the missing
	// DirLeaf and fail, returning a non-2xx that the client surfaces as an error.
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644},
	})
	if err != nil {
		t.Fatal(err)
	}
	node, err := fstree.EncodeDirNode([]fstree.DirPair{
		{SepName: []byte("f"), ChildKey: leaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Store only the DirNode; the DirLeaf child is absent.
	if err := h.store.Put(node.Key, node.Bytes); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReachableKeys(context.Background(), node.Key); err == nil {
		t.Fatal("expected an error walking an incomplete tree")
	}
}

// openTestCollector opens a GC collector over the pair, as serve does.
func openTestCollector(t *testing.T, store *packstore.Store, refs *refstore.Store) *gc.Collector {
	t.Helper()
	c, err := gc.Open(filepath.Join(t.TempDir(), "closures"), store, refs, gc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
