package remoteclient_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

// newGCHarness is newHarness with a collector wired into the server.
func newGCHarness(t *testing.T) *harness {
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
	coll, err := gc.Open(filepath.Join(dir, "closures"), store, refs, gc.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 2, nil, inbox.WithLeaser(inbox.LeaserOf(coll.Lease)))
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
		Store: store, Inbox: ib, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
		GC:       coll,
	}))
	t.Cleanup(srv.Close)
	return &harness{srv: srv, store: store, inbox: ib, refs: refs, identity: identity, client: client, admin: admin}
}

func TestGCMethods(t *testing.T) {
	h := newGCHarness(t)
	ctx := context.Background()
	o, err := fstree.EncodeBlob([]byte("remote gc blob"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	rc := h.rc(t)
	if err := rc.PutRef(ctx, "v", signedRecord(t, "v", o.Key[:], h.client)); err != nil {
		t.Fatal(err)
	}
	st, err := rc.GCStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Refs != 1 || st.Union == 0 {
		t.Fatalf("status = %+v, want 1 ref and a nonempty union", st)
	}
	names, err := rc.GCWhy(ctx, o.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "v" {
		t.Fatalf("why = %v, want [v]", names)
	}
	// Run needs admin: the read/write client key is refused.
	if _, err := rc.GCRun(ctx, -1); err == nil {
		t.Fatal("gc run with a non-admin key succeeded")
	}
	arc, err := remoteclient.New(h.srv.URL, h.admin, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arc.GCRun(ctx, -1); err != nil {
		t.Fatalf("gc run with the admin key: %v", err)
	}
}
