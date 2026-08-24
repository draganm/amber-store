package server_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

// newGCTestServer is newTestServer plus an open collector wired into the
// handler; the admin key also carries wipe.
func newGCTestServer(t *testing.T) (*testServer, *gc.Collector) {
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
		"admin,wipe " + string(ssh.MarshalAuthorizedKey(admin.PublicKey()))
	allow, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
		Inbox:    ib,
		GC:       coll,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testServer{srv: srv, store: store, refs: refs, inbox: ib,
		identity: identity, client: client, admin: admin}, coll
}

// TestNewRefusesUnleasedInboxWithGC pins the wiring guard: a collector
// without inbox leases would silently weaken the safety argument, so New
// refuses the configuration outright.
func TestNewRefusesUnleasedInboxWithGC(t *testing.T) {
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
	ib, err := inbox.Open(filepath.Join(dir, "inbox"), store, 1, nil) // no leaser
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ib.Close() })
	defer func() {
		if recover() == nil {
			t.Fatal("server.New accepted a collector with an unleased inbox")
		}
	}()
	identity := testSigner(t)
	server.New(server.Config{
		Store: store, Refs: refs, Inbox: ib, GC: coll, Identity: identity,
		Allow: func() *allowlist.List { return &allowlist.List{} },
	})
}

func TestGCRefLifecycleSigned(t *testing.T) {
	ts, coll := newGCTestServer(t)
	o, err := fstree.EncodeBlob([]byte("gc server blob"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	rec := signedRef(t, "v", o.Key[:], ts.client)
	if code, body := ts.signedDo(t, ts.client, http.MethodPut, "/v1/refs?name=v", rec); code != http.StatusNoContent {
		t.Fatalf("put ref: %d %s", code, body)
	}
	code, body := ts.signedDo(t, ts.client, http.MethodGet, "/v1/gc/why/"+hex.EncodeToString(o.Key[:]), nil)
	if code != http.StatusOK {
		t.Fatalf("gc why: %d %s", code, body)
	}
	var names []string
	if err := json.Unmarshal(body, &names); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "v" {
		t.Fatalf("gc why = %v, want [v]", names)
	}
	if code, body := ts.signedDo(t, ts.admin, http.MethodDelete, "/v1/refs?name=v", nil); code != http.StatusNoContent {
		t.Fatalf("delete ref: %d %s", code, body)
	}
	if got := coll.Counters().Union; got != 0 {
		t.Fatalf("union holds %d tails after delete, want 0", got)
	}
}

func TestGCRunNeedsAdmin(t *testing.T) {
	ts, _ := newGCTestServer(t)
	if code, _ := ts.signedDo(t, ts.client, http.MethodPost, "/v1/gc/run", nil); code != http.StatusForbidden {
		t.Fatalf("gc run with read key: %d, want 403", code)
	}
	code, body := ts.signedDo(t, ts.admin, http.MethodPost, "/v1/gc/run", nil)
	if code != http.StatusOK {
		t.Fatalf("gc run with admin key: %d %s", code, body)
	}
	var stats gc.CycleStats
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if code, _ := ts.signedDo(t, ts.client, http.MethodGet, "/v1/gc", nil); code != http.StatusOK {
		t.Fatalf("gc status with read key: %d, want 200", code)
	}
}

func TestGCDisabledIs503(t *testing.T) {
	ts := newTestServer(t) // no collector
	if code, _ := ts.signedDo(t, ts.client, http.MethodGet, "/v1/gc", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("gc status without collector: %d, want 503", code)
	}
}

// TestPushLeaseReleasedByRefPut pins the upload-lease lifecycle: a pack
// landing in the inbox takes a lease for its root; the reference PUT that
// concludes the upload releases it.
func TestPushLeaseReleasedByRefPut(t *testing.T) {
	ts, coll := newGCTestServer(t)
	o, err := fstree.EncodeBlob([]byte("leased push"))
	if err != nil {
		t.Fatal(err)
	}
	tmp, h, _, err := ts.inbox.Stage(inbox.Meta{Root: o.Key[:]}, bytes.NewReader(packOf(t, o)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts.inbox.Commit(tmp, h, o.Key); err != nil {
		t.Fatal(err)
	}
	if got := coll.Counters().Leases; got != 1 {
		t.Fatalf("leases after pack commit = %d, want 1", got)
	}
	rec := signedRef(t, "v", o.Key[:], ts.client)
	if code, body := ts.signedDo(t, ts.client, http.MethodPut, "/v1/refs?name=v", rec); code != http.StatusNoContent {
		t.Fatalf("put ref: %d %s", code, body)
	}
	if got := coll.Counters().Leases; got != 0 {
		t.Fatalf("leases after ref put = %d, want 0", got)
	}
}

func TestWipeEmptiesClosures(t *testing.T) {
	ts, coll := newGCTestServer(t)
	o, err := fstree.EncodeBlob([]byte("wiped"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	rec := signedRef(t, "v", o.Key[:], ts.client)
	if code, body := ts.signedDo(t, ts.client, http.MethodPut, "/v1/refs?name=v", rec); code != http.StatusNoContent {
		t.Fatalf("put ref: %d %s", code, body)
	}
	if code, body := ts.signedDo(t, ts.admin, http.MethodPost, "/v1/wipe", nil); code != http.StatusOK {
		t.Fatalf("wipe: %d %s", code, body)
	}
	ct := coll.Counters()
	if ct.Union != 0 || ct.Closures != 0 || ct.Refs != 0 {
		t.Fatalf("counters after wipe = %+v, want empty", ct)
	}
}
