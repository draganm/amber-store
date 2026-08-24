package embedded_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/embedded"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/remotes"
	"github.com/draganm/amber-store/remotesync"
	"github.com/draganm/amber-store/server"
	"github.com/draganm/amber-store/sshsign"
	"golang.org/x/crypto/ssh"
)

func embSigner(t *testing.T) ssh.Signer {
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

// startCentral runs a caps-enforcing server; returns its URL and identity key.
func startCentral(t *testing.T, allowLines string) (string, ssh.Signer) {
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
	identity := embSigner(t)
	allow, err := allowlist.Parse([]byte(allowLines))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)
	return srv.URL, identity
}

// buildTree writes a two-blob tree into st and returns its root key.
func buildTree(t *testing.T, st *embedded.Store, payload string) key.Key {
	t.Helper()
	b1, err := fstree.EncodeBlob([]byte(payload + "-1"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fstree.EncodeBlob([]byte(payload + "-2"))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("f"), Mode: 0o100644, ContentKey: fn.Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{b1, b2, fn, leaf} {
		if err := st.Objects.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return leaf.Key
}

func signRecord(t *testing.T, signer ssh.Signer, name string, root key.Key) []byte {
	t.Helper()
	rec := reference.Reference{
		Name: name, Key: root[:], User: "engine", CreatedAt: time.Now().UnixNano(),
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
	raw, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRunnerEnginePullRoundTrip is the spec's facade round-trip: a runner
// store with only a grant pushes a tree, the delegate engine publishes the
// pre-signed ref WITHOUT holding the objects, and a fresh store pulls it all
// back by ref name.
func TestRunnerEnginePullRoundTrip(t *testing.T) {
	engineKey := embSigner(t)
	url, central := startCentral(t,
		"read,push-objects,write-refs,delegate "+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engineKey.PublicKey()))))
	ctx := context.Background()
	centralWire := central.PublicKey().Marshal()

	// Runner: auto identity + grant, no allowlist entry.
	var signedGrant []byte
	runner, err := embedded.Open(t.TempDir(), embedded.Config{Grant: func() []byte { return signedGrant }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runner.Close() })
	now := time.Now()
	signedGrant, err = grant.Sign(grant.Grant{
		Subject:   runner.Identity.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead, allowlist.CapPushObjects},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, engineKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: centralWire}); err != nil {
		t.Fatal(err)
	}

	// Engine: allowlisted transport signer, no grant.
	engine, err := embedded.Open(t.TempDir(), embedded.Config{Signer: engineKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	if err := engine.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: centralWire}); err != nil {
		t.Fatal(err)
	}

	// 1. Runner pushes the tree (objects only).
	root := buildTree(t, runner, "artifact")
	if _, err := runner.PushTree(ctx, "central", root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}

	// 2. Engine publishes the pre-signed ref without holding any objects.
	raw := signRecord(t, engineKey, "build-output:test", root)
	if err := engine.PublishRef(ctx, "central", raw); err != nil {
		t.Fatal(err)
	}

	// 3. Engine sees it in the remote listing.
	infos, err := engine.ListRemoteRefs(ctx, "central")
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Name != "build-output:test" {
		t.Fatalf("listing = %+v", infos)
	}

	// 4. A fresh consumer store pulls by ref name and holds the tree.
	consumer, err := embedded.Open(t.TempDir(), embedded.Config{Signer: engineKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { consumer.Close() })
	if err := consumer.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: centralWire}); err != nil {
		t.Fatal(err)
	}
	gotRoot, _, err := consumer.Pull(ctx, "central", "build-output:test", remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if gotRoot != root {
		t.Fatalf("pulled root %s, want %s", gotRoot, root)
	}
	has, err := consumer.Objects.Has(root)
	if err != nil || !has {
		t.Fatalf("root object not local after pull: has=%v err=%v", has, err)
	}
	// The pull wrote the reference through the collector: one closure on
	// disk, its tails in the union.
	ct := consumer.GC.Counters()
	if ct.Refs != 1 || ct.Closures != 1 || ct.Union == 0 {
		t.Fatalf("collector counters after pull = %+v, want 1 ref, 1 closure, a nonempty union", ct)
	}
}

// TestGrantCannotPushRefs: the runner's facade Push (which ends in PutRef)
// must fail at the ref step even though its object push succeeds.
func TestGrantCannotPushRefs(t *testing.T) {
	engineKey := embSigner(t)
	url, central := startCentral(t,
		"read,push-objects,write-refs,delegate "+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engineKey.PublicKey()))))
	ctx := context.Background()

	var signedGrant []byte
	runner, err := embedded.Open(t.TempDir(), embedded.Config{Grant: func() []byte { return signedGrant }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runner.Close() })
	now := time.Now()
	signedGrant, err = grant.Sign(grant.Grant{
		Subject:   runner.Identity.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead, allowlist.CapPushObjects},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, engineKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: central.PublicKey().Marshal()}); err != nil {
		t.Fatal(err)
	}

	root := buildTree(t, runner, "sneaky")
	raw := signRecord(t, runner.Identity, "sneaky:1", root)
	if err := runner.Refs.Put("sneaky:1", raw); err != nil {
		t.Fatal(err)
	}
	_, err = runner.Push(ctx, "central", "sneaky:1", remotesync.Opts{})
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != 403 {
		t.Fatalf("Push err = %v, want StatusError 403 at the ref step", err)
	}
}

// TestRemoteClientCaching: RemoteClient returns the same *remoteclient.Client
// for repeated calls against an unchanged remote, and a fresh client once the
// registry entry (here, the pinned server key) changes underneath it.
func TestRemoteClientCaching(t *testing.T) {
	st, err := embedded.Open(t.TempDir(), embedded.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	key1 := embSigner(t).PublicKey().Marshal()
	if err := st.Remotes.Add("central", remotes.Remote{URL: "http://example.invalid", ServerKey: key1}); err != nil {
		t.Fatal(err)
	}

	c1, err := st.RemoteClient("central")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := st.RemoteClient("central")
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatalf("RemoteClient rebuilt the client for an unchanged remote")
	}

	key2 := embSigner(t).PublicKey().Marshal()
	if err := st.Remotes.Remove("central"); err != nil {
		t.Fatal(err)
	}
	if err := st.Remotes.Add("central", remotes.Remote{URL: "http://example.invalid", ServerKey: key2}); err != nil {
		t.Fatal(err)
	}
	c3, err := st.RemoteClient("central")
	if err != nil {
		t.Fatal(err)
	}
	if c3 == c1 {
		t.Fatalf("RemoteClient reused a stale client after the remote's server key changed")
	}
}

// TestPublishRefDanglingIs404: publishing a ref whose objects were never
// pushed must fail with the server's completeness 404.
func TestPublishRefDanglingIs404(t *testing.T) {
	engineKey := embSigner(t)
	url, central := startCentral(t,
		"read,push-objects,write-refs,delegate "+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engineKey.PublicKey()))))
	engine, err := embedded.Open(t.TempDir(), embedded.Config{Signer: engineKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	if err := engine.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: central.PublicKey().Marshal()}); err != nil {
		t.Fatal(err)
	}
	blob, err := fstree.EncodeBlob([]byte("never pushed"))
	if err != nil {
		t.Fatal(err)
	}
	raw := signRecord(t, engineKey, "dangling:1", blob.Key)
	err = engine.PublishRef(context.Background(), "central", raw)
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("err = %v, want StatusError 404", err)
	}
}

// TestRemoteWipe: an engine store whose key carries the wipe capability
// factory-resets central through the facade; a key without it is refused.
func TestRemoteWipe(t *testing.T) {
	engineKey := embSigner(t)
	url, central := startCentral(t,
		"read,push-objects,write-refs,delegate,wipe "+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engineKey.PublicKey()))))
	ctx := context.Background()
	centralWire := central.PublicKey().Marshal()

	engine, err := embedded.Open(t.TempDir(), embedded.Config{Signer: engineKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	if err := engine.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: centralWire}); err != nil {
		t.Fatal(err)
	}

	root := buildTree(t, engine, "artifact")
	if _, err := engine.PushTree(ctx, "central", root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	if err := engine.PublishRef(ctx, "central", signRecord(t, engineKey, "build-output:wipe-me", root)); err != nil {
		t.Fatal(err)
	}

	if err := engine.RemoteWipe(ctx, "central"); err != nil {
		t.Fatal(err)
	}
	infos, err := engine.ListRemoteRefs(ctx, "central")
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 0 {
		t.Fatalf("refs survived the remote wipe: %+v", infos)
	}
	// Central keeps serving: push+publish works again after the wipe.
	if _, err := engine.PushTree(ctx, "central", root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	if err := engine.PublishRef(ctx, "central", signRecord(t, engineKey, "build-output:again", root)); err != nil {
		t.Fatal(err)
	}
}

// TestRemoteWipeWithoutCapability: a delegate-but-not-wipe key is refused.
func TestRemoteWipeWithoutCapability(t *testing.T) {
	engineKey := embSigner(t)
	url, central := startCentral(t,
		"read,push-objects,write-refs,delegate "+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engineKey.PublicKey()))))
	engine, err := embedded.Open(t.TempDir(), embedded.Config{Signer: engineKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	if err := engine.Remotes.Add("central", remotes.Remote{URL: url, ServerKey: central.PublicKey().Marshal()}); err != nil {
		t.Fatal(err)
	}
	if err := engine.RemoteWipe(context.Background(), "central"); err == nil {
		t.Fatal("RemoteWipe succeeded without the wipe capability")
	}
}
