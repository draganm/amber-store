package remoteclient_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/inbox"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/server"
	"golang.org/x/crypto/ssh"
)

func grantTestSigner(t *testing.T) ssh.Signer {
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

func store2Record(o fstree.Object) ([]byte, error) {
	dir, err := os.MkdirTemp("", "grantrec")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	s, err := packstore.Open(dir, packstore.WithSync(false))
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := s.Put(o.Key, o.Bytes); err != nil {
		return nil, err
	}
	return s.GetRecord(o.Key)
}

// TestGrantAuthedClient runs an unlisted client against a caps-enforcing
// server purely on a delegate-minted grant: reads and object pushes succeed,
// ref writes stay forbidden, and the provider is consulted per request.
func TestGrantAuthedClient(t *testing.T) {
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

	identity, engine, runner := grantTestSigner(t), grantTestSigner(t), grantTestSigner(t)
	allow, err := allowlist.Parse([]byte(
		"read,push-objects,write-refs,delegate " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(engine.PublicKey())))))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(server.Config{
		Store: store, Inbox: ib, Refs: refs,
		Allow:    func() *allowlist.List { return allow },
		Identity: identity,
	}))
	t.Cleanup(srv.Close)

	now := time.Now()
	var calls atomic.Int64
	signedGrant, err := grant.Sign(grant.Grant{
		Subject:   runner.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead, allowlist.CapPushObjects},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, engine)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := remoteclient.New(srv.URL, runner, identity.PublicKey().Marshal(),
		remoteclient.WithGrant(func() []byte {
			calls.Add(1)
			return signedGrant
		}))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Read path (buffered request signing).
	blob, err := fstree.EncodeBlob([]byte("granted payload"))
	if err != nil {
		t.Fatal(err)
	}
	missing, err := rc.Missing(ctx, []key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Fatalf("missing = %v", missing)
	}
	// Push path (streaming request signing).
	rec, err := store2Record(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.PushPackRaw(ctx, "", blob.Key, [][]byte{rec}); err != nil {
		t.Fatal(err)
	}
	// Ref write must stay forbidden even with the grant.
	err = rc.PutRef(ctx, "x:1", []byte("junk-record"))
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != 403 {
		t.Fatalf("PutRef err = %v, want StatusError 403", err)
	}
	if calls.Load() < 3 {
		t.Fatalf("grant provider consulted %d times, want one per request (>=3)", calls.Load())
	}
}
