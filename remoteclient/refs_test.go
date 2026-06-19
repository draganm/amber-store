package remoteclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/sshsign"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/remoteclient"
	"golang.org/x/crypto/ssh"
)

func signedRecord(t *testing.T, name string, k []byte, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name: name, Key: k, User: "u@example.com",
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
	return b
}

func TestRefRoundTripAndListing(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	obj := blobs(t, "ref target")[0]
	// PUT /v1/refs below waits on the inbox internally, so no explicit
	// WaitFor is needed between this push and setting the reference.
	if err := c.PushPack(ctx, "main", obj.Key, []fstree.Object{obj}); err != nil {
		t.Fatal(err)
	}
	rec := signedRecord(t, "main", obj.Key[:], testSigner(t))
	if err := c.PutRef(ctx, "main", rec); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetRef(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(rec) {
		t.Fatal("round-tripped record differs")
	}
	infos, err := c.ListRefs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Name != "main" || !infos[0].Signed {
		t.Fatalf("listing = %+v", infos)
	}
}

func TestGetRefAbsent(t *testing.T) {
	h := newHarness(t)
	_, err := h.rc(t).GetRef(context.Background(), "nope")
	if !errors.Is(err, remoteclient.ErrRefNotFound) {
		t.Fatalf("err = %v, want ErrRefNotFound", err)
	}
}
