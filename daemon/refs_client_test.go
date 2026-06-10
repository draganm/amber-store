package daemon_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
)

func TestRefs_ClientRoundTrip(t *testing.T) {
	store := openStore(t)
	o, err := fstree.EncodeBlob([]byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	cl := serveOnSocket(t, store)
	ctx := context.Background()

	rec := reference.Reference{Name: "a/b c", Key: o.Key[:], User: "u", CreatedAt: 7}
	if err := cl.PutRef(ctx, rec); err != nil {
		t.Fatalf("PutRef: %v", err)
	}

	got, err := cl.GetRef(ctx, "a/b c")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	if got.Name != rec.Name || !bytes.Equal(got.Key, rec.Key) || got.User != "u" || got.CreatedAt != 7 {
		t.Fatalf("GetRef = %+v", got)
	}

	infos, err := cl.ListRefs(ctx)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(infos) != 1 || infos[0].Name != "a/b c" || infos[0].Key != o.Key.String() {
		t.Fatalf("ListRefs = %+v", infos)
	}

	if err := cl.DeleteRef(ctx, "a/b c"); err != nil {
		t.Fatalf("DeleteRef: %v", err)
	}
	if _, err := cl.GetRef(ctx, "a/b c"); !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("GetRef after delete = %v, want ErrRefNotFound", err)
	}
	if err := cl.DeleteRef(ctx, "a/b c"); !errors.Is(err, client.ErrRefNotFound) {
		t.Fatalf("DeleteRef absent = %v, want ErrRefNotFound", err)
	}
}
