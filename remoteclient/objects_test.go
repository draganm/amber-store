package remoteclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
)

func blobs(t *testing.T, contents ...string) []fstree.Object {
	t.Helper()
	var out []fstree.Object
	for _, c := range contents {
		o, err := fstree.EncodeBlob([]byte(c))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

func TestPushPackAndMissing(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "po one", "po two")
	keys := []key.Key{objs[0].Key, objs[1].Key}

	missing, err := c.Missing(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 2 {
		t.Fatalf("missing before push = %d, want 2", len(missing))
	}
	if err := c.PushPack(ctx, "main", objs[0].Key, objs); err != nil {
		t.Fatal(err)
	}
	// PushPack acks before the pack is processed; drain the inbox so the
	// objects are stored before re-checking Missing.
	h.inbox.WaitFor(objs[0].Key)
	missing, err = c.Missing(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing after push = %d, want 0", len(missing))
	}
}

func TestFetchObjects(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "fo one", "fo two")
	if err := c.PushPack(ctx, "main", objs[0].Key, objs); err != nil {
		t.Fatal(err)
	}
	// Drain the staged pack so the objects exist before fetching them.
	h.inbox.WaitFor(objs[0].Key)
	got, err := c.FetchObjects(ctx, []key.Key{objs[0].Key, objs[1].Key})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("fetched %d objects, want 2", len(got))
	}
	byKey := map[key.Key][]byte{}
	for _, o := range got {
		byKey[o.Key] = o.Bytes
	}
	if string(byKey[objs[0].Key]) != "fo one" || string(byKey[objs[1].Key]) != "fo two" {
		t.Fatal("fetched payloads differ")
	}
}

func TestFetchObjectsAbsentIsStatusError(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	absent := blobs(t, "absent")[0]
	_, err := c.FetchObjects(context.Background(), []key.Key{absent.Key})
	var se *remoteclient.StatusError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("err = %v, want StatusError 404", err)
	}
}

func TestFetchObjectsWrongPinFails(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "pin check")
	if err := c.PushPack(ctx, "main", objs[0].Key, objs); err != nil {
		t.Fatal(err)
	}
	h.inbox.WaitFor(objs[0].Key)
	wrong, err := remoteclient.New(h.srv.URL, h.client, testSigner(t).PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.FetchObjects(ctx, []key.Key{objs[0].Key}); err == nil {
		t.Fatal("fetch with wrong pinned key succeeded")
	}
}
