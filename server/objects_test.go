package server_test

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
)

// storeBlobs writes blobs into ts.store and returns their objects.
func storeBlobs(t *testing.T, ts *testServer, contents ...string) []fstree.Object {
	t.Helper()
	var out []fstree.Object
	for _, c := range contents {
		o, err := fstree.EncodeBlob([]byte(c))
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

func TestMissingReturnsAbsentSubset(t *testing.T) {
	ts := newTestServer(t)
	present := storeBlobs(t, ts, "present one", "present two")
	absent, err := fstree.EncodeBlob([]byte("absent"))
	if err != nil {
		t.Fatal(err)
	}
	req := keylist.Flatten([]key.Key{present[0].Key, absent.Key, present[1].Key})
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", req)
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	got, err := keylist.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != absent.Key {
		t.Fatalf("missing = %v, want [%s]", got, absent.Key)
	}
}

func TestMissingRejectsBadList(t *testing.T) {
	ts := newTestServer(t)
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects/missing", make([]byte, 33)); code != 422 {
		t.Fatalf("status = %d, want 422", code)
	}
}
