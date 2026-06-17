package remoteclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remoteclient"
)

// trailerStrippingProxy forwards requests to backend, preserving method,
// path+query, headers and body, but copying back only the response headers and
// body — never HTTP trailers. It models the common reverse proxy / CDN that
// drops trailers, which is what breaks a trailer-signed fetch in the field.
func trailerStrippingProxy(t *testing.T, backend string) *httptest.Server {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out, err := http.NewRequestWithContext(r.Context(), r.Method, backend+r.URL.RequestURI(), bytes.NewReader(reqBody))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			if k == "Trailer" { // hide that any trailer was promised
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		// resp.Trailer is deliberately never copied.
	}))
	t.Cleanup(proxy.Close)
	return proxy
}

func TestFetchObjectsThroughTrailerStrippingProxy(t *testing.T) {
	h := newHarness(t)
	objs := blobs(t, "fp one", "fp two")
	for _, o := range objs {
		if err := h.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	proxy := trailerStrippingProxy(t, h.srv.URL)
	c, err := remoteclient.New(proxy.URL, h.client, h.identity.PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.FetchObjects(context.Background(), []key.Key{objs[0].Key, objs[1].Key})
	if err != nil {
		t.Fatalf("fetch through trailer-stripping proxy: %v", err)
	}
	byKey := map[key.Key][]byte{}
	for _, o := range got {
		byKey[o.Key] = o.Bytes
	}
	if string(byKey[objs[0].Key]) != "fp one" || string(byKey[objs[1].Key]) != "fp two" {
		t.Fatal("fetched payloads differ through proxy")
	}
}

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

func TestPushPackRawAndMissing(t *testing.T) {
	h := newHarness(t)
	c := h.rc(t)
	ctx := context.Background()
	objs := blobs(t, "raw one", "raw two")
	keys := []key.Key{objs[0].Key, objs[1].Key}

	recs := make([][]byte, len(objs))
	for i, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = rec
	}
	if err := c.PushPackRaw(ctx, "main", objs[0].Key, recs); err != nil {
		t.Fatal(err)
	}
	h.inbox.WaitFor(objs[0].Key)
	missing, err := c.Missing(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing after raw push = %d, want 0", len(missing))
	}
	// The raw-pushed objects must come back with their original payloads.
	got, err := c.FetchObjects(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[key.Key][]byte{}
	for _, o := range got {
		byKey[o.Key] = o.Bytes
	}
	if string(byKey[objs[0].Key]) != "raw one" || string(byKey[objs[1].Key]) != "raw two" {
		t.Fatal("raw-pushed payloads differ")
	}
}

func TestPushPackRawStreamsBody(t *testing.T) {
	// A streamed push sends a chunked body (no Content-Length); a buffered one
	// would set Content-Length to the pack size. Asserting the server sees an
	// unknown length (-1) pins the streaming path so a regression to a full
	// in-memory buffer is caught. The response is intentionally unsigned, so the
	// client's verification error is expected and ignored — the request is what
	// we inspect.
	var gotContentLength int64 = -999
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/objects" {
			_, _ = io.Copy(io.Discard, r.Body)
			gotContentLength = r.ContentLength
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c, err := remoteclient.New(srv.URL, testSigner(t), testSigner(t).PublicKey().Marshal())
	if err != nil {
		t.Fatal(err)
	}
	objs := blobs(t, "stream a", "stream b")
	recs := make([][]byte, len(objs))
	for i, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = rec
	}
	// The unsigned 200 fails response verification; we only care about the request.
	_ = c.PushPackRaw(context.Background(), "main", objs[0].Key, recs)
	if gotContentLength != -1 {
		t.Fatalf("request Content-Length = %d, want -1 (chunked/streamed, not buffered)", gotContentLength)
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
