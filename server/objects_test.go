package server_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/httpsig"
	"github.com/draganm/amber-store/keylist"
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

// packOf serializes objects as one amberpack stream.
func packOf(t *testing.T, objs ...fstree.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestObjectUploadStoresAndDedupes(t *testing.T) {
	ts := newTestServer(t)
	o1, err := fstree.EncodeBlob([]byte("upload one"))
	if err != nil {
		t.Fatal(err)
	}
	o2, err := fstree.EncodeBlob([]byte("upload two"))
	if err != nil {
		t.Fatal(err)
	}
	// Pushes are acked on durable staging and processed asynchronously; the
	// upload is tagged with one of its object keys as the root and verified by
	// waiting on the inbox barrier, then checking the store.
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects?root="+o1.Key.String(), packOf(t, o1, o2))
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	ts.inbox.WaitFor(o1.Key)
	if has, _ := ts.store.Has(o1.Key); !has {
		t.Fatal("o1 not in store after upload")
	}
	if has, _ := ts.store.Has(o2.Key); !has {
		t.Fatal("o2 not in store after upload")
	}
	// re-upload of an already-stored object is accepted and is a no-op (dedupe)
	code, body = ts.signedDo(t, ts.client, "POST", "/v1/objects?root="+o1.Key.String(), packOf(t, o1))
	if code != 200 {
		t.Fatalf("re-upload status = %d: %s", code, body)
	}
	ts.inbox.WaitFor(o1.Key)
	if has, _ := ts.store.Has(o1.Key); !has {
		t.Fatal("o1 not in store after re-upload")
	}
}

func TestObjectUploadRejectsHashMismatch(t *testing.T) {
	ts := newTestServer(t)
	good, err := fstree.EncodeBlob([]byte("good payload"))
	if err != nil {
		t.Fatal(err)
	}
	evil := fstree.Object{Key: good.Key, Bytes: []byte("evil payload")}
	// The push is acked on staging; processing verifies each object's payload
	// against its key, so the mismatching pack is quarantined and never stored.
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects?root="+good.Key.String(), packOf(t, evil))
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	ts.inbox.WaitFor(good.Key)
	if has, _ := ts.store.Has(good.Key); has {
		t.Fatal("mismatching object was stored")
	}
}

func TestObjectUploadRejectsMalformedStream(t *testing.T) {
	ts := newTestServer(t)
	// A real root key whose pack body is garbage: the push is acked on staging,
	// then processing fails to decode it and quarantines it, storing nothing.
	root, err := fstree.EncodeBlob([]byte("malformed-root"))
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects?root="+root.Key.String(), []byte("garbage"))
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	ts.inbox.WaitFor(root.Key)
	if has, _ := ts.store.Has(root.Key); has {
		t.Fatal("object from malformed stream was stored")
	}
}

func TestObjectsGetReturnsInBandSignedPack(t *testing.T) {
	ts := newTestServer(t)
	objs := storeBlobs(t, ts, "stream one", "stream two", "stream three")
	want := []key.Key{objs[0].Key, objs[2].Key}
	body := keylist.Flatten(want)

	req, err := http.NewRequest("POST", ts.srv.URL+"/v1/objects/get", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("nonce-16-bytes!!")
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), nonce, body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// The signature is appended in-band, not in a header or trailer, so it
	// survives proxies that strip trailers while the server still streams.
	if resp.Header.Get(httpsig.HeaderSignature) != "" || resp.Trailer.Get(httpsig.HeaderSignature) != "" {
		t.Fatal("fetch success must carry the signature in-band, not in a header/trailer")
	}
	pack, sig, ok := httpsig.SplitSignatureTrailer(respBody)
	if !ok {
		t.Fatal("response has no in-band signature trailer")
	}
	// The in-band signature covers the exact pack bytes.
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nonce, 200,
		httpsig.HashBody(pack), sig); err != nil {
		t.Fatalf("in-band signature: %v", err)
	}
	// the pack is an amberpack of exactly the requested objects
	got := map[key.Key][]byte{}
	for o, err := range amberpack.NewReader(bytes.NewReader(pack)).All() {
		if err != nil {
			t.Fatal(err)
		}
		got[o.Key] = o.Bytes
	}
	if len(got) != 2 || string(got[objs[0].Key]) != "stream one" || string(got[objs[2].Key]) != "stream three" {
		t.Fatalf("got objects %v", got)
	}
}

func TestPostObjectsStagesAndStores(t *testing.T) {
	ts := newTestServer(t)

	obj, err := fstree.EncodeBlob([]byte("pushed via async inbox"))
	if err != nil {
		t.Fatal(err)
	}
	root := obj.Key
	body := packOf(t, obj)

	status, _ := ts.signedDo(t, ts.client, http.MethodPost, "/v1/objects?ref=site&root="+root.String(), body)
	if status != http.StatusOK {
		t.Fatalf("push status = %d, want 200", status)
	}

	ts.inbox.WaitFor(root)
	has, err := ts.store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("object not stored after WaitFor: has=%v err=%v", has, err)
	}
}

func TestPostObjectsRejectsUnlistedKey(t *testing.T) {
	ts := newTestServer(t)
	stranger := testSigner(t) // unlisted key, same as TestAuthRejections
	o, err := fstree.EncodeBlob([]byte("from an unlisted key"))
	if err != nil {
		t.Fatal(err)
	}
	code, _ := ts.signedDo(t, stranger, "POST", "/v1/objects?root="+o.Key.String(), packOf(t, o))
	if code != 403 {
		t.Fatalf("unlisted key push = %d, want 403", code)
	}
	// And it must not have been staged/stored.
	ts.inbox.WaitFor(o.Key)
	if has, _ := ts.store.Has(o.Key); has {
		t.Fatalf("object from an unlisted key must not be stored")
	}
}

func TestPostObjectsReplayedNonceRejected(t *testing.T) {
	ts := newTestServer(t)
	o, err := fstree.EncodeBlob([]byte("replayed pack push"))
	if err != nil {
		t.Fatal(err)
	}
	body := packOf(t, o)
	req, err := http.NewRequest("POST", ts.srv.URL+"/v1/objects?root="+o.Key.String(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := httpsig.SignRequest(req, ts.client, time.Now().UnixNano(), []byte("fixed-nonce-0123"), body); err != nil {
		t.Fatal(err)
	}
	send := func() int {
		r2 := req.Clone(req.Context())
		r2.Body = io.NopCloser(bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := send(); code != 200 {
		t.Fatalf("first send = %d, want 200", code)
	}
	if code := send(); code != 401 {
		t.Fatalf("replay = %d, want 401", code)
	}
}

func TestPushThenSetRefWaitsForProcessing(t *testing.T) {
	ts := newTestServer(t)

	blob, err := fstree.EncodeBlob([]byte("root blob for barrier test"))
	if err != nil {
		t.Fatal(err)
	}
	root := blob.Key

	// Async push: returns 200 once durably staged, BEFORE processing.
	if code, _ := ts.signedDo(t, ts.client, "POST",
		"/v1/objects?ref=site&root="+root.String(), packOf(t, blob)); code != 200 {
		t.Fatalf("push status = %d, want 200", code)
	}

	// Immediately set the ref — no WaitFor here. putRef's internal barrier must
	// wait for the pushed pack to be processed before CheckComplete, so this is
	// deterministically 204, never a race-induced 404.
	if code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=site",
		signedRef(t, "site", root[:], ts.client)); code != 204 {
		t.Fatalf("set ref status = %d, want 204; body=%s", code, body)
	}
}

func TestObjectsGetAbsentKeyIs404BeforeStreaming(t *testing.T) {
	ts := newTestServer(t)
	absent, err := fstree.EncodeBlob([]byte("never stored"))
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects/get", keylist.Flatten([]key.Key{absent.Key}))
	if code != 404 {
		t.Fatalf("status = %d, want 404", code)
	}
	if !strings.Contains(string(body), absent.Key.String()) {
		t.Fatalf("404 body does not name the missing key: %s", body)
	}
}
