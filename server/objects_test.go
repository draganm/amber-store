package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/httpsig"
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
	code, body := ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, o1, o2))
	if code != 200 {
		t.Fatalf("status = %d: %s", code, body)
	}
	var stats struct {
		Stored      int   `json:"objects_stored"`
		Deduped     int   `json:"objects_deduped"`
		BytesStored int64 `json:"bytes_stored"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 2 {
		t.Fatalf("stored = %d, want 2", stats.Stored)
	}
	if stats.BytesStored != int64(len(o1.Bytes)+len(o2.Bytes)) {
		t.Fatalf("bytes_stored = %d, want %d", stats.BytesStored, len(o1.Bytes)+len(o2.Bytes))
	}
	if has, _ := ts.store.Has(o1.Key); !has {
		t.Fatal("o1 not in store after upload")
	}
	// re-upload dedupes
	code, body = ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, o1))
	if code != 200 {
		t.Fatalf("re-upload status = %d", code)
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 0 || stats.Deduped != 1 {
		t.Fatalf("re-upload stored/deduped = %d/%d, want 0/1", stats.Stored, stats.Deduped)
	}
}

func TestObjectUploadRejectsHashMismatch(t *testing.T) {
	ts := newTestServer(t)
	good, err := fstree.EncodeBlob([]byte("good payload"))
	if err != nil {
		t.Fatal(err)
	}
	evil := fstree.Object{Key: good.Key, Bytes: []byte("evil payload")}
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects", packOf(t, evil)); code != 422 {
		t.Fatalf("status = %d, want 422", code)
	}
	if has, _ := ts.store.Has(good.Key); has {
		t.Fatal("mismatching object was stored")
	}
}

func TestObjectUploadRejectsMalformedStream(t *testing.T) {
	ts := newTestServer(t)
	if code, _ := ts.signedDo(t, ts.client, "POST", "/v1/objects", []byte("garbage")); code != 422 {
		t.Fatalf("status = %d, want 422", code)
	}
}

func TestObjectsGetStreamsPackWithSignedTrailer(t *testing.T) {
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
	// trailer signature covers the exact body bytes
	sig := resp.Trailer.Get(httpsig.HeaderSignature)
	if err := httpsig.VerifyResponse(ts.identity.PublicKey().Marshal(), nonce, 200,
		httpsig.HashBody(respBody), sig); err != nil {
		t.Fatalf("trailer signature: %v", err)
	}
	// the body is an amberpack of exactly the requested objects
	got := map[key.Key][]byte{}
	for o, err := range amberpack.NewReader(bytes.NewReader(respBody)).All() {
		if err != nil {
			t.Fatal(err)
		}
		got[o.Key] = o.Bytes
	}
	if len(got) != 2 || string(got[objs[0].Key]) != "stream one" || string(got[objs[2].Key]) != "stream three" {
		t.Fatalf("got objects %v", got)
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
