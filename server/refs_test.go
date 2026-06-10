package server_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/reference"
	"golang.org/x/crypto/ssh"
)

// signedRef builds a signed reference record pointing at k, signed by signer.
func signedRef(t *testing.T, name string, k []byte, signer ssh.Signer) []byte {
	t.Helper()
	rec := reference.Reference{
		Name:      name,
		Key:       k,
		User:      "tester@example.com",
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

func TestRefPutGetRoundTrip(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "ref target")[0]
	owner := testSigner(t)
	rec := signedRef(t, "backups/main", target.Key[:], owner)
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=backups%2Fmain", rec)
	if code != 204 {
		t.Fatalf("put status = %d: %s", code, body)
	}
	code, got := ts.signedDo(t, ts.client, "GET", "/v1/refs?name=backups%2Fmain", nil)
	if code != 200 {
		t.Fatalf("get status = %d", code)
	}
	if string(got) != string(rec) {
		t.Fatal("stored record differs from uploaded record")
	}
}

func TestRefPutRejectsUnsigned(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "unsigned target")[0]
	rec, err := (reference.Reference{
		Name: "unsigned", Key: target.Key[:], User: "u", CreatedAt: time.Now().UnixNano(),
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=unsigned", rec)
	if code != 422 || !strings.Contains(string(body), "signed") {
		t.Fatalf("status = %d body = %s, want 422 mentioning signing", code, body)
	}
}

func TestRefPutRequiresPointedToKey(t *testing.T) {
	ts := newTestServer(t)
	absent, err := fstree.EncodeBlob([]byte("not uploaded"))
	if err != nil {
		t.Fatal(err)
	}
	rec := signedRef(t, "dangling", absent.Key[:], testSigner(t))
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=dangling", rec); code != 404 {
		t.Fatalf("status = %d, want 404", code)
	}
}

func TestRefOwnership(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "owned target")[0]
	owner, intruder := testSigner(t), testSigner(t)
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], owner)); code != 204 {
		t.Fatal("initial put failed")
	}
	// a different signer cannot overwrite, even over an allowed transport key
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], intruder)); code != 403 {
		t.Fatal("intruder overwrite was not rejected with 403")
	}
	// the same signer can update from any transport key
	if code, _ := ts.signedDo(t, ts.admin, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], owner)); code != 204 {
		t.Fatal("owner update over another transport key failed")
	}
	// an admin transport key bypasses ownership
	if code, _ := ts.signedDo(t, ts.admin, "PUT", "/v1/refs?name=owned", signedRef(t, "owned", target.Key[:], intruder)); code != 204 {
		t.Fatal("admin override failed")
	}
}

func TestRefDeleteIsAdminOnly(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "delete target")[0]
	if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=doomed", signedRef(t, "doomed", target.Key[:], testSigner(t))); code != 204 {
		t.Fatal("put failed")
	}
	if code, _ := ts.signedDo(t, ts.client, "DELETE", "/v1/refs?name=doomed", nil); code != 403 {
		t.Fatal("non-admin delete was not rejected")
	}
	if code, _ := ts.signedDo(t, ts.admin, "DELETE", "/v1/refs?name=doomed", nil); code != 204 {
		t.Fatal("admin delete failed")
	}
	if code, _ := ts.signedDo(t, ts.client, "GET", "/v1/refs?name=doomed", nil); code != 404 {
		t.Fatal("deleted ref still present")
	}
}

func TestRefListing(t *testing.T) {
	ts := newTestServer(t)
	target := storeBlobs(t, ts, "list target")[0]
	for _, n := range []string{"b-ref", "a-ref"} {
		if code, _ := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name="+n, signedRef(t, n, target.Key[:], testSigner(t))); code != 204 {
			t.Fatalf("put %s failed", n)
		}
	}
	code, body := ts.signedDo(t, ts.client, "GET", "/v1/refs", nil)
	if code != 200 {
		t.Fatalf("list status = %d", code)
	}
	var names []string
	dec := json.NewDecoder(strings.NewReader(string(body)))
	for dec.More() {
		var line struct {
			Name   string `json:"name"`
			Signed bool   `json:"signed"`
		}
		if err := dec.Decode(&line); err != nil {
			t.Fatal(err)
		}
		if !line.Signed {
			t.Fatalf("ref %s listed as unsigned", line.Name)
		}
		names = append(names, line.Name)
	}
	if len(names) != 2 || names[0] != "a-ref" || names[1] != "b-ref" {
		t.Fatalf("listing order = %v", names)
	}
}
