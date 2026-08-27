package server_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/sshsign"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"golang.org/x/crypto/ssh"
)

// signedRef builds a signed reference record pointing at k, signed by signer.
func signedRef(t *testing.T, name string, k []byte, signer ssh.Signer) []byte {
	t.Helper()
	return signedRefAt(t, name, k, signer, time.Now().UnixNano())
}

func signedRefAt(t *testing.T, name string, k []byte, signer ssh.Signer, createdAt int64) []byte {
	t.Helper()
	rec := reference.Reference{
		Name:      name,
		Key:       k,
		User:      "tester@example.com",
		CreatedAt: createdAt,
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

func TestRefPutRequiresCompleteContent(t *testing.T) {
	ts := newTestServer(t)
	blobA, err := fstree.EncodeBlob([]byte("chunk a"))
	if err != nil {
		t.Fatal(err)
	}
	blobB, err := fstree.EncodeBlob([]byte("chunk b"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blobA.Key, blobB.Key})
	if err != nil {
		t.Fatal(err)
	}
	// the root and one chunk are present, the other chunk is missing
	for _, o := range []fstree.Object{fileNode, blobA} {
		if err := ts.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	owner := testSigner(t)
	rec := signedRef(t, "partial", fileNode.Key[:], owner)
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=partial", rec)
	if code != 404 {
		t.Fatalf("status = %d: %s, want 404 for a missing leaf", code, body)
	}
	if !strings.Contains(string(body), blobB.Key.String()) {
		t.Fatalf("404 body does not name the missing object: %s", body)
	}
	// completing the content makes the same put succeed
	if err := ts.store.Put(blobB.Key, blobB.Bytes); err != nil {
		t.Fatal(err)
	}
	if code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=partial", rec); code != 204 {
		t.Fatalf("status = %d: %s, want 204 once content is complete", code, body)
	}
}

func TestRefPutRequiresCompleteContent_MissingInteriorNode(t *testing.T) {
	ts := newTestServer(t)
	blob, err := fstree.EncodeBlob([]byte("file content"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, ContentKey: fileNode.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// the root leaf and the blob are present, the FileNode between them is not
	for _, o := range []fstree.Object{leaf, blob} {
		if err := ts.store.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	rec := signedRef(t, "torn", leaf.Key[:], testSigner(t))
	code, body := ts.signedDo(t, ts.client, "PUT", "/v1/refs?name=torn", rec)
	if code != 404 {
		t.Fatalf("status = %d: %s, want 404 for a missing interior node", code, body)
	}
	if !strings.Contains(string(body), fileNode.Key.String()) {
		t.Fatalf("404 body does not name the missing object: %s", body)
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

// Replaying an older signed record must not roll a name back. Re-pushing
// the current record is a no-op and admins keep their override.
func TestRefPutRejectsRollbackToOlderRecord(t *testing.T) {
	ts := newTestServer(t)
	blobs := storeBlobs(t, ts, "v1", "v2")
	owner := testSigner(t)
	const base = 1_700_000_000_000_000_000
	v1 := signedRefAt(t, "rel", blobs[0].Key[:], owner, base)
	v2 := signedRefAt(t, "rel", blobs[1].Key[:], owner, base+1)
	sameTimeOther := signedRefAt(t, "rel", blobs[0].Key[:], owner, base+1)

	put := func(signer ssh.Signer, rec []byte) int {
		code, _ := ts.signedDo(t, signer, "PUT", "/v1/refs?name=rel", rec)
		return code
	}
	if code := put(ts.client, v1); code != 204 {
		t.Fatalf("v1 = %d", code)
	}
	if code := put(ts.client, v2); code != 204 {
		t.Fatalf("v2 = %d", code)
	}
	if code := put(ts.client, v1); code != 409 {
		t.Fatalf("replaying v1 over v2 = %d, want 409", code)
	}
	if code := put(ts.client, sameTimeOther); code != 409 {
		t.Fatalf("different record with equal created_at = %d, want 409", code)
	}
	if code := put(ts.client, v2); code != 204 {
		t.Fatalf("re-pushing the current record = %d, want 204", code)
	}
	_, got := ts.signedDo(t, ts.client, "GET", "/v1/refs?name=rel", nil)
	if !bytes.Equal(got, v2) {
		t.Fatal("stored record is not v2")
	}
	if code := put(ts.admin, v1); code != 204 {
		t.Fatalf("admin rollback = %d, want 204", code)
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
