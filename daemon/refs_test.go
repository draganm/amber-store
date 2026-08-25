package daemon_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

func openRefs(t *testing.T) *refstore.Store {
	t.Helper()
	rs, err := refstore.Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs
}

// refsServer serves the daemon handler over plain HTTP with one stored blob,
// returning the server and the blob's key bytes.
func refsServer(t *testing.T) (*httptest.Server, []byte) {
	t.Helper()
	store := openStore(t)
	o, err := fstree.EncodeBlob([]byte("blob content"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(o.Key, o.Bytes); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newHandler(t, store, nil))
	t.Cleanup(srv.Close)
	return srv, o.Key[:]
}

func refURL(base, name string) string {
	return base + "/v1/refs?name=" + url.QueryEscape(name)
}

func doReq(t *testing.T, method, u string, body []byte) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func encodeRef(t *testing.T, r reference.Reference) []byte {
	t.Helper()
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRefs_PutGetDeleteRoundTrip(t *testing.T) {
	srv, kb := refsServer(t)
	name := "backups/2026/../06" // exercises '/', '..' through the query param
	rec := reference.Reference{Name: name, Key: kb, User: "u", CreatedAt: 42}

	if resp := doReq(t, http.MethodPut, refURL(srv.URL, name), encodeRef(t, rec)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", resp.StatusCode)
	}

	resp := doReq(t, http.MethodGet, refURL(srv.URL, name), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/cbor" {
		t.Fatalf("GET content type = %q, want application/cbor", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reference.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != name || got.User != "u" || got.CreatedAt != 42 {
		t.Fatalf("GET returned %+v", got)
	}

	if resp := doReq(t, http.MethodDelete, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodGet, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodDelete, refURL(srv.URL, name), nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE absent status = %d, want 404", resp.StatusCode)
	}
}

func TestRefs_PutOverwrites(t *testing.T) {
	srv, kb := refsServer(t)
	first := reference.Reference{Name: "n", Key: kb, User: "alice", CreatedAt: 1}
	second := reference.Reference{Name: "n", Key: kb, User: "bob", CreatedAt: 2}
	if resp := doReq(t, http.MethodPut, refURL(srv.URL, "n"), encodeRef(t, first)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("setup PUT first: status = %d, want 204", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodPut, refURL(srv.URL, "n"), encodeRef(t, second)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("setup PUT second: status = %d, want 204", resp.StatusCode)
	}

	resp := doReq(t, http.MethodGet, refURL(srv.URL, "n"), nil)
	body, _ := io.ReadAll(resp.Body)
	got, err := reference.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.User != "bob" || got.CreatedAt != 2 {
		t.Fatalf("overwrite did not take: %+v", got)
	}
}

func TestRefs_PutErrors(t *testing.T) {
	srv, kb := refsServer(t)
	missingKey := make([]byte, len(kb))
	copy(missingKey, kb)
	missingKey[len(missingKey)-1] ^= 0xff // valid encoding, absent from store

	cases := []struct {
		name   string
		url    string
		body   []byte
		status int
	}{
		{"missing name param", srv.URL + "/v1/refs", encodeRef(t, reference.Reference{Name: "n", Key: kb}), 422},
		{"bad cbor", refURL(srv.URL, "n"), []byte("garbage"), 422},
		{"name mismatch", refURL(srv.URL, "other"), encodeRef(t, reference.Reference{Name: "n", Key: kb}), 422},
		{"dangling key", refURL(srv.URL, "n"), encodeRef(t, reference.Reference{Name: "n", Key: missingKey}), 404},
		{"body over cap", refURL(srv.URL, "n"), bytes.Repeat([]byte{0xff}, (1<<20)+1), 422},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doReq(t, http.MethodPut, tc.url, tc.body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}

	resp := doReq(t, http.MethodDelete, srv.URL+"/v1/refs", nil)
	if resp.StatusCode != 422 {
		t.Fatalf("DELETE without name status = %d, want 422", resp.StatusCode)
	}

	invalidNameURL := refURL(srv.URL, "a@b")
	if resp := doReq(t, http.MethodGet, invalidNameURL, nil); resp.StatusCode != 422 {
		t.Fatalf("GET invalid name status = %d, want 422", resp.StatusCode)
	}
	if resp := doReq(t, http.MethodDelete, invalidNameURL, nil); resp.StatusCode != 422 {
		t.Fatalf("DELETE invalid name status = %d, want 422", resp.StatusCode)
	}
}

func TestRefs_ListNDJSON(t *testing.T) {
	srv, kb := refsServer(t)
	for _, n := range []string{"zeta", "alpha"} {
		rec := reference.Reference{Name: n, Key: kb, User: "u", CreatedAt: 1700000000000000000, Signature: []byte{1}}
		if resp := doReq(t, http.MethodPut, refURL(srv.URL, n), encodeRef(t, rec)); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("setup PUT %q: status = %d, want 204", n, resp.StatusCode)
		}
	}
	resp := doReq(t, http.MethodGet, srv.URL+"/v1/refs", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), body)
	}
	var first struct {
		Name      string `json:"name"`
		Key       string `json:"key"`
		User      string `json:"user"`
		CreatedAt string `json:"created_at"`
		Signed    bool   `json:"signed"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Name != "alpha" { // lexicographic order
		t.Fatalf("first line name = %q, want alpha", first.Name)
	}
	if first.Key == "" || first.User != "u" || !first.Signed || first.CreatedAt == "" {
		t.Fatalf("line fields wrong: %+v", first)
	}
}
