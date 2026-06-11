package admin_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/draganm/amber-store/admin"
	"github.com/draganm/amber-store/internal/allowstore"
	"golang.org/x/crypto/ssh"
)

const password = "open sesame"

func testKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))
}

var testUI = fstest.MapFS{
	"index.html":    {Data: []byte("<html>amber admin</html>")},
	"assets/app.js": {Data: []byte("console.log('ui')")},
}

// testServer returns a started admin server and the allowed-keys store
// behind it, seeded with one key.
func testServer(t *testing.T) (*httptest.Server, *allowstore.Store) {
	t.Helper()
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := keys.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	_, line := testKey(t)
	if err := keys.Add(line, false); err != nil {
		t.Fatal(err)
	}
	h, err := admin.New(admin.Config{Password: password, Keys: keys, UI: testUI})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, keys
}

// login authenticates and returns the session cookie.
func login(t *testing.T, srv *httptest.Server) *http.Cookie {
	t.Helper()
	resp, err := http.Post(srv.URL+"/admin/api/login", "application/json",
		strings.NewReader(`{"password":"`+password+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login status = %d, want 204", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == admin.SessionCookie {
			if !c.HttpOnly {
				t.Fatal("session cookie is not HttpOnly")
			}
			return c
		}
	}
	t.Fatal("no session cookie set")
	return nil
}

func do(t *testing.T, method, url string, cookie *http.Cookie, body string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestNewRequiresPassword(t *testing.T) {
	if _, err := admin.New(admin.Config{UI: testUI}); err == nil {
		t.Fatal("want error for empty password")
	}
}

func TestSessionLifecycle(t *testing.T) {
	srv, _ := testServer(t)

	// no cookie: not logged in
	resp := do(t, "GET", srv.URL+"/admin/api/session", nil, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session without cookie = %d, want 401", resp.StatusCode)
	}

	// wrong password: 401, no cookie
	resp = do(t, "POST", srv.URL+"/admin/api/login", nil, `{"password":"nope"}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-password login = %d, want 401", resp.StatusCode)
	}
	if len(resp.Cookies()) != 0 {
		t.Fatal("wrong password still set a cookie")
	}

	cookie := login(t, srv)
	resp = do(t, "GET", srv.URL+"/admin/api/session", cookie, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("session with cookie = %d, want 204", resp.StatusCode)
	}

	resp = do(t, "POST", srv.URL+"/admin/api/logout", cookie, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout = %d, want 204", resp.StatusCode)
	}
	resp = do(t, "GET", srv.URL+"/admin/api/session", cookie, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session after logout = %d, want 401", resp.StatusCode)
	}
}

func TestKeysRequireSession(t *testing.T) {
	srv, _ := testServer(t)
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/admin/api/keys", ""},
		{"POST", "/admin/api/keys", `{"line":"x"}`},
		{"DELETE", "/admin/api/keys?fingerprint=SHA256:x", ""},
	} {
		resp := do(t, c.method, srv.URL+c.path, nil, c.body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s %s without session = %d, want 401", c.method, c.path, resp.StatusCode)
		}
	}
}

func keysOf(t *testing.T, resp *http.Response) []allowstore.Key {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list keys = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Keys []allowstore.Key `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Keys
}

func TestKeysCRUD(t *testing.T) {
	srv, keys := testServer(t)
	cookie := login(t, srv)

	listed := keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 1 {
		t.Fatalf("initial list has %d keys, want 1", len(listed))
	}

	pub, line := testKey(t)
	resp := do(t, "POST", srv.URL+"/admin/api/keys", cookie,
		`{"line":"`+line+` web-added","admin":true}`)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("add key = %d, want 204 (%s)", resp.StatusCode, b)
	}
	if e, ok := keys.Current().Lookup(pub.Marshal()); !ok || !e.Admin {
		t.Fatalf("added key live lookup: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}

	listed = keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 2 {
		t.Fatalf("list after add has %d keys, want 2", len(listed))
	}
	// List orders by wire-key bytes, so find the new key by comment.
	var added *allowstore.Key
	for i := range listed {
		if listed[i].Comment == "web-added" {
			added = &listed[i]
		}
	}
	if added == nil {
		t.Fatalf("added key not listed: %+v", listed)
	}
	if !added.Admin {
		t.Fatalf("added key listed wrong: %+v", added)
	}

	resp = do(t, "DELETE",
		srv.URL+"/admin/api/keys?fingerprint="+strings.ReplaceAll(added.Fingerprint, "+", "%2B"),
		cookie, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete key = %d, want 204", resp.StatusCode)
	}
	if _, ok := keys.Current().Lookup(pub.Marshal()); ok {
		t.Fatal("deleted key still in live list")
	}
	listed = keysOf(t, do(t, "GET", srv.URL+"/admin/api/keys", cookie, ""))
	if len(listed) != 1 {
		t.Fatalf("list after delete has %d keys, want 1", len(listed))
	}
}

func TestAddInvalidKey(t *testing.T) {
	srv, _ := testServer(t)
	cookie := login(t, srv)
	resp := do(t, "POST", srv.URL+"/admin/api/keys", cookie, `{"line":"not a key"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("add garbage = %d, want 400", resp.StatusCode)
	}
	var e struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Error == "" {
		t.Fatalf("error body missing: err=%v body=%+v", err, e)
	}
}

func TestDeleteUnknownKey(t *testing.T) {
	srv, _ := testServer(t)
	cookie := login(t, srv)
	resp := do(t, "DELETE", srv.URL+"/admin/api/keys?fingerprint=SHA256:absent", cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete unknown = %d, want 404", resp.StatusCode)
	}
}

func TestCrossOriginMutationRejected(t *testing.T) {
	srv, _ := testServer(t)
	req, err := http.NewRequest("POST", srv.URL+"/admin/api/login",
		strings.NewReader(`{"password":"`+password+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin login = %d, want 403", resp.StatusCode)
	}
}

func TestServesSPA(t *testing.T) {
	srv, _ := testServer(t)
	for path, want := range map[string]string{
		"/admin/":              "amber admin",
		"/admin":               "amber admin", // redirect
		"/admin/assets/app.js": "console.log",
		"/admin/deep/route":    "amber admin", // SPA fallback
	} {
		resp := do(t, "GET", srv.URL+path, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
		}
		b, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(b), want) {
			t.Fatalf("GET %s body %q does not contain %q", path, b, want)
		}
	}
}
