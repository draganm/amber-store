package admin_test

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/draganm/amber-store/admin"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/allowstore"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// memObjects is an in-memory admin.ObjectGetter.
type memObjects map[key.Key][]byte

func (m memObjects) Get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("object %s: %w", k, packstore.ErrNotFound)
	}
	return b, nil
}

func (m memObjects) put(o fstree.Object) { m[o.Key] = o.Bytes }

// memRefs is an in-memory admin.RefStore.
type memRefs map[string][]byte

func (m memRefs) Get(name string) ([]byte, error) {
	b, ok := m[name]
	if !ok {
		return nil, refstore.ErrNotFound
	}
	return b, nil
}

func (m memRefs) All() ([]refstore.Record, error) {
	recs := make([]refstore.Record, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		recs = append(recs, refstore.Record{Name: name, Data: m[name]})
	}
	return recs, nil
}

func mustObj(t *testing.T, o fstree.Object, err error) fstree.Object {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// mustRef encodes a reference record pointing name at k.
func mustRef(t *testing.T, name string, k key.Key, user string) []byte {
	t.Helper()
	b, err := reference.Reference{Name: name, Key: k[:], User: user, CreatedAt: 12345}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// seedTree stores this fixture tree in objs and returns its root directory
// key and the key of hello.txt's content:
//
//	hello.txt        "hello, amber"
//	sub/link         -> ../hello.txt
//	sub/nested.txt   "nested"
func seedTree(t *testing.T, objs memObjects) (root, hello key.Key) {
	t.Helper()
	helloBlobObj, helloBlobErr := fstree.EncodeBlob([]byte("hello, amber"))
	helloBlob := mustObj(t, helloBlobObj, helloBlobErr)
	objs.put(helloBlob)
	nestedObj, nestedErr := fstree.EncodeBlob([]byte("nested"))
	nested := mustObj(t, nestedObj, nestedErr)
	objs.put(nested)
	subLeafObj, subLeafErr := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("link"), Mode: 0o120777, Mtime: 3, LinkTarget: []byte("../hello.txt")},
		{Name: []byte("nested.txt"), Mode: 0o100644, Mtime: 2, ContentKey: nested.Key[:]},
	})
	subLeaf := mustObj(t, subLeafObj, subLeafErr)
	objs.put(subLeaf)
	rootLeafObj, rootLeafErr := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("hello.txt"), Mode: 0o100644, Mtime: 1, ContentKey: helloBlob.Key[:]},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: 4, ContentKey: subLeaf.Key[:]},
	})
	rootLeaf := mustObj(t, rootLeafObj, rootLeafErr)
	objs.put(rootLeaf)
	return rootLeaf.Key, helloBlob.Key
}

// browseServer starts an admin server whose browse endpoints see objs and
// refs, returning it with a logged-in session cookie.
func browseServer(t *testing.T, objs memObjects, refs memRefs) (*httptest.Server, *http.Cookie) {
	t.Helper()
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = keys.Close() })
	h, err := admin.New(admin.Config{
		Password: password, Keys: keys, Objects: objs, Refs: refs, UI: testUI,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, login(t, srv)
}

type refInfoJSON struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Kind      string `json:"kind"`
}

func TestListRefs(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
		"zzz-bad":      []byte{0x42}, // undecodable record
	}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", srv.URL+"/admin/api/refs", cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refs = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Refs []refInfoJSON `json:"refs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Refs) != 3 {
		t.Fatalf("got %d refs, want 3: %+v", len(out.Refs), out.Refs)
	}
	if r := out.Refs[0]; r.Name != "backup/daily" || r.Kind != "dir" ||
		r.User != "alice@example.com" || r.Key == "" ||
		r.CreatedAt != "1970-01-01T00:00:00.000012345Z" {
		t.Fatalf("refs[0] = %+v", r)
	}
	if r := out.Refs[1]; r.Name != "motd" || r.Kind != "file" || r.User != "bob" {
		t.Fatalf("refs[1] = %+v", r)
	}
	if r := out.Refs[2]; r.Name != "zzz-bad" || r.Kind != "invalid" ||
		r.Key != "" || r.User != "" || r.CreatedAt != "" {
		t.Fatalf("refs[2] = %+v", r)
	}
}

func TestListRefsEmpty(t *testing.T) {
	srv, cookie := browseServer(t, memObjects{}, memRefs{})
	resp := do(t, "GET", srv.URL+"/admin/api/refs", cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refs = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Refs []refInfoJSON `json:"refs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Refs == nil || len(out.Refs) != 0 {
		t.Fatalf("refs = %#v, want an empty (non-null) array", out.Refs)
	}
}

type treeEntryJSON struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Size        uint64 `json:"size"`
	Mode        uint64 `json:"mode"`
	Mtime       int64  `json:"mtime"`
	Target      string `json:"target"`
	NameInvalid bool   `json:"raw_name_invalid"`
}

type treeJSON struct {
	Kind    string          `json:"kind"`
	Entries []treeEntryJSON `json:"entries"`
	More    bool            `json:"more"`
	Stat    *treeEntryJSON  `json:"stat"`
}

func treeURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/tree?" + q.Encode()
}

func getTree(t *testing.T, srv *httptest.Server, cookie *http.Cookie, params map[string]string, wantStatus int) treeJSON {
	t.Helper()
	resp := do(t, "GET", treeURL(srv, params), cookie, "")
	if resp.StatusCode != wantStatus {
		t.Fatalf("tree %v = %d, want %d", params, resp.StatusCode, wantStatus)
	}
	var out treeJSON
	if wantStatus == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestTreeListing(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	// Root listing: full metadata, name order.
	got := getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": ""}, http.StatusOK)
	if got.Kind != "dir" || got.More || len(got.Entries) != 2 {
		t.Fatalf("root tree = %+v", got)
	}
	if e := got.Entries[0]; e.Name != "hello.txt" || e.Kind != "file" ||
		e.Size != 12 || e.Mode != 0o100644 || e.Mtime != 1 {
		t.Fatalf("entries[0] = %+v", e)
	}
	if e := got.Entries[1]; e.Name != "sub" || e.Kind != "dir" || e.Mode != 0o040755 {
		t.Fatalf("entries[1] = %+v", e)
	}

	// Subdirectory: symlink carries its target.
	got = getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "sub"}, http.StatusOK)
	if len(got.Entries) != 2 || got.Entries[0].Kind != "symlink" || got.Entries[0].Target != "../hello.txt" {
		t.Fatalf("sub tree = %+v", got)
	}

	// A file path returns its stat.
	got = getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "sub/nested.txt"}, http.StatusOK)
	if got.Kind != "file" || got.Stat == nil || got.Stat.Size != 6 || got.Stat.Name != "nested.txt" {
		t.Fatalf("file tree = %+v", got)
	}

	// A ref that points straight at a file: stat from the key alone.
	got = getTree(t, srv, cookie, map[string]string{"ref": "motd", "path": ""}, http.StatusOK)
	if got.Kind != "file" || got.Stat == nil || got.Stat.Size != 12 {
		t.Fatalf("file-ref tree = %+v", got)
	}

	// Errors.
	getTree(t, srv, cookie, map[string]string{"ref": "nope", "path": ""}, http.StatusNotFound)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "ghost"}, http.StatusNotFound)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "hello.txt/x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "motd", "path": "x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"path": "x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "limit": "0"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "limit": "junk"}, http.StatusBadRequest)
}

// bigDirRef seeds a 1200-entry directory (multi-level prolly tree) under a
// "big" ref and returns the server.
func bigDirRef(t *testing.T) (*httptest.Server, *http.Cookie) {
	t.Helper()
	objs := memObjects{}
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	objs.put(blob)
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(3))
	emit := func(o fstree.Object) error { objs.put(o); return nil }
	for i := range 1200 {
		e := fstree.Entry{Name: fmt.Appendf(nil, "e%05d", i), Mode: 0o100644, ContentKey: blob.Key[:]}
		if err := db.AddEntry(emit, e); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(emit)
	if err != nil {
		t.Fatal(err)
	}
	refs := memRefs{"big": mustRef(t, "big", root, "alice")}
	return browseServer(t, objs, refs)
}

func TestTreePagination(t *testing.T) {
	srv, cookie := bigDirRef(t)

	// Page through with limit=500 (the default): 500 + 500 + 200.
	var names []string
	after := ""
	for page := 0; ; page++ {
		params := map[string]string{"ref": "big", "path": "", "after": after}
		got := getTree(t, srv, cookie, params, http.StatusOK)
		for _, e := range got.Entries {
			names = append(names, e.Name)
		}
		if !got.More {
			break
		}
		if page > 3 {
			t.Fatal("more=true never cleared")
		}
		after = got.Entries[len(got.Entries)-1].Name
	}
	if len(names) != 1200 || names[0] != "e00000" || names[1199] != "e01199" {
		t.Fatalf("paged %d names, first %q last %q", len(names), names[0], names[len(names)-1])
	}

	// Explicit small limit.
	got := getTree(t, srv, cookie, map[string]string{"ref": "big", "after": "e00009", "limit": "5"}, http.StatusOK)
	if len(got.Entries) != 5 || got.Entries[0].Name != "e00010" || !got.More {
		t.Fatalf("limited page = %+v", got)
	}

	// Limits above the cap are clamped to 1000, not rejected.
	got = getTree(t, srv, cookie, map[string]string{"ref": "big", "limit": "5000"}, http.StatusOK)
	if len(got.Entries) != 1000 || !got.More {
		t.Fatalf("clamped page: len=%d more=%v, want 1000 true", len(got.Entries), got.More)
	}
}

// TestTreeInvalidName pins the lossy-name contract: invalid UTF-8 is
// replaced with U+FFFD and flagged, never dropped or hidden.
func TestTreeInvalidName(t *testing.T) {
	objs := memObjects{}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a\x80b"), Mode: 0o100644, Mtime: 1, ContentKey: leafContentKey(t, objs)},
		{Name: []byte("ok"), Mode: 0o120777, Mtime: 2, LinkTarget: []byte("t\x80")},
	})
	if err != nil {
		t.Fatal(err)
	}
	objs.put(leaf)
	refs := memRefs{"weird": mustRef(t, "weird", leaf.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	got := getTree(t, srv, cookie, map[string]string{"ref": "weird"}, http.StatusOK)
	if len(got.Entries) != 2 {
		t.Fatalf("entries = %+v", got.Entries)
	}
	if e := got.Entries[0]; e.Name != "a�b" || !e.NameInvalid {
		t.Fatalf("invalid-name entry = %+v", e)
	}
	if e := got.Entries[1]; e.Name != "ok" || e.NameInvalid || e.Target != "t�" {
		t.Fatalf("ok entry = %+v", e)
	}
}

func rawURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/raw?" + q.Encode()
}

func TestRawFile(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	// View: inline disposition, typed by extension, sandboxed.
	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": "hello.txt"}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello, amber" {
		t.Fatalf("raw body = %q", body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain*", ct)
	}
	if resp.Header.Get("Content-Security-Policy") != "sandbox" {
		t.Fatalf("CSP = %q, want sandbox", resp.Header.Get("Content-Security-Policy"))
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "12" {
		t.Fatalf("Content-Length = %q, want 12", cl)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "inline") || !strings.Contains(cd, "hello.txt") {
		t.Fatalf("Content-Disposition = %q, want inline with filename", cd)
	}

	// Download: attachment disposition.
	resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": "hello.txt", "dl": "1"}), cookie, "")
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") {
		t.Fatalf("dl Content-Disposition = %q, want attachment", cd)
	}

	// A ref pointing straight at a file streams with the ref basename and a
	// sniffed type (no extension).
	resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "motd", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("file-ref raw = %d, want 200", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if string(body) != "hello, amber" {
		t.Fatalf("file-ref raw body = %q", body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("sniffed Content-Type = %q, want text/plain*", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "motd") {
		t.Fatalf("file-ref Content-Disposition = %q, want the ref basename", cd)
	}

	// Non-files are 400.
	for _, path := range []string{"sub", "sub/link"} {
		resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": path}), cookie, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("raw %s = %d, want 400", path, resp.StatusCode)
		}
	}
}

func TestRawChunkedFile(t *testing.T) {
	objs := memObjects{}
	first, err := fstree.EncodeBlob([]byte("first-"))
	if err != nil {
		t.Fatal(err)
	}
	objs.put(first)
	second, err := fstree.EncodeBlob([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	objs.put(second)
	node, err := fstree.EncodeFileNode([]key.Key{first.Key, second.Key})
	if err != nil {
		t.Fatal(err)
	}
	objs.put(node)
	refs := memRefs{"chunked": mustRef(t, "chunked", node.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "chunked", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "first-second" {
		t.Fatalf("chunked body = %q, want first-second", body)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "12" {
		t.Fatalf("Content-Length = %q, want 12", cl)
	}
}

func TestRawMissingObject(t *testing.T) {
	objs := memObjects{}
	// A ref whose target object was never stored.
	ghost, err := fstree.EncodeBlob([]byte("never stored"))
	if err != nil {
		t.Fatal(err)
	}
	refs := memRefs{"ghost": mustRef(t, "ghost", ghost.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "ghost", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("raw missing object = %d, want 404", resp.StatusCode)
	}

	// The same dangling object behind an extension-typed name must also
	// 404 cleanly — the Content-Type never needs the object, but the
	// pre-flight probe does.
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("ghost.txt"), Mode: 0o100644, Mtime: 1, ContentKey: ghost.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	objs.put(leaf)
	refs["dir"] = mustRef(t, "dir", leaf.Key, "alice")
	resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "dir", "path": "ghost.txt"}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("typed raw missing object = %d, want 404", resp.StatusCode)
	}
}

// TestRawHTMLSandboxed pins the stored-HTML defense: whether typed by
// extension or by content sniffing, HTML renders only inside an opaque
// CSP sandbox.
func TestRawHTMLSandboxed(t *testing.T) {
	objs := memObjects{}
	html, err := fstree.EncodeBlob([]byte("<html><script>document.title='pwned'</script></html>"))
	if err != nil {
		t.Fatal(err)
	}
	objs.put(html)
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("page.html"), Mode: 0o100644, Mtime: 1, ContentKey: html.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	objs.put(leaf)
	refs := memRefs{
		"site":     mustRef(t, "site", leaf.Key, "alice"),
		"htmlblob": mustRef(t, "htmlblob", html.Key, "alice"), // extensionless: sniffed
	}
	srv, cookie := browseServer(t, objs, refs)

	for name, params := range map[string]map[string]string{
		"by extension": {"ref": "site", "path": "page.html"},
		"by sniffing":  {"ref": "htmlblob", "path": ""},
	} {
		resp := do(t, "GET", rawURL(srv, params), cookie, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s = %d, want 200", name, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("%s Content-Type = %q, want text/html*", name, ct)
		}
		if csp := resp.Header.Get("Content-Security-Policy"); csp != "sandbox" {
			t.Fatalf("%s CSP = %q, want exactly sandbox", name, csp)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s missing nosniff", name)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("%s Cache-Control = %q, want no-store", name, cc)
		}
	}
}

// leafContentKey stores a small blob and returns its key bytes.
func leafContentKey(t *testing.T, objs memObjects) []byte {
	t.Helper()
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	objs.put(blob)
	return blob.Key[:]
}

func TestTreeDotDotPath(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, cookie := browseServer(t, objs, refs)
	for _, p := range []string{"..", "sub/..", "../sub"} {
		getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": p}, http.StatusBadRequest)
	}
}

func TestTreeUnbrowsableRef(t *testing.T) {
	objs := memObjects{}
	xattrs, err := fstree.EncodeXattrSet(map[string][]byte{"user.a": []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	objs.put(xattrs)
	refs := memRefs{"odd": mustRef(t, "odd", xattrs.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)
	getTree(t, srv, cookie, map[string]string{"ref": "odd"}, http.StatusBadRequest)
}

// TestTreeNextCursor pages a directory whose names include invalid UTF-8
// using the server-supplied raw cursor; the walk must visit every entry
// exactly once and terminate.
func TestTreeNextCursor(t *testing.T) {
	objs := memObjects{}
	ck := leafContentKey(t, objs)
	// Bytewise-sorted names, two of them invalid UTF-8.
	rawNames := [][]byte{[]byte("ab"), []byte("a\x80"), []byte("a\xff"), []byte("b")}
	entries := make([]fstree.Entry, len(rawNames))
	for i, n := range rawNames {
		entries[i] = fstree.Entry{Name: n, Mode: 0o100644, Mtime: int64(i), ContentKey: ck}
	}
	leaf, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	objs.put(leaf)
	refs := memRefs{"weird": mustRef(t, "weird", leaf.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	var mtimes []int64
	next := ""
	for page := 0; ; page++ {
		u := treeURL(srv, map[string]string{"ref": "weird", "limit": "1"})
		if next != "" {
			u += "&after=" + next // next is pre-encoded by the server
		}
		resp := do(t, "GET", u, cookie, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d = %d, want 200", page, resp.StatusCode)
		}
		var out struct {
			Entries []treeEntryJSON `json:"entries"`
			More    bool            `json:"more"`
			Next    string          `json:"next"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		for _, e := range out.Entries {
			mtimes = append(mtimes, e.Mtime)
		}
		if !out.More {
			if out.Next != "" {
				t.Fatalf("page %d: next %q set without more", page, out.Next)
			}
			break
		}
		if out.Next == "" {
			t.Fatalf("page %d: more without next", page)
		}
		next = out.Next
		if page > 5 {
			t.Fatal("cursor never terminated")
		}
	}
	// Mtimes are unique per entry, so they prove each entry arrived
	// exactly once and in order despite the lossy JSON names.
	if len(mtimes) != 4 || mtimes[0] != 0 || mtimes[1] != 1 || mtimes[2] != 2 || mtimes[3] != 3 {
		t.Fatalf("paged mtimes = %v, want [0 1 2 3]", mtimes)
	}
}

func archiveURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/archive?" + q.Encode()
}

// tarNames reads all member names (and the content of regular files) from a
// tar stream.
func tarNames(t *testing.T, rd io.Reader) map[string]string {
	t.Helper()
	out := map[string]string{}
	tr := tar.NewReader(rd)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content := ""
		if hdr.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			content = string(b)
		}
		out[hdr.Name] = content
	}
	return out
}

func wantSeedTreeMembers(t *testing.T, got map[string]string) {
	t.Helper()
	want := map[string]string{
		"hello.txt":      "hello, amber",
		"sub/":           "",
		"sub/link":       "",
		"sub/nested.txt": "nested",
	}
	if len(got) != len(want) {
		t.Fatalf("archive members = %v, want %v", got, want)
	}
	for name, content := range want {
		if c, ok := got[name]; !ok || c != content {
			t.Fatalf("member %q = %q,%v want %q", name, c, ok, content)
		}
	}
}

func TestArchiveTar(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	// Default format is tar; the filename derives from the ref basename.
	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("archive = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-tar" {
		t.Fatalf("Content-Type = %q, want application/x-tar", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") || !strings.Contains(cd, "daily.tar") {
		t.Fatalf("Content-Disposition = %q, want attachment daily.tar", cd)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
	wantSeedTreeMembers(t, tarNames(t, resp.Body))

	// A subdirectory archive contains only that subtree.
	resp = do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "path": "sub", "format": "tar"}), cookie, "")
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "sub.tar") {
		t.Fatalf("Content-Disposition = %q, want sub.tar", cd)
	}
	got := tarNames(t, resp.Body)
	if len(got) != 2 || got["nested.txt"] != "nested" {
		t.Fatalf("sub archive members = %v", got)
	}
}

func TestArchiveTgz(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "format": "tgz"}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("archive = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("Content-Type = %q, want application/gzip", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "daily.tar.gz") {
		t.Fatalf("Content-Disposition = %q, want daily.tar.gz", cd)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	wantSeedTreeMembers(t, tarNames(t, gz))
}

func TestArchiveErrors(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	for name, params := range map[string]map[string]string{
		"file path":  {"ref": "backup/daily", "path": "hello.txt"},
		"file ref":   {"ref": "motd"},
		"bad format": {"ref": "backup/daily", "format": "zip"},
	} {
		resp := do(t, "GET", archiveURL(srv, params), cookie, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", name, resp.StatusCode)
		}
	}
	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "nope"}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown ref = %d, want 404", resp.StatusCode)
	}

	// A dangling ref (object never stored) is a clean 404, not an abort.
	ghostDir, err := fstree.EncodeDirLeaf([]fstree.Entry{})
	if err != nil {
		t.Fatal(err)
	}
	refs["dangling"] = mustRef(t, "dangling", ghostDir.Key, "alice")
	resp = do(t, "GET", archiveURL(srv, map[string]string{"ref": "dangling"}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("dangling ref = %d, want 404", resp.StatusCode)
	}
	// And filenames for "." paths fall back sensibly.
	resp = do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "path": "./sub/."}), cookie, "")
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "sub.tar") || strings.Contains(cd, "..tar") {
		t.Fatalf("dot-path Content-Disposition = %q, want sub.tar", cd)
	}
}

// TestBrowseRequiresSession is the spec's authentication gate: no live
// session cookie, no content — on every browse endpoint.
func TestBrowseRequiresSession(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, _ := browseServer(t, objs, refs)

	for _, path := range []string{
		"/admin/api/refs",
		"/admin/api/tree?ref=backup/daily",
		"/admin/api/raw?ref=backup/daily&path=hello.txt",
		"/admin/api/archive?ref=backup/daily",
	} {
		resp := do(t, "GET", srv.URL+path, nil, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s without session = %d, want 401", path, resp.StatusCode)
		}
	}
}
