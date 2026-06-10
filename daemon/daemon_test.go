package daemon_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// serveOnSocket starts the daemon handler on a fresh unix socket under a temp
// dir and returns a client for it. The listener closes at test end.
// We use os.MkdirTemp with /tmp as the parent to keep the path short enough
// for macOS's 104-byte unix socket path limit.
func serveOnSocket(t *testing.T, store *diskstore.Store) *client.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "amber-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: daemon.New(store, openRefs(t), nil)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return client.New(sock)
}

func openStore(t *testing.T) *diskstore.Store {
	t.Helper()
	s, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// packOf serializes objects into a pack-write stream.
func packOf(t *testing.T, objs ...fstree.Object) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	w := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func mustBlob(t *testing.T, s string) fstree.Object {
	t.Helper()
	o, err := fstree.EncodeBlob([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// syncBuf is a concurrency-safe log sink: the server goroutine writes while the
// test polls String.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitForLog polls buf until every want substring appears, or fails the test.
// The request log line is written after the handler returns, which may race the
// client seeing the response, so the test cannot assert synchronously.
func waitForLog(t *testing.T, buf *syncBuf, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := buf.String()
		missing := ""
		for _, w := range wants {
			if !strings.Contains(got, w) {
				missing = w
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log output %q never contained %q", got, missing)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLogging_RejectedIngestIsLogged(t *testing.T) {
	store := openStore(t)
	buf := &syncBuf{}
	srv := httptest.NewServer(daemon.New(store, openRefs(t), slog.New(slog.NewTextHandler(buf, nil))))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/objects", "application/octet-stream",
		bytes.NewReader([]byte("not a valid pack stream")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	// The rejection reason and the request line must both be logged.
	waitForLog(t, buf, "malformed stream", "POST", "/v1/objects", "status=422")
}

func TestLogging_SuccessfulIngestIsLogged(t *testing.T) {
	store := openStore(t)
	buf := &syncBuf{}
	srv := httptest.NewServer(daemon.New(store, openRefs(t), slog.New(slog.NewTextHandler(buf, nil))))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/objects", "application/octet-stream",
		packOf(t, mustBlob(t, "alpha")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	waitForLog(t, buf, "ingest complete", "stored=1", "status=200")
}

func TestPostObjects_StoresAndReportsStats(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	a, b := mustBlob(t, "alpha"), mustBlob(t, "beta")
	stats, err := c.Ingest(context.Background(), packOf(t, a, b, a))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if stats.ObjectsStored != 2 || stats.ObjectsDeduped != 1 {
		t.Fatalf("stats = %+v, want stored=2 deduped=1", stats)
	}
	if stats.BytesStored != int64(len("alpha")+len("beta")) {
		t.Fatalf("BytesStored = %d, want %d", stats.BytesStored, len("alpha")+len("beta"))
	}
	if got, _ := store.Get(a.Key); string(got) != "alpha" {
		t.Fatalf("stored blob = %q, want alpha", got)
	}
}

func TestPostObjects_MalformedStreamIs422(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/objects", "application/octet-stream",
		bytes.NewReader([]byte("not a valid pack stream")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for a malformed stream", resp.StatusCode)
	}
}

func TestPostObjects_TamperedKeyIs422(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	good := mustBlob(t, "honest")
	bad := good
	bad.Key[key.Size-1] ^= 0xFF // key no longer matches payload

	resp, err := http.Post(srv.URL+"/v1/objects", "application/octet-stream", packOf(t, bad))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for a tampered key", resp.StatusCode)
	}
}

func TestPostObjects_RejectsTamperedKey(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	good := mustBlob(t, "honest")
	bad := good
	bad.Key[key.Size-1] ^= 0xFF // key no longer matches payload

	_, err := c.Ingest(context.Background(), packOf(t, bad))
	if err == nil {
		t.Fatalf("expected error uploading a tampered object")
	}
	if has, _ := store.Has(bad.Key); has {
		t.Fatalf("tampered object must not be stored")
	}
}

func TestGetTar_RoundTripAfterSplitUpload(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	// Build a one-leaf directory with two files, then upload the objects in TWO
	// separate packs (the blobs in one, the leaf in another) to exercise partial
	// uploads.
	a, b := mustBlob(t, "alpha"), mustBlob(t, "beta")
	entries := []fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: a.Key[:]},
		{Name: []byte("b"), Mode: 0o100644, Mtime: 2, ContentKey: b.Key[:]},
	}
	leaf, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.Ingest(context.Background(), packOf(t, a, b)); err != nil {
		t.Fatalf("upload blobs: %v", err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, leaf)); err != nil {
		t.Fatalf("upload leaf: %v", err)
	}

	body, err := c.Tar(context.Background(), leaf.Key, "")
	if err != nil {
		t.Fatalf("Tar: %v", err)
	}
	defer body.Close()

	got := map[string]string{}
	tr := tar.NewReader(body)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if got["a"] != "alpha" || got["b"] != "beta" {
		t.Fatalf("tar = %v, want a=alpha b=beta", got)
	}
}

func TestGetTar_PathTarsSubdirectory(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	content := mustBlob(t, "alpha")
	inner, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("deep.txt"), Mode: 0o100644, Mtime: 1, ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	mid, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("inner"), Mode: 0o040755, Mtime: 2, ContentKey: inner.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, Mtime: 3, ContentKey: content.Key[:]},
		{Name: []byte("mid"), Mode: 0o040755, Mtime: 4, ContentKey: mid.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, content, inner, mid, root)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	body, err := c.Tar(context.Background(), root.Key, "mid/inner")
	if err != nil {
		t.Fatalf("Tar(mid/inner): %v", err)
	}
	defer body.Close()

	got := map[string]string{}
	tr := tar.NewReader(body)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		got[h.Name] = string(data)
	}
	if len(got) != 1 || got["deep.txt"] != "alpha" {
		t.Fatalf("tar = %v, want only deep.txt=alpha", got)
	}

	// A missing path component is a 404.
	if _, err := c.Tar(context.Background(), root.Key, "mid/nope"); err == nil ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("missing path: err = %v, want 404", err)
	}

	// A path through a regular file is a 400.
	if _, err := c.Tar(context.Background(), root.Key, "file"); err == nil ||
		!strings.Contains(err.Error(), "400") {
		t.Fatalf("file path: err = %v, want 400", err)
	}
}

func TestGetTar_MissingRootIs404(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	// A well-formed directory key that was never stored.
	xb := mustBlob(t, "x")
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("x"), Mode: 0o100644, ContentKey: xb.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/v1/tar/" + leaf.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an absent root", resp.StatusCode)
	}
}

func TestGetTar_NonDirectoryKeyIs400(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	// A blob key is not a directory object; rejected with 400 before any lookup,
	// so it need not be stored.
	blob := mustBlob(t, "data")
	resp, err := http.Get(srv.URL + "/v1/tar/" + blob.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a non-directory key", resp.StatusCode)
	}
}

func TestGetLs_ListsDirectoryEntries(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	// A subdirectory leaf and a root leaf holding a file, a symlink and the
	// subdirectory.
	content := mustBlob(t, "alpha")
	subLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("nested"), Mode: 0o100600, Mtime: 3, ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, UID: 501, GID: 20, Mtime: 1, ContentKey: content.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, Mtime: 2, LinkTarget: []byte("a.txt")},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: 4, ContentKey: subLeaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, content, subLeaf, rootLeaf)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := c.Ls(context.Background(), rootLeaf.Key, "")
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(got), got)
	}

	file := got[0]
	if file.Name != "a.txt" || file.Mode != 0o100644 || file.UID != 501 || file.GID != 20 {
		t.Fatalf("file entry = %+v", file)
	}
	if file.MtimeNs != 1 || file.Size != uint64(len("alpha")) || file.Key != content.Key.String() {
		t.Fatalf("file entry = %+v", file)
	}

	link := got[1]
	if link.Name != "link" || link.LinkTarget != "a.txt" || link.Size != uint64(len("a.txt")) {
		t.Fatalf("link entry = %+v", link)
	}

	sub := got[2]
	if sub.Name != "sub" || sub.Key != subLeaf.Key.String() {
		t.Fatalf("sub entry = %+v", sub)
	}

	// The subdirectory's key from the listing must itself be listable.
	subKey, err := key.Parse(subLeaf.Key[:])
	if err != nil {
		t.Fatal(err)
	}
	nested, err := c.Ls(context.Background(), subKey, "")
	if err != nil {
		t.Fatalf("Ls(sub): %v", err)
	}
	if len(nested) != 1 || nested[0].Name != "nested" {
		t.Fatalf("nested entries = %+v", nested)
	}
}

func TestGetLs_StreamsNDJSON(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	content := mustBlob(t, "alpha")
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, Mtime: 1, ContentKey: content.Key[:]},
		{Name: []byte("b"), Mode: 0o100644, Mtime: 2, ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(leaf.Key, leaf.Bytes); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/v1/ls/" + leaf.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), body)
	}
	for _, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %q is not valid JSON: %v", line, err)
		}
	}
}

func TestGetLs_MissingDirIs404(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	xb := mustBlob(t, "x")
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("x"), Mode: 0o100644, ContentKey: xb.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/v1/ls/" + leaf.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an absent directory", resp.StatusCode)
	}
}

func TestGetLs_NonDirectoryKeyIs400(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	blob := mustBlob(t, "data")
	resp, err := http.Get(srv.URL + "/v1/ls/" + blob.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a non-directory key", resp.StatusCode)
	}
}

func TestGetContentKeys_ListsReachableKeys(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	content := mustBlob(t, "alpha")
	subLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("nested"), Mode: 0o100644, Mtime: 1, ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// content is referenced both at the root and in the subdirectory; it must be
	// listed once.
	root, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, Mtime: 2, ContentKey: content.Key[:]},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: 3, ContentKey: subLeaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, content, subLeaf, root)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := c.ContentKeys(context.Background(), root.Key, "")
	if err != nil {
		t.Fatalf("ContentKeys: %v", err)
	}
	want := []string{root.Key.String(), content.Key.String(), subLeaf.Key.String()}
	if len(got) != len(want) {
		t.Fatalf("got %d keys %v, want %v", len(got), got, want)
	}
	wantSet := map[string]bool{}
	for _, k := range want {
		wantSet[k] = true
	}
	for _, k := range got {
		if !wantSet[k] {
			t.Errorf("unexpected key %s", k)
		}
	}
	if got[0] != root.Key.String() {
		t.Errorf("first key = %s, want the root %s", got[0], root.Key)
	}

	// With a path the listing is rooted at the subdirectory.
	got, err = c.ContentKeys(context.Background(), root.Key, "sub")
	if err != nil {
		t.Fatalf("ContentKeys(sub): %v", err)
	}
	want = []string{subLeaf.Key.String(), content.Key.String()}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("keys = %v, want %v", got, want)
	}

	// A missing path component is a 404.
	if _, err := c.ContentKeys(context.Background(), root.Key, "nope"); err == nil ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("missing path: err = %v, want 404", err)
	}

	// A path through a regular file is a 400.
	if _, err := c.ContentKeys(context.Background(), root.Key, "a.txt"); err == nil ||
		!strings.Contains(err.Error(), "400") {
		t.Fatalf("file path: err = %v, want 400", err)
	}
}

func TestGetContentKeys_FileKeyRoot(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	content := mustBlob(t, "alpha")
	if _, err := c.Ingest(context.Background(), packOf(t, content)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// A blob key is a valid root: its content is just itself.
	got, err := c.ContentKeys(context.Background(), content.Key, "")
	if err != nil {
		t.Fatalf("ContentKeys(blob): %v", err)
	}
	if len(got) != 1 || got[0] != content.Key.String() {
		t.Fatalf("keys = %v, want [%s]", got, content.Key)
	}

	// A path under a non-directory root is a 400.
	if _, err := c.ContentKeys(context.Background(), content.Key, "sub"); err == nil ||
		!strings.Contains(err.Error(), "400") {
		t.Fatalf("path under blob: err = %v, want 400", err)
	}
}

func TestGetContentKeys_MissingRootIs404(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store, openRefs(t), nil))
	defer srv.Close()

	xb := mustBlob(t, "x")
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("x"), Mode: 0o100644, ContentKey: xb.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/v1/content-keys/" + leaf.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an absent root", resp.StatusCode)
	}
}

func TestGetLs_PathListsSubdirectory(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	content := mustBlob(t, "alpha")
	inner, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("deep.txt"), Mode: 0o100644, Mtime: 1, ContentKey: content.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	mid, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("inner"), Mode: 0o040755, Mtime: 2, ContentKey: inner.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, Mtime: 3, ContentKey: content.Key[:]},
		{Name: []byte("mid"), Mode: 0o040755, Mtime: 4, ContentKey: mid.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, content, inner, mid, root)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := c.Ls(context.Background(), root.Key, "mid/inner")
	if err != nil {
		t.Fatalf("Ls(mid/inner): %v", err)
	}
	if len(got) != 1 || got[0].Name != "deep.txt" {
		t.Fatalf("entries = %+v, want [deep.txt]", got)
	}

	// A missing path component is a 404.
	if _, err := c.Ls(context.Background(), root.Key, "mid/nope"); err == nil ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("missing path: err = %v, want 404", err)
	}

	// A path through a regular file is a 400.
	if _, err := c.Ls(context.Background(), root.Key, "file"); err == nil ||
		!strings.Contains(err.Error(), "400") {
		t.Fatalf("file path: err = %v, want 400", err)
	}
}
