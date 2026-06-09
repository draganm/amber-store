package daemon_test

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
	srv := &http.Server{Handler: daemon.New(store)}
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
	srv := httptest.NewServer(daemon.New(store))
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
	srv := httptest.NewServer(daemon.New(store))
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

	body, err := c.Tar(context.Background(), leaf.Key)
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

func TestGetTar_MissingRootIs404(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(daemon.New(store))
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
	srv := httptest.NewServer(daemon.New(store))
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
