package daemon_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

func TestGetFile_Blob(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	blob := mustBlob(t, "alpha")
	if _, err := c.Ingest(context.Background(), packOf(t, blob)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	rc, err := c.File(context.Background(), blob.Key)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "alpha" {
		t.Fatalf("File = %q, want %q", got, "alpha")
	}
}

func TestGetFile_MultiChunkFileNode(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	a, b, d := mustBlob(t, "alpha"), mustBlob(t, "beta"), mustBlob(t, "gamma")
	fn, err := fstree.EncodeFileNode([]key.Key{a.Key, b.Key, d.Key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ingest(context.Background(), packOf(t, a, b, d, fn)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	rc, err := c.File(context.Background(), fn.Key)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	const want = "alphabetagamma"
	if string(got) != want {
		t.Fatalf("File = %q, want %q", got, want)
	}
	if uint64(len(got)) != fn.Key.Length() {
		t.Fatalf("read %d bytes, key length says %d", len(got), fn.Key.Length())
	}
}

func TestGetFile_NotFound(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	// A valid blob key that was never ingested.
	blob := mustBlob(t, "absent")
	if _, err := c.File(context.Background(), blob.Key); err == nil {
		t.Fatal("File of an un-ingested key: want error, got nil")
	}
}

func TestGetFile_RejectsDirectoryKey(t *testing.T) {
	store := openStore(t)
	c := serveOnSocket(t, store)

	x := mustBlob(t, "x")
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("f"), Mode: 0o100644, Mtime: 1, ContentKey: x.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.File(context.Background(), leaf.Key); err == nil {
		t.Fatal("File of a directory key: want error, got nil")
	}
}

func TestGetFile_SetsContentLength(t *testing.T) {
	store := openStore(t)
	srv := httptest.NewServer(newHandler(t, store, nil))
	defer srv.Close()
	c := serveOnSocket(t, store)

	blob := mustBlob(t, "alpha")
	if _, err := c.Ingest(context.Background(), packOf(t, blob)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := http.Get(srv.URL + "/v1/file/" + blob.Key.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.FormatUint(blob.Key.Length(), 10) {
		t.Fatalf("Content-Length = %q, want %d", got, blob.Key.Length())
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "alpha" {
		t.Fatalf("body = %q, want alpha", body)
	}
}
