package nixcache_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/draganm/amber-store/narexport"
	"github.com/draganm/amber-store/nixcache"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// upstream is a fake binary cache serving one signed path.
type upstream struct {
	srv     *httptest.Server
	pub     ed25519.PublicKey
	narinfo []byte
	nar     []byte // compressed
	narURL  string
}

func newUpstream(t *testing.T, compression string, tamper func(nar, doc []byte) ([]byte, []byte)) *upstream {
	t.Helper()
	st := newRecStore()
	root := buildTree(t, st, []byte("upstream content"))
	var nar bytes.Buffer
	if err := narexport.Export(&nar, root, st.Get); err != nil {
		t.Fatal(err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n := nixcache.Narinfo{
		StorePath:  "/nix/store/" + hashPart(7) + "-upstream-1.0",
		NarHash:    sha256.Sum256(nar.Bytes()),
		NarSize:    uint64(nar.Len()),
		References: []string{hashPart(7) + "-upstream-1.0"},
	}
	sig := "test-1:" + base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(n.Fingerprint())))

	var comp bytes.Buffer
	switch compression {
	case "zstd":
		zw, _ := zstd.NewWriter(&comp)
		zw.Write(nar.Bytes())
		zw.Close()
	case "xz":
		xw, err := xz.NewWriter(&comp)
		if err != nil {
			t.Fatal(err)
		}
		xw.Write(nar.Bytes())
		xw.Close()
	default:
		comp.Write(nar.Bytes())
	}

	u := &upstream{pub: pub, narURL: "nar/abc123.nar." + compression}
	doc := fmt.Sprintf("StorePath: %s\nURL: %s\nCompression: %s\nNarHash: sha256:%s\nNarSize: %d\nReferences: %s\nSig: %s\n",
		n.StorePath, u.narURL, compression, nixcache.EncodeNixBase32(n.NarHash[:]), n.NarSize,
		n.References[0], sig)
	u.nar, u.narinfo = comp.Bytes(), []byte(doc)
	if tamper != nil {
		u.nar, u.narinfo = tamper(u.nar, u.narinfo)
	}

	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/" + hashPart(7) + ".narinfo":
			w.Write(u.narinfo)
		case "/" + u.narURL:
			w.Write(u.nar)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *upstream) fetcher(st *recStore) *nixcache.Fetcher {
	return &nixcache.Fetcher{
		BaseURL: u.srv.URL,
		Trusted: map[string]ed25519.PublicKey{"test-1": u.pub},
		Emit:    st.emit,
		Get:     st.Get,
	}
}

func TestFetchPath(t *testing.T) {
	for _, compression := range []string{"zstd", "xz", "none"} {
		t.Run(compression, func(t *testing.T) {
			u := newUpstream(t, compression, nil)
			st := newRecStore()
			pi, err := u.fetcher(st).FetchPath(context.Background(), hashPart(7))
			if err != nil {
				t.Fatal(err)
			}
			if pi.UpstreamCompression != compression || len(pi.Sigs) != 1 {
				t.Fatalf("%+v", pi)
			}
			if _, err := st.Get(pi.RootKey); err != nil {
				t.Fatal("tree not ingested")
			}
		})
	}
}

func TestFetchPathRejects(t *testing.T) {
	cases := map[string]func(nar, doc []byte) ([]byte, []byte){
		"tampered nar": func(nar, doc []byte) ([]byte, []byte) {
			var comp bytes.Buffer
			zw, _ := zstd.NewWriter(&comp)
			zw.Write([]byte("evil"))
			zw.Close()
			return comp.Bytes(), doc
		},
		"bad signature": func(nar, doc []byte) ([]byte, []byte) {
			return nar, bytes.Replace(doc, []byte("NarSize: "), []byte("NarSize: 9"), 1)
		},
		"no signature": func(nar, doc []byte) ([]byte, []byte) {
			return nar, bytes.ReplaceAll(doc, []byte("Sig: "), []byte("XSig: "))
		},
		"wrong path": func(nar, doc []byte) ([]byte, []byte) {
			return nar, bytes.Replace(doc, []byte(hashPart(7)+"-upstream"), []byte(hashPart(8)+"-upstream"), 1)
		},
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			u := newUpstream(t, "zstd", tamper)
			st := newRecStore()
			if _, err := u.fetcher(st).FetchPath(context.Background(), hashPart(7)); err == nil {
				t.Fatal("fetch accepted tampered upstream")
			}
		})
	}
}

func TestFetchPathUpstream404(t *testing.T) {
	u := newUpstream(t, "zstd", nil)
	st := newRecStore()
	if _, err := u.fetcher(st).FetchPath(context.Background(), hashPart(8)); err == nil {
		t.Fatal("missing upstream path accepted")
	}
}
