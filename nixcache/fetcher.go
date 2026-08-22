package nixcache

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/narexport"
	"github.com/draganm/amber-store/narimport"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// Fetcher ingests store paths from an upstream binary cache. It is the only
// component talking to upstream.
type Fetcher struct {
	// BaseURL of the upstream cache, e.g. https://cache.nixos.org.
	BaseURL string
	// Trusted narinfo signing keys. At least one signature must verify.
	Trusted map[string]ed25519.PublicKey
	// Emit receives the imported tree's objects.
	Emit fstree.Emit
	// Get reads emitted objects back for the round-trip gate.
	Get    func(key.Key) ([]byte, error)
	Client *http.Client
}

// FetchPath fetches, verifies, and ingests one store path: narinfo signature
// against Trusted, then the NAR, then the round-trip gate — the imported tree
// re-exported must hash to the signed NarHash. Nothing is returned (and so
// nothing indexed or pinned) unless every check passes.
func (f *Fetcher) FetchPath(ctx context.Context, hashpart string) (PathInfo, error) {
	n, err := f.fetchNarinfo(ctx, hashpart)
	if err != nil {
		return PathInfo{}, err
	}
	root, err := f.fetchNar(ctx, n)
	if err != nil {
		return PathInfo{}, fmt.Errorf("nixcache: NAR for %s: %w", n.StorePath, err)
	}
	pi := n.pathInfo(root)
	pi.IngestedAt = time.Now().Unix()
	return pi, nil
}

// pathInfo lifts a verified narinfo into an index record for the tree at
// root. IngestedAt is left for the caller.
func (n Narinfo) pathInfo(root key.Key) PathInfo {
	return PathInfo{
		StorePath:           n.StorePath,
		RootKey:             root,
		NarHash:             n.NarHash,
		NarSize:             n.NarSize,
		References:          n.References,
		Deriver:             n.Deriver,
		Sigs:                n.Sigs,
		UpstreamCompression: n.Compression,
	}
}

func (f *Fetcher) fetchNarinfo(ctx context.Context, hashpart string) (Narinfo, error) {
	body, err := f.get(ctx, hashpart+".narinfo")
	if err != nil {
		return Narinfo{}, err
	}
	defer body.Close()
	doc, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return Narinfo{}, err
	}
	n, err := ParseNarinfo(doc)
	if err != nil {
		return Narinfo{}, err
	}
	if HashPart(n.StorePath) != hashpart {
		return Narinfo{}, fmt.Errorf("nixcache: narinfo for %s answers query %s", n.StorePath, hashpart)
	}
	for _, sig := range n.Sigs {
		if n.VerifySig(sig, f.Trusted) {
			return n, nil
		}
	}
	return Narinfo{}, fmt.Errorf("nixcache: no trusted signature on %s", n.StorePath)
}

func (f *Fetcher) fetchNar(ctx context.Context, n Narinfo) (key.Key, error) {
	body, err := f.get(ctx, n.URL)
	if err != nil {
		return key.Key{}, err
	}
	defer body.Close()
	nar, err := decompress(n.Compression, body)
	if err != nil {
		return key.Key{}, err
	}
	defer nar.Close()

	root, err := narimport.Import(nar, f.Emit)
	if err != nil {
		return key.Key{}, err
	}
	h := sha256.New()
	if err := narexport.Export(h, root, f.Get); err != nil {
		return key.Key{}, fmt.Errorf("round-trip export: %w", err)
	}
	if [32]byte(h.Sum(nil)) != n.NarHash {
		return key.Key{}, errors.New("round-trip mismatch against signed NarHash")
	}
	return root, nil
}

func (f *Fetcher) get(ctx context.Context, path string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.BaseURL+"/"+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cmp.Or(f.Client, http.DefaultClient).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("nixcache: GET %s: %s", path, resp.Status)
	}
	return resp.Body, nil
}

func decompress(compression string, r io.Reader) (io.ReadCloser, error) {
	switch compression {
	case "zstd":
		dec, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return dec.IOReadCloser(), nil
	case "xz":
		xr, err := xz.NewReader(r)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(xr), nil
	case "none", "":
		return io.NopCloser(r), nil
	default:
		return nil, fmt.Errorf("nixcache: unsupported compression %q", compression)
	}
}
