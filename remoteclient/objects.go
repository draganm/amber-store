package remoteclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/httpsig"
	"github.com/draganm/amber-store/internal/keylist"
	"github.com/draganm/amber-store/key"
	"github.com/zeebo/blake3"
)

// Missing returns the subset of keys the server does not have.
func (c *Client) Missing(ctx context.Context, keys []key.Key) ([]key.Key, error) {
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects/missing", "application/octet-stream", keylist.Flatten(keys))
	if err != nil {
		return nil, err
	}
	return keylist.Parse(body)
}

// PushPack uploads objs as one amberpack to the remote, tagged with the (ref,
// root) the objects belong to. The server stages the pack durably and acks
// before processing it; this returns once the pack is accepted, not once it is
// stored. Completeness is enforced when the reference is set.
func (c *Client) PushPack(ctx context.Context, ref string, root key.Key, objs []fstree.Object) error {
	return c.postPack(ctx, ref, root, func(w io.Writer) error {
		pw := amberpack.NewWriter(w)
		for _, o := range objs {
			if err := pw.Add(o); err != nil {
				return err
			}
		}
		return pw.Close()
	})
}

// PushPackRaw uploads pre-encoded records (EncodeRecord outputs, as stored
// verbatim on disk) as one amberpack, identically to PushPack but without
// decoding and re-encoding each object. It is the zero-copy push path: records
// read straight from a local packstore are wire-format-identical, so they travel
// untouched. The server validates each record's framing and CRC on receipt.
func (c *Client) PushPackRaw(ctx context.Context, ref string, root key.Key, records [][]byte) error {
	return c.postPack(ctx, ref, root, func(w io.Writer) error {
		pw := amberpack.NewWriter(w)
		for _, rec := range records {
			if err := pw.AddRecord(rec); err != nil {
				return err
			}
		}
		return pw.Close()
	})
}

// postPack streams an amberpack body (emitted by writeBody) tagged with
// (ref, root) to the server without ever holding the whole pack in memory. The
// pack is written twice: first into a blake3 hasher to compute the body hash
// the request signature needs (the signature header must precede the body),
// then into a pipe feeding the request body. For PushPackRaw both passes are
// cheap copies of records that already live in memory; this avoids the extra
// full-pack buffer the upload would otherwise allocate per in-flight pack.
func (c *Client) postPack(ctx context.Context, ref string, root key.Key, writeBody func(io.Writer) error) error {
	h := blake3.New()
	if err := writeBody(h); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	go func() {
		if err := writeBody(pw); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	q := url.Values{}
	q.Set("ref", ref)
	q.Set("root", root.String())
	_, _, err := c.doStreaming(ctx, http.MethodPost, "/v1/objects?"+q.Encode(), "application/octet-stream", h.Sum(nil), pr)
	return err
}

// ReachableKeys asks the server for the full set of keys reachable from root:
// the server walks its tree and returns them as raw concatenated 32-byte keys.
// The response is verified against the pinned server key by do.
func (c *Client) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects/reachable", "application/octet-stream", root[:])
	if err != nil {
		return nil, err
	}
	return keylist.Parse(body)
}

// FetchObjects downloads the requested objects as an amberpack carrying an
// in-band trailing signature (httpsig.AppendSignatureTrailer): the server
// streams the pack while hashing it and appends the signature once the body
// hash is known. The signature is part of the body — not an HTTP trailer — so
// it survives reverse proxies that drop trailers. The client buffers the
// response (bounded by the pull batch size), splits off the signature, verifies
// it over the exact pack bytes against the pinned key, then parses the pack.
// Error responses (non-200) are header-signed like every other endpoint.
func (c *Client) FetchObjects(ctx context.Context, keys []key.Key) ([]fstree.Object, error) {
	req, nonce, err := c.signedRequest(ctx, http.MethodPost, "/v1/objects/get", "application/octet-stream", keylist.Flatten(keys))
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting remote %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		if err := httpsig.VerifyResponse(c.serverPubWire, nonce, resp.StatusCode,
			httpsig.HashBody(body), resp.Header.Get(httpsig.HeaderSignature)); err != nil {
			return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
		}
		return nil, &StatusError{Code: resp.StatusCode, Msg: string(body)}
	}
	pack, sig, ok := httpsig.SplitSignatureTrailer(body)
	if !ok {
		return nil, fmt.Errorf("server identity mismatch for %s: response is missing its in-band signature", c.base)
	}
	if err := httpsig.VerifyResponse(c.serverPubWire, nonce, http.StatusOK, httpsig.HashBody(pack), sig); err != nil {
		return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
	}
	var objs []fstree.Object
	for o, err := range amberpack.NewReader(bytes.NewReader(pack)).All() {
		if err != nil {
			return nil, fmt.Errorf("reading object stream: %w", err)
		}
		objs = append(objs, o)
	}
	return objs, nil
}
