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
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	return c.postPack(ctx, ref, root, buf.Bytes())
}

// PushPackRaw uploads pre-encoded records (EncodeRecord outputs, as stored
// verbatim on disk) as one amberpack, identically to PushPack but without
// decoding and re-encoding each object. It is the zero-copy push path: records
// read straight from a local packstore are wire-format-identical, so they travel
// untouched. The server validates each record's framing and CRC on receipt.
func (c *Client) PushPackRaw(ctx context.Context, ref string, root key.Key, records [][]byte) error {
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, rec := range records {
		if err := pw.AddRecord(rec); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	return c.postPack(ctx, ref, root, buf.Bytes())
}

// postPack POSTs an assembled pack body tagged with (ref, root).
func (c *Client) postPack(ctx context.Context, ref string, root key.Key, body []byte) error {
	q := url.Values{}
	q.Set("ref", ref)
	q.Set("root", root.String())
	_, _, err := c.do(ctx, http.MethodPost, "/v1/objects?"+q.Encode(), "application/octet-stream", body)
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

// FetchObjects downloads the requested objects as a streamed amberpack,
// verifying the trailer signature (over the exact body bytes) against the
// pinned server key before returning. Error responses are buffered and
// header-signed like every other response.
func (c *Client) FetchObjects(ctx context.Context, keys []key.Key) ([]fstree.Object, error) {
	body := keylist.Flatten(keys)
	req, nonce, err := c.signedRequest(ctx, http.MethodPost, "/v1/objects/get", "application/octet-stream", body)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting remote %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if rerr != nil {
			return nil, rerr
		}
		if err := httpsig.VerifyResponse(c.serverPubWire, nonce, resp.StatusCode,
			httpsig.HashBody(respBody), resp.Header.Get(httpsig.HeaderSignature)); err != nil {
			return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
		}
		return nil, &StatusError{Code: resp.StatusCode, Msg: string(respBody)}
	}
	hasher := blake3.New()
	tee := io.TeeReader(resp.Body, hasher)
	var objs []fstree.Object
	for o, err := range amberpack.NewReader(tee).All() {
		if err != nil {
			return nil, fmt.Errorf("reading object stream: %w", err)
		}
		objs = append(objs, o)
	}
	// Drain to EOF so the hash covers the whole body and the trailer arrives.
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return nil, err
	}
	if err := httpsig.VerifyResponse(c.serverPubWire, nonce, http.StatusOK,
		hasher.Sum(nil), resp.Trailer.Get(httpsig.HeaderSignature)); err != nil {
		return nil, fmt.Errorf("server identity mismatch for %s: %w", c.base, err)
	}
	return objs, nil
}
