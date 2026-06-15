package remoteclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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

// Stats mirrors the server's upload response.
type Stats struct {
	Stored      int   `json:"objects_stored"`
	Deduped     int   `json:"objects_deduped"`
	BytesStored int64 `json:"bytes_stored"`
}

// PushPack uploads objs as one amberpack stream; the server verifies every
// payload against its key before storing.
func (c *Client) PushPack(ctx context.Context, objs []fstree.Object) (Stats, error) {
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			return Stats{}, err
		}
	}
	if err := pw.Close(); err != nil {
		return Stats{}, err
	}
	_, body, err := c.do(ctx, http.MethodPost, "/v1/objects", "application/octet-stream", buf.Bytes())
	if err != nil {
		return Stats{}, err
	}
	var s Stats
	if err := json.Unmarshal(body, &s); err != nil {
		return Stats{}, fmt.Errorf("decoding upload response: %w", err)
	}
	return s, nil
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
