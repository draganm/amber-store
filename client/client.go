// Package client talks to the amber-store daemon over a unix socket using
// HTTP/1.1. The same calls work against a TCP daemon by changing the dialer.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/draganm/amber-store/key"
)

// Stats mirrors the daemon's POST /v1/objects response.
type Stats struct {
	ObjectsStored  int   `json:"objects_stored"`
	ObjectsDeduped int   `json:"objects_deduped"`
	BytesStored    int64 `json:"bytes_stored"`
}

// Client issues requests to a daemon listening on a unix socket.
type Client struct {
	hc       *http.Client
	sockPath string
}

// New returns a Client dialing the unix socket at sockPath.
func New(sockPath string) *Client {
	return &Client{
		sockPath: sockPath,
		hc: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
				},
			},
		},
	}
}

// baseURL is a fixed authority; the host is ignored for unix sockets.
const baseURL = "http://amber"

// dialHint augments a connection error with a reminder to start the daemon.
func (c *Client) dialHint(err error) error {
	return fmt.Errorf("contacting daemon at %s: %w (is the daemon running? try: amber-store daemon --store DIR)", c.sockPath, err)
}

// Ingest uploads a pack-write stream and returns the daemon's store stats.
func (c *Client) Ingest(ctx context.Context, body io.Reader) (Stats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/objects", body)
	if err != nil {
		return Stats{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.hc.Do(req)
	if err != nil {
		return Stats{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return Stats{}, fmt.Errorf("ingest failed: %s: %s", resp.Status, msg)
	}
	var s Stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Stats{}, fmt.Errorf("decoding ingest response: %w", err)
	}
	return s, nil
}

// Tar requests the directory tar for k. The caller must close the returned
// reader. A non-2xx status is drained and returned as an error.
func (c *Client) Tar(ctx context.Context, k key.Key) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/tar/"+k.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		resp.Body.Close()
		return nil, fmt.Errorf("tar request failed: %s: %s", resp.Status, msg)
	}
	return resp.Body, nil
}
