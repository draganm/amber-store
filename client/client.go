// Package client talks to the amber-store daemon over a unix socket using
// HTTP/1.1. The same calls work against a TCP daemon by changing the dialer.
package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"

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

// Entry mirrors one NDJSON line of the daemon's GET /v1/ls/{key} response.
// Size is the logical content length for regular files and directories and the
// target length for symlinks. Key is the hex content key of regular-file and
// directory entries (usable for further Ls or Tar calls).
type Entry struct {
	Name       string   `json:"name"`
	Mode       uint64   `json:"mode"`
	UID        uint64   `json:"uid"`
	GID        uint64   `json:"gid"`
	MtimeNs    int64    `json:"mtime_ns"`
	Size       uint64   `json:"size"`
	Key        string   `json:"key,omitempty"`
	LinkTarget string   `json:"link_target,omitempty"`
	Rdev       []uint64 `json:"rdev,omitempty"`
}

// Ls fetches the entries of the directory object k, in name order. A non-empty
// path names a subdirectory of k (slash-separated) to list instead of k itself.
func (c *Client) Ls(ctx context.Context, k key.Key, path string) ([]Entry, error) {
	u := baseURL + "/v1/ls/" + k.String()
	if path != "" {
		u += "?path=" + url.QueryEscape(path)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("ls request failed: %s: %s", resp.Status, msg)
	}
	var entries []Entry
	dec := json.NewDecoder(resp.Body)
	for {
		var e Entry
		if err := dec.Decode(&e); err == io.EOF {
			return entries, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding ls response: %w", err)
		}
		entries = append(entries, e)
	}
}

// ContentKeys returns the hex keys of every object reachable from k — the set
// that must be fetched to hold the whole content — root first, each key once.
// A non-empty path names a subdirectory of k (slash-separated) to walk instead
// of k itself.
func (c *Client) ContentKeys(ctx context.Context, k key.Key, path string) ([]string, error) {
	u := baseURL + "/v1/content-keys/" + k.String()
	if path != "" {
		u += "?path=" + url.QueryEscape(path)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("content-keys request failed: %s: %s", resp.Status, msg)
	}
	var keys []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			keys = append(keys, line)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading content-keys response: %w", err)
	}
	return keys, nil
}

// Tar requests the directory tar for k. A non-empty path names a subdirectory
// of k (slash-separated) to tar instead of k itself. The caller must close the
// returned reader. A non-2xx status is drained and returned as an error.
func (c *Client) Tar(ctx context.Context, k key.Key, path string) (io.ReadCloser, error) {
	u := baseURL + "/v1/tar/" + k.String()
	if path != "" {
		u += "?path=" + url.QueryEscape(path)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
