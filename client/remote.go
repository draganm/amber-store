package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// PreflightInfo is the daemon's report on a prospective remote's identity.
type PreflightInfo struct {
	PublicKey   []byte // SSH wire format
	Fingerprint string
	KeyType     string
}

// RemotePreflight asks the daemon to fetch a server's identity for
// fingerprint confirmation.
func (c *Client) RemotePreflight(ctx context.Context, remoteURL string) (PreflightInfo, error) {
	u := baseURL + "/v1/remotes/preflight?url=" + url.QueryEscape(remoteURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return PreflightInfo{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return PreflightInfo{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return PreflightInfo{}, fmt.Errorf("preflight failed: %s: %s", resp.Status, msg)
	}
	var body struct {
		PublicKey   string `json:"public_key"`
		Fingerprint string `json:"fingerprint"`
		KeyType     string `json:"key_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return PreflightInfo{}, fmt.Errorf("decoding preflight response: %w", err)
	}
	pubWire, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil {
		return PreflightInfo{}, err
	}
	return PreflightInfo{PublicKey: pubWire, Fingerprint: body.Fingerprint, KeyType: body.KeyType}, nil
}

// RemoteAdd persists a confirmed remote on the daemon.
func (c *Client) RemoteAdd(ctx context.Context, name, remoteURL string, publicKey []byte) error {
	u := baseURL + "/v1/remotes?name=" + url.QueryEscape(name) + "&url=" + url.QueryEscape(remoteURL)
	body, err := json.Marshal(map[string]string{
		"public_key": base64.StdEncoding.EncodeToString(publicKey),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("remote add failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// RemoteRemove unregisters a remote.
func (c *Client) RemoteRemove(ctx context.Context, name string) error {
	u := baseURL + "/v1/remotes?name=" + url.QueryEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("remote rm failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// RemoteInfo is one registered remote.
type RemoteInfo struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
}

// RemoteList returns the registered remotes in name order.
func (c *Client) RemoteList(ctx context.Context) ([]RemoteInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/remotes", nil)
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
		return nil, fmt.Errorf("remote ls failed: %s: %s", resp.Status, msg)
	}
	var infos []RemoteInfo
	dec := json.NewDecoder(resp.Body)
	for {
		var info RemoteInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding remote listing: %w", err)
		}
		infos = append(infos, info)
	}
}

// PushStats / PullStats mirror the daemon's final sync events.
type PushStats struct {
	ObjectsTotal  int   `json:"objects_total"`
	ObjectsPushed int   `json:"objects_pushed"`
	BytesPushed   int64 `json:"bytes_pushed"`
}

type PullStats struct {
	ObjectsFetched int   `json:"objects_fetched"`
	BytesFetched   int64 `json:"bytes_fetched"`
}

// syncEvent mirrors the daemon's NDJSON sync event lines.
type syncEvent struct {
	Done      int        `json:"done"`
	Total     int        `json:"total"`
	PushStats *PushStats `json:"push_stats"`
	PullStats *PullStats `json:"pull_stats"`
	Key       string     `json:"key"`
	Error     string     `json:"error"`
}

// runSync POSTs a sync route and consumes its NDJSON event stream, invoking
// onProgress per progress line and returning the final event.
func (c *Client) runSync(ctx context.Context, pathQuery string, onProgress func(done, total int)) (*syncEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+pathQuery, nil)
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
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: %s", ErrRemoteRefNotFound, bytes.TrimSpace(msg))
		}
		return nil, fmt.Errorf("sync failed: %s: %s", resp.Status, msg)
	}
	var last *syncEvent
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var ev syncEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			return nil, fmt.Errorf("decoding sync event: %w", err)
		}
		if ev.Error != "" {
			return nil, fmt.Errorf("sync failed: %s", ev.Error)
		}
		if ev.PushStats != nil || ev.PullStats != nil {
			e := ev
			last = &e
			continue
		}
		if onProgress != nil {
			onProgress(ev.Done, ev.Total)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if last == nil {
		return nil, fmt.Errorf("sync stream ended without a final event (connection cut?)")
	}
	return last, nil
}

// syncQuery builds the common query string for sync routes.
func syncQuery(remote, name string, jobs int, batchBytes uint64) string {
	q := url.Values{}
	if remote != "" {
		q.Set("remote", remote)
	}
	q.Set("name", name)
	if jobs > 0 {
		q.Set("jobs", fmt.Sprint(jobs))
	}
	if batchBytes > 0 {
		q.Set("batch_bytes", fmt.Sprint(batchBytes))
	}
	return "?" + q.Encode()
}

// RemotePush pushes the objects reachable from local ref name AND the signed
// reference to the remote — one streamed operation.
func (c *Client) RemotePush(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PushStats, error) {
	ev, err := c.runSync(ctx, "/v1/remote/push"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PushStats{}, err
	}
	if ev.PushStats == nil {
		return PushStats{}, fmt.Errorf("sync stream ended without push stats")
	}
	return *ev.PushStats, nil
}

// RemotePull fetches the reference name from the remote AND every object it
// reaches into the local store — one streamed operation; returns the resolved
// root key (hex).
func (c *Client) RemotePull(ctx context.Context, remote, name string, jobs int, batchBytes uint64, onProgress func(done, total int)) (PullStats, string, error) {
	ev, err := c.runSync(ctx, "/v1/remote/pull"+syncQuery(remote, name, jobs, batchBytes), onProgress)
	if err != nil {
		return PullStats{}, "", err
	}
	if ev.PullStats == nil {
		return PullStats{}, "", fmt.Errorf("sync stream ended without pull stats")
	}
	return *ev.PullStats, ev.Key, nil
}

// RemoteRefInfo mirrors the remote listing lines (same shape as RefInfo).
type RemoteRefInfo = RefInfo

// RemoteLsRefs lists the references on the remote.
func (c *Client) RemoteLsRefs(ctx context.Context, remote string) ([]RemoteRefInfo, error) {
	q := url.Values{}
	if remote != "" {
		q.Set("remote", remote)
	}
	u := baseURL + "/v1/remote/refs"
	if len(q) > 0 {
		u += "?" + q.Encode()
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
		return nil, fmt.Errorf("remote ls-refs failed: %s: %s", resp.Status, msg)
	}
	var infos []RemoteRefInfo
	dec := json.NewDecoder(resp.Body)
	for {
		var info RemoteRefInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding remote ref listing: %w", err)
		}
		infos = append(infos, info)
	}
}
