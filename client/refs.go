package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/draganm/amber-store/reference"
)

// ErrRefNotFound reports an absent reference name.
var ErrRefNotFound = errors.New("reference not found")

// refsURL builds the /v1/refs URL, with the name as a query parameter (names
// may contain '/', '..' and other path-hostile characters).
func refsURL(name string) string {
	u := baseURL + "/v1/refs"
	if name != "" {
		u += "?name=" + url.QueryEscape(name)
	}
	return u
}

// PutRef creates or overwrites the reference rec under rec.Name.
func (c *Client) PutRef(ctx context.Context, rec reference.Reference) error {
	body, err := rec.Encode()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, refsURL(rec.Name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/cbor")
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("put-ref failed: %s: %s", resp.Status, msg)
	}
	return nil
}

// GetRef fetches and decodes the reference stored under name.
func (c *Client) GetRef(ctx context.Context, name string) (reference.Reference, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refsURL(name), nil)
	if err != nil {
		return reference.Reference{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return reference.Reference{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return reference.Reference{}, fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return reference.Reference{}, fmt.Errorf("get-ref failed: %s: %s", resp.Status, msg)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("reading get-ref response: %w", err)
	}
	return reference.Decode(body)
}

// RefInfo mirrors one NDJSON line of the daemon's GET /v1/refs listing.
type RefInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"` // RFC 3339
	Signed    bool   `json:"signed"`
}

// ListRefs returns every reference, in name order.
func (c *Client) ListRefs(ctx context.Context) ([]RefInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refsURL(""), nil)
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
		return nil, fmt.Errorf("list-refs failed: %s: %s", resp.Status, msg)
	}
	var infos []RefInfo
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 64<<10)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var info RefInfo
		if err := json.Unmarshal(line, &info); err != nil {
			return nil, fmt.Errorf("decoding list-refs response: %w", err)
		}
		infos = append(infos, info)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading list-refs response: %w", err)
	}
	return infos, nil
}

// DeleteRef removes the reference stored under name.
func (c *Client) DeleteRef(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, refsURL(name), nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("delete-ref failed: %s: %s", resp.Status, msg)
	}
	return nil
}
