package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
)

// gcDo issues one gc request and decodes the JSON response into out.
func (c *Client) gcDo(ctx context.Context, method, pathQuery string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, baseURL+pathQuery, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("gc request failed: %s: %s", resp.Status, msg)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// GCStatus fetches the daemon's gc report: per-pack scores, totals,
// closures, the last cycle.
func (c *Client) GCStatus(ctx context.Context) (gc.Status, error) {
	var st gc.Status
	err := c.gcDo(ctx, http.MethodGet, "/v1/gc", &st)
	return st, err
}

// GCRun runs one cycle now. garbage >= 0 forces the selection line;
// negative means policy (0.5, or 0.1 under min-free pressure).
func (c *Client) GCRun(ctx context.Context, garbage float64) (gc.CycleStats, error) {
	p := "/v1/gc/run"
	if garbage >= 0 {
		p += "?garbage=" + url.QueryEscape(strconv.FormatFloat(garbage, 'g', -1, 64))
	}
	var stats gc.CycleStats
	err := c.gcDo(ctx, http.MethodPost, p, &stats)
	return stats, err
}

// GCWhy returns the sorted names of the references whose closure holds k's
// tail — why the object is alive. Empty: unreferenced.
func (c *Client) GCWhy(ctx context.Context, k key.Key) ([]string, error) {
	var names []string
	err := c.gcDo(ctx, http.MethodGet, "/v1/gc/why/"+hex.EncodeToString(k[:]), &names)
	return names, err
}
