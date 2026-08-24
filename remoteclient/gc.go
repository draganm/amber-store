package remoteclient

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
)

// The server's gc routes (signed): status and why need the read capability,
// run needs admin.

// GCStatus fetches the server's gc report: per-pack scores, totals,
// closures, the last cycle.
func (c *Client) GCStatus(ctx context.Context) (gc.Status, error) {
	var st gc.Status
	_, body, err := c.do(ctx, http.MethodGet, "/v1/gc", "", nil)
	if err != nil {
		return st, err
	}
	err = json.Unmarshal(body, &st)
	return st, err
}

// GCRun runs one cycle on the server now. garbage >= 0 forces the selection
// line; negative means policy (0.5, or 0.1 under min-free pressure).
func (c *Client) GCRun(ctx context.Context, garbage float64) (gc.CycleStats, error) {
	p := "/v1/gc/run"
	if garbage >= 0 {
		p += "?garbage=" + url.QueryEscape(strconv.FormatFloat(garbage, 'g', -1, 64))
	}
	var stats gc.CycleStats
	_, body, err := c.do(ctx, http.MethodPost, p, "", nil)
	if err != nil {
		return stats, err
	}
	err = json.Unmarshal(body, &stats)
	return stats, err
}

// GCWhy returns the sorted names of the references whose closure holds k's
// tail — why the object is alive on the server. Empty: unreferenced.
func (c *Client) GCWhy(ctx context.Context, k key.Key) ([]string, error) {
	_, body, err := c.do(ctx, http.MethodGet, "/v1/gc/why/"+hex.EncodeToString(k[:]), "", nil)
	if err != nil {
		return nil, err
	}
	var names []string
	err = json.Unmarshal(body, &names)
	return names, err
}
