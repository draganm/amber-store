package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/key"
)

// ErrCycleRunning reports that the daemon already has a GC cycle in flight;
// cycles never overlap.
var ErrCycleRunning = errors.New("a gc cycle is already running")

// gcError drains resp and wraps its status and message.
func gcError(op string, resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	return fmt.Errorf("%s failed: %s: %s", op, resp.Status, msg)
}

// GCStatus reports per-pack liveness against a fresh advisory mark, totals,
// and the last cycle. The daemon walks every live tree to answer, so on a
// big store this call is slow by design.
func (c *Client) GCStatus(ctx context.Context) (gc.Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/gc/status", nil)
	if err != nil {
		return gc.Status{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return gc.Status{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return gc.Status{}, gcError("gc-status", resp)
	}
	var st gc.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return gc.Status{}, fmt.Errorf("decoding gc status: %w", err)
	}
	return st, nil
}

// GCRun runs one collection cycle on the daemon and returns its stats.
// garbage >= 0 forces the selection line; garbage < 0 uses the daemon's
// policy (0.5, or 0.1 under min-free pressure). The call is synchronous —
// it returns when the cycle is done. An overlapping cycle surfaces as
// ErrCycleRunning.
func (c *Client) GCRun(ctx context.Context, garbage float64) (gc.CycleStats, error) {
	u := baseURL + "/v1/gc/run"
	if garbage >= 0 {
		u += "?garbage=" + url.QueryEscape(strconv.FormatFloat(garbage, 'f', -1, 64))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return gc.CycleStats{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return gc.CycleStats{}, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return gc.CycleStats{}, ErrCycleRunning
	}
	if resp.StatusCode != http.StatusOK {
		return gc.CycleStats{}, gcError("gc-run", resp)
	}
	var stats gc.CycleStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return gc.CycleStats{}, fmt.Errorf("decoding gc stats: %w", err)
	}
	return stats, nil
}

// GCWhy lists the references whose tree reaches k — why the object is
// alive. An empty list means unreferenced: a future cycle may reap it.
func (c *Client) GCWhy(ctx context.Context, k key.Key) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/gc/why?key="+k.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.dialHint(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, gcError("gc-why", resp)
	}
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, fmt.Errorf("decoding gc why: %w", err)
	}
	return names, nil
}
