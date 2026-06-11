package remoteclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ErrRefNotFound reports an absent reference name on the server.
var ErrRefNotFound = errors.New("reference not found on the remote")

func refsPath(name string) string {
	p := "/v1/refs"
	if name != "" {
		p += "?name=" + url.QueryEscape(name)
	}
	return p
}

// PutRef uploads a canonical (signed) reference record verbatim.
func (c *Client) PutRef(ctx context.Context, name string, record []byte) error {
	_, _, err := c.do(ctx, http.MethodPut, refsPath(name), "application/cbor", record)
	return err
}

// GetRef fetches the raw canonical record stored under name.
func (c *Client) GetRef(ctx context.Context, name string) ([]byte, error) {
	_, body, err := c.do(ctx, http.MethodGet, refsPath(name), "", nil)
	var se *StatusError
	if errors.As(err, &se) && se.Code == http.StatusNotFound {
		return nil, fmt.Errorf("reference %q: %w", name, ErrRefNotFound)
	}
	if err != nil {
		return nil, err
	}
	return body, nil
}

// RefInfo mirrors one NDJSON line of the server's listing.
type RefInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signed    bool   `json:"signed"`
}

// ListRefs returns every reference on the server, in name order.
func (c *Client) ListRefs(ctx context.Context) ([]RefInfo, error) {
	_, body, err := c.do(ctx, http.MethodGet, refsPath(""), "", nil)
	if err != nil {
		return nil, err
	}
	var infos []RefInfo
	dec := json.NewDecoder(strings.NewReader(string(body)))
	for {
		var info RefInfo
		if err := dec.Decode(&info); err == io.EOF {
			return infos, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding ref listing: %w", err)
		}
		infos = append(infos, info)
	}
}
