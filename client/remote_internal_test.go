package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// dialAll returns a Client whose every request is dialed to addr regardless of
// the request URL host (which runSync hardcodes to "amber").
func dialAll(t *testing.T, addr string) *Client {
	t.Helper()
	return &Client{hc: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		},
	}}}
}

func TestRemotePullMapsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "reference not found", http.StatusNotFound)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	c := dialAll(t, u.Host)

	_, _, err := c.RemotePull(context.Background(), "origin", "missing", 0, 0, nil)
	if !errors.Is(err, ErrRemoteRefNotFound) {
		t.Fatalf("RemotePull err = %v, want errors.Is ErrRemoteRefNotFound", err)
	}
}

func TestRemotePullNon404StaysGeneric(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	c := dialAll(t, u.Host)

	_, _, err := c.RemotePull(context.Background(), "origin", "x", 0, 0, nil)
	if err == nil || errors.Is(err, ErrRemoteRefNotFound) {
		t.Fatalf("RemotePull err = %v, want a non-nil error that is NOT ErrRemoteRefNotFound", err)
	}
}
