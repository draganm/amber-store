package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsMiddlewareCountsByPattern(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := metricsMiddleware(reg, mux)

	srv := httptest.NewServer(h)
	defer srv.Close()
	if _, err := http.Get(srv.URL + "/v1/identity"); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get(srv.URL + "/nope"); err != nil {
		t.Fatal(err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	for _, mf := range mfs {
		out.WriteString(mf.String())
	}
	got := out.String()
	for _, want := range []string{"GET /v1/identity", `value:"200"`, "unmatched", `value:"404"`} {
		if !strings.Contains(got, want) {
			t.Errorf("gathered metrics missing %q\n%s", want, got)
		}
	}
}
