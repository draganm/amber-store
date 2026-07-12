package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newDebugRegistry builds the process metrics registry: Go runtime + process
// collectors plus a constant service marker.
func newDebugRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "amber_build_info",
		Help:        "Constant 1; identifies the service on this debug listener.",
		ConstLabels: prometheus.Labels{"service": "amber-store"},
	})
	info.Set(1)
	reg.MustRegister(info)
	return reg
}

// statusRecorder captures the response code for the metrics middleware while
// staying transparent to streaming handlers: Unwrap lets
// http.ResponseController reach Flush/deadline controls on the real writer.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// metricsMiddleware instruments every request with count + duration, labeled
// by the ServeMux pattern that matched (bounded cardinality — never the raw
// path). The mux sets r.Pattern during matching, so it is read AFTER next
// returns; an empty pattern (404/405) is labeled "unmatched".
func metricsMiddleware(reg prometheus.Registerer, next http.Handler) http.Handler {
	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "amber_http_requests_total", Help: "HTTP requests by mux pattern, method and status code.",
	}, []string{"route", "method", "code"})
	dur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "amber_http_request_duration_seconds", Help: "HTTP request duration by mux pattern and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})
	reg.MustRegister(reqs, dur)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		reqs.WithLabelValues(route, r.Method, strconv.Itoa(rec.code)).Inc()
		dur.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}

// startDebugServer binds addr and serves /metrics + /debug/pprof/* until ctx
// is cancelled. Empty addr = disabled (nil, nil). A bind failure is returned —
// callers treat it as fatal: a crash-loop is visible, a silently missing
// metrics port is not. pprof handlers are mounted explicitly on a dedicated
// mux; net/http/pprof's DefaultServeMux side effect is unused.
func startDebugServer(ctx context.Context, addr string, reg *prometheus.Registry, logger *slog.Logger) (net.Addr, error) {
	if addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Handler: mux, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError)}
	go func() {
		<-ctx.Done()
		sd, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sd)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("debug listener failed", "addr", addr, "err", err)
		}
	}()
	return ln.Addr(), nil
}
