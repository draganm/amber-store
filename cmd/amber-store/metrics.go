package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/draganm/amber-store/gc"
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

// gcCollector exports the garbage collector's cheap figures (gc.Counters —
// nothing that scores packs) as amber_gc_* gauges, read at scrape time.
type gcCollector struct {
	coll *gc.Collector

	refs, closures, pending, union, leases      *prometheus.Desc
	cycleStart, cycleDuration                   *prometheus.Desc
	cycleScored, cycleReaped                    *prometheus.Desc
	cycleCopiedBytes, cycleFreedBytes, cycleErr *prometheus.Desc
}

// registerGCMetrics wires the amber_gc_* gauges for coll into reg.
func registerGCMetrics(reg prometheus.Registerer, coll *gc.Collector) {
	g := &gcCollector{
		coll:     coll,
		refs:     prometheus.NewDesc("amber_gc_refs", "Reference names.", nil, nil),
		closures: prometheus.NewDesc("amber_gc_closures", "Closure files on disk.", nil, nil),
		pending:  prometheus.NewDesc("amber_gc_pending_roots", "Named roots without a valid closure yet.", nil, nil),
		union:    prometheus.NewDesc("amber_gc_union_tails", "Live tails in the union.", nil, nil),
		leases:   prometheus.NewDesc("amber_gc_upload_leases", "Live upload leases.", nil, nil),
		cycleStart: prometheus.NewDesc("amber_gc_last_cycle_start_timestamp_seconds",
			"Start of the last completed cycle as a Unix timestamp.", nil, nil),
		cycleDuration: prometheus.NewDesc("amber_gc_last_cycle_duration_seconds",
			"Duration of the last completed cycle.", nil, nil),
		cycleScored: prometheus.NewDesc("amber_gc_last_cycle_scored_packs",
			"Packs scored by the last completed cycle.", nil, nil),
		cycleReaped: prometheus.NewDesc("amber_gc_last_cycle_reaped_packs",
			"Packs reaped by the last completed cycle.", nil, nil),
		cycleCopiedBytes: prometheus.NewDesc("amber_gc_last_cycle_copied_bytes",
			"Live record bytes the last completed cycle copied forward.", nil, nil),
		cycleFreedBytes: prometheus.NewDesc("amber_gc_last_cycle_freed_bytes",
			"Net bytes the last completed cycle freed.", nil, nil),
		cycleErr: prometheus.NewDesc("amber_gc_last_cycle_error",
			"1 when the last cycle recorded an error, else 0.", nil, nil),
	}
	reg.MustRegister(g)
}

func (g *gcCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		g.refs, g.closures, g.pending, g.union, g.leases,
		g.cycleStart, g.cycleDuration, g.cycleScored, g.cycleReaped,
		g.cycleCopiedBytes, g.cycleFreedBytes, g.cycleErr,
	} {
		ch <- d
	}
}

func (g *gcCollector) Collect(ch chan<- prometheus.Metric) {
	ct := g.coll.Counters()
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	gauge(g.refs, float64(ct.Refs))
	gauge(g.closures, float64(ct.Closures))
	gauge(g.pending, float64(ct.Pending))
	gauge(g.union, float64(ct.Union))
	gauge(g.leases, float64(ct.Leases))
	errv := 0.0
	if ct.LastErr != "" {
		errv = 1
	}
	gauge(g.cycleErr, errv)
	if ct.Last == nil {
		return
	}
	gauge(g.cycleStart, float64(ct.Last.Start.UnixNano())/1e9)
	gauge(g.cycleDuration, ct.Last.Duration.Seconds())
	gauge(g.cycleScored, float64(ct.Last.Scored))
	gauge(g.cycleReaped, float64(len(ct.Last.Reaped)))
	gauge(g.cycleCopiedBytes, float64(ct.Last.CopiedBytes))
	gauge(g.cycleFreedBytes, float64(ct.Last.FreedBytes))
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
