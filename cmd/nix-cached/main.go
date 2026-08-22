// Command nix-cached runs a local pull-through Nix binary cache node.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/draganm/amber-store/nixcache"
)

type listFlag []string

func (l *listFlag) String() string     { return fmt.Sprint(*l) }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

func fatalf(code int, format string, a ...any) {
	fmt.Fprintf(os.Stderr, "nix-cached: "+format+"\n", a...)
	os.Exit(code)
}

func main() {
	var (
		trusted  listFlag
		catalogs listFlag
	)
	cfg := nixcache.NodeConfig{}
	listen := flag.String("listen", "127.0.0.1:8321", "substituter listen address")
	flag.StringVar(&cfg.Dir, "dir", "", "node state directory (required)")
	flag.StringVar(&cfg.Upstream, "upstream", "https://cache.nixos.org", "upstream cache URL")
	flag.Var(&trusted, "trusted-key", "trusted narinfo signing key name:base64 (repeatable)")
	flag.Var(&catalogs, "catalog-url", "store-paths list URL (repeatable)")
	flag.DurationVar(&cfg.SyncEvery, "sync-every", time.Hour, "catalog sync interval")
	flag.Int64Var(&cfg.BudgetBytes, "budget-bytes", 0, "refuse ingest above this store size (0: unlimited)")
	flag.Parse()

	if cfg.Dir == "" {
		fmt.Fprintln(os.Stderr, "nix-cached: -dir is required")
		os.Exit(2)
	}
	if len(trusted) == 0 {
		trusted = []string{"cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="}
	}
	cfg.TrustedKeys, cfg.CatalogURLs = trusted, catalogs

	node, err := nixcache.OpenNode(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nix-cached: %v\n", err)
		os.Exit(1)
	}
	defer node.Close()

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nix-cached: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("nix-cached: serving on http://%s\n", l.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := node.Run(ctx, l); err != nil {
		fatalf(1, "%v", err)
	}
}

type durFlag struct{ d *time.Duration }

func (f durFlag) String() string {
	if f.d == nil {
		return ""
	}
	return f.d.String()
}

func (f durFlag) Set(s string) error {
	if d, err := time.ParseDuration(s); err == nil {
		*f.d = d
		return nil
	}
	var n float64
	var unit string
	days := map[string]float64{"d": 1, "w": 7}
	if _, err := fmt.Sscanf(s, "%f%s", &n, &unit); err == nil && days[unit] > 0 {
		*f.d = time.Duration(n * days[unit] * 24 * float64(time.Hour))
		return nil
	}
	return fmt.Errorf("invalid duration %q (try 90s, 5m, 12h, 7d, 2w)", s)
}
