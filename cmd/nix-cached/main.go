// Command nix-cached runs a local pull-through Nix binary cache node.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/draganm/amber-store/nixcache"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/relay"
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
		peers    listFlag
	)
	cfg := nixcache.NodeConfig{}
	listen := flag.String("listen", "127.0.0.1:8321", "substituter listen address")
	flag.Var(&peers, "peer", "peer as <endpointid>@host:port or endpoint ticket (repeatable)")
	p2pPort := flag.Int("p2p-port", 0, fmt.Sprintf("swarm UDP port (default %d when peering)", nixcache.DefaultP2PPort))
	flag.StringVar(&cfg.Dir, "dir", "", "node state directory (required)")
	flag.StringVar(&cfg.Upstream, "upstream", "https://cache.nixos.org", "upstream cache URL")
	flag.Var(&trusted, "trusted-key", "trusted narinfo signing key name:base64 (repeatable)")
	flag.Var(&catalogs, "catalog-url", "store-paths list URL (repeatable)")
	flag.DurationVar(&cfg.SyncEvery, "sync-every", 5*time.Minute, "catalog and peer sync interval")
	flag.DurationVar(&cfg.StallTimeout, "stall-timeout", time.Minute, "abort upstream transfers with no bytes for this long")
	flag.DurationVar(&cfg.CatalogTTL, "catalog-ttl", 0, "drop paths this long after they leave the catalog (0: keep forever)")
	flag.Int64Var(&cfg.BudgetBytes, "budget-bytes", 0, "refuse ingest above this store size (0: unlimited)")
	flag.Int64Var(&cfg.PeerByteRate, "peer-byte-rate", 0, "peer-serving bandwidth cap, bytes/second (0: unlimited)")
	flag.BoolVar(&cfg.Seed, "seed", false, "eagerly ingest every catalogued path")
	flag.Parse()

	if cfg.Dir == "" {
		fmt.Fprintln(os.Stderr, "nix-cached: -dir is required")
		os.Exit(2)
	}
	if len(trusted) == 0 {
		trusted = []string{"cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="}
	}
	cfg.TrustedKeys, cfg.CatalogURLs = trusted, catalogs

	if len(peers) > 0 || *p2pPort != 0 {
		if *p2pPort == 0 {
			*p2pPort = nixcache.DefaultP2PPort
		}
		var err error
		if cfg.Peers, err = nixcache.ParsePeers(peers); err != nil {
			fatalf(2, "%v", err)
		}
		sw, err := nixcache.NewSwarm(context.Background(), nixcache.SwarmOpts{
			KeyPath: filepath.Join(cfg.Dir, "p2p.key"),
			Bind:    netip.AddrPortFrom(netip.IPv6Unspecified(), uint16(*p2pPort)),
			Relay:   relay.ModeDisabled(),
		})
		if err != nil {
			fatalf(1, "%v", err)
		}
		defer sw.Close()
		cfg.Swarm = sw
		fmt.Printf("nix-cached: swarm endpoint id=%s\n", sw.ID())
		fmt.Printf("nix-cached: swarm ticket=%s\n", endpointticket.Encode(sw.Addr()))
	}

	node, err := nixcache.OpenNode(cfg)
	if err != nil {
		fatalf(1, "%v", err)
	}
	defer node.Close()

	l, err := net.Listen("tcp", *listen)
	if err != nil && !strings.Contains(*listen, ":") {
		err = fmt.Errorf("invalid --listen %q: want host:port, e.g. 127.0.0.1:8321", *listen)
	}
	if err != nil {
		fatalf(1, "%v", err)
	}
	fmt.Printf("nix-cached: serving on http://%s\n", l.Addr())

	sock := filepath.Join(cfg.Dir, "admin.sock")
	os.Remove(sock)
	al, err := net.Listen("unix", sock)
	if err != nil {
		fatalf(1, "%v", err)
	}
	if err := os.Chmod(sock, 0o660); err != nil {
		fatalf(1, "%v", err)
	}
	defer al.Close()
	go http.Serve(al, node.AdminHandler())

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
