// Command nix-cached runs a local pull-through Nix binary cache node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
		peers    listFlag
		relays   listFlag
	)
	cfg := nixcache.NodeConfig{}
	listen := flag.String("listen", "127.0.0.1:8321", "substituter listen address")
	flag.Var(&peers, "peer", "peer as <endpointid>@host:port or endpoint ticket (repeatable)")
	flag.Var(&relays, "relay", "relay URL replacing the default n0 relays (repeatable)")
	noRelay := flag.Bool("no-relay", false, "do not use relays, direct UDP only")
	serveRelay := flag.String("serve-relay", "", "run an iroh relay for the swarm on this TCP listen address, e.g. :3340")
	relayURL := flag.String("relay-url", "", "external URL of --serve-relay (default http://<interface addr>:<port>)")
	relayCert := flag.String("relay-cert", "", "PEM certificate for --serve-relay; serves HTTPS and QUIC address discovery on the same port")
	relayKey := flag.String("relay-key", "", "PEM key for --relay-cert")
	relayCA := flag.String("relay-ca", "", "PEM CA bundle to trust for relays in addition to the system roots")
	p2pPort := flag.Int("p2p-port", 0, fmt.Sprintf("swarm UDP port (default %d when peering)", nixcache.DefaultP2PPort))
	flag.StringVar(&cfg.Dir, "dir", "", "node state directory (required)")
	flag.StringVar(&cfg.Upstream, "upstream", "https://cache.nixos.org", "upstream cache URL")
	flag.Var(&trusted, "trusted-key", "trusted narinfo signing key name:base64 (repeatable)")
	flag.Var(&catalogs, "catalog-url", "store-paths list URL (repeatable)")
	cfg.SyncEvery = 5 * time.Minute
	cfg.StallTimeout = time.Minute
	flag.Var(durFlag{&cfg.SyncEvery}, "sync-every", "catalog and peer sync interval (default 5m)")
	flag.Var(durFlag{&cfg.StallTimeout}, "stall-timeout", "abort upstream transfers with no bytes for this long (default 1m)")
	flag.Var(durFlag{&cfg.CatalogTTL}, "catalog-ttl", "drop paths this long after they leave the catalog (0: keep forever)")
	flag.Int64Var(&cfg.BudgetBytes, "budget-bytes", 0, "total NAR-size budget, evicts to fit (0: unlimited)")
	flag.Int64Var(&cfg.PeerByteRate, "peer-byte-rate", 0, "peer-serving bandwidth cap, bytes/second (0: unlimited)")
	flag.BoolVar(&cfg.Seed, "seed", false, "eagerly ingest every catalogued path")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of nix-cached:")
		var b strings.Builder
		flag.CommandLine.SetOutput(&b)
		flag.PrintDefaults()
		flag.CommandLine.SetOutput(os.Stderr)
		fmt.Fprint(os.Stderr, strings.ReplaceAll("\n"+b.String(), "\n  -", "\n  --")[1:])
	}
	flag.Parse()

	if cfg.Dir == "" {
		fatalf(2, "--dir is required")
	}
	if len(trusted) == 0 {
		trusted = []string{"cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="}
	}
	cfg.TrustedKeys, cfg.CatalogURLs = trusted, catalogs
	cfg.Upstream = strings.TrimRight(cfg.Upstream, "/")
	for _, u := range append([]string{cfg.Upstream}, catalogs...) {
		if p, err := url.Parse(u); err != nil || p.Scheme != "http" && p.Scheme != "https" {
			fatalf(2, "%q is not an http(s) URL", u)
		}
	}
	if cfg.SyncEvery <= 0 {
		fatalf(2, "--sync-every must be positive")
	}
	if cfg.Seed && len(catalogs) == 0 {
		fatalf(2, "--seed needs at least one --catalog-url")
	}
	if *serveRelay == "" && (*relayURL != "" || *relayCert != "") {
		fatalf(2, "--relay-url and --relay-cert need --serve-relay")
	}
	if (*relayCert == "") != (*relayKey == "") {
		fatalf(2, "--relay-cert and --relay-key go together")
	}
	if *serveRelay != "" && len(peers) == 0 && *p2pPort == 0 {
		*p2pPort = nixcache.DefaultP2PPort
	}
	if len(peers) > 0 || *p2pPort != 0 {
		if *p2pPort == 0 {
			*p2pPort = nixcache.DefaultP2PPort
		}
		var err error
		if cfg.Peers, err = nixcache.ParsePeers(peers); err != nil {
			fatalf(2, "%v", err)
		}
		if *serveRelay != "" {
			rh, err := nixcache.ServeRelay(nixcache.RelayOpts{Listen: *serveRelay, ExternalURL: *relayURL, CertFile: *relayCert, KeyFile: *relayKey})
			if err != nil {
				fatalf(1, "serve relay: %v", err)
			}
			defer rh.Close()
			cfg.RelayHost = rh
			slog.Info("serving relay", "listen", *serveRelay, "url", rh.URL())
		}
		relayMode, err := nixcache.RelayMode(relays, *noRelay, cfg.RelayHost)
		if err != nil {
			fatalf(2, "%v", err)
		}
		cfg.Peers = nixcache.WithSwarmRelays(cfg.Peers, relays, cfg.RelayHost)
		sw, err := nixcache.NewSwarm(context.Background(), nixcache.SwarmOpts{
			KeyPath: filepath.Join(cfg.Dir, "p2p.key"),
			Bind:    netip.AddrPortFrom(netip.IPv6Unspecified(), uint16(*p2pPort)),
			Relay:   relayMode,
			RelayCA: *relayCA,
		})
		if err != nil {
			fatalf(1, "%v", err)
		}
		defer sw.Close()
		cfg.Swarm = sw
		slog.Info("swarm endpoint", "id", sw.ID(), "udp_port", *p2pPort)
		sw.LogAddr()
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
