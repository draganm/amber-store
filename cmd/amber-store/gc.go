package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/socketpath"
	"github.com/urfave/cli/v2"
)

// gcConfig holds the collector flags shared by daemon and serve
// (architecture/simple-gc.md §Configuration).
type gcConfig struct {
	enabled  bool
	interval time.Duration
	grace    time.Duration
	garbage  float64
	minFree  uint64
	rate     int64
}

// gcFlags returns the --gc* flags that fill cfg.
func gcFlags(cfg *gcConfig) []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:        "gc",
			Value:       true,
			Usage:       "run the garbage collector (reference writes gain the completeness walk)",
			Destination: &cfg.enabled,
		},
		&cli.DurationFlag{
			Name:        "gc-interval",
			Value:       time.Hour,
			Usage:       "time between background cycles (0 = only on gc run)",
			Destination: &cfg.interval,
		},
		&cli.DurationFlag{
			Name:        "gc-grace",
			Value:       gc.DefaultGrace,
			Usage:       "minimum age of a sealed pack before it can be reaped; upload-lease idle timeout",
			Destination: &cfg.grace,
		},
		&cli.Float64Flag{
			Name:        "gc-garbage",
			Value:       gc.DefaultGarbage,
			Usage:       "an eligible pack with more garbage than this fraction is reaped",
			Destination: &cfg.garbage,
		},
		&cli.Uint64Flag{
			Name:        "gc-min-free",
			Usage:       "free-space floor in bytes; below it the line drops to 0.1 (0 = 5% of the filesystem)",
			Destination: &cfg.minFree,
		},
		&cli.Int64Flag{
			Name:        "gc-rate",
			Usage:       "reap-copier bandwidth cap in bytes/s (0 = unlimited)",
			Destination: &cfg.rate,
		},
	}
}

// openCollector opens the collector at <storeDir>/closures over the
// already-open store pair, or returns nil with --gc=false. Close it before
// the stores.
func (cfg *gcConfig) openCollector(storeDir string, objects *packstore.Store, refs *refstore.Store, sync bool) (*gc.Collector, error) {
	if !cfg.enabled {
		return nil, nil
	}
	return gc.Open(filepath.Join(storeDir, "closures"), objects, refs, gc.Options{
		Grace:    cfg.grace,
		Garbage:  cfg.garbage,
		MinFree:  cfg.minFree,
		Rate:     cfg.rate,
		Interval: cfg.interval,
		NoSync:   !sync,
	})
}

func gcCommand() *cli.Command {
	var socket string
	var garbage float64
	return &cli.Command{
		Name:  "gc",
		Usage: "garbage collection: score packs, reap the mostly-dead ones",
		Subcommands: []*cli.Command{
			{
				Name:  "status",
				Usage: "packs: id, sealed, bytes, garbage, eligible; totals; closures; union; last cycle",
				Flags: []cli.Flag{socketFlag(&socket)},
				Action: func(c *cli.Context) error {
					if c.NArg() != 0 {
						return fmt.Errorf("gc status takes no arguments, got %d", c.NArg())
					}
					st, err := client.New(socketpath.Resolve(socket)).GCStatus(c.Context)
					if err != nil {
						return err
					}
					w := c.App.Writer
					fmt.Fprintf(w, "%-16s  %-20s  %10s  %7s  %s\n", "PACK", "SEALED", "BYTES", "GARBAGE", "ELIGIBLE")
					for _, p := range st.Packs {
						fmt.Fprintf(w, "%016x  %-20s  %10s  %6.1f%%  %v\n",
							p.ID, p.Sealed.Format(time.RFC3339), humanBytes(uint64(p.Body)), 100*p.Garbage, p.Eligible)
					}
					fmt.Fprintf(w, "live %s, garbage %s; %d refs, %d closures (%d pending), %d live tails\n",
						humanBytes(uint64(st.LiveBytes)), humanBytes(uint64(max(st.GarbageBytes, 0))),
						st.Refs, st.Closures, st.Pending, st.Union)
					if st.Last != nil {
						fmt.Fprintf(w, "last cycle: %s, %d packs scored, %d reaped, %s copied, %s freed\n",
							st.Last.Start.Format(time.RFC3339), st.Last.Scored, len(st.Last.Reaped),
							humanBytes(uint64(st.Last.CopiedBytes)), humanBytes(uint64(max(st.Last.FreedBytes, 0))))
					}
					if st.LastError != "" {
						fmt.Fprintf(w, "last cycle error: %s\n", st.LastError)
					}
					return nil
				},
			},
			{
				Name:  "run",
				Usage: "score now, reap packs above the garbage line",
				Flags: []cli.Flag{
					socketFlag(&socket),
					&cli.Float64Flag{
						Name:        "garbage",
						Usage:       "force the selection line (fraction; default: 0.5, or 0.1 under min-free pressure)",
						Destination: &garbage,
						Value:       -1,
					},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() != 0 {
						return fmt.Errorf("gc run takes no arguments, got %d", c.NArg())
					}
					stats, err := client.New(socketpath.Resolve(socket)).GCRun(c.Context, garbage)
					if err != nil {
						return err
					}
					if stats.Skipped {
						fmt.Fprintln(c.App.Writer, "skipped: nothing changed since the last cycle")
						return nil
					}
					fmt.Fprintf(c.App.Writer, "%d packs scored, %d reaped, %d records (%s) copied, %s freed in %s\n",
						stats.Scored, len(stats.Reaped), stats.CopiedRecords,
						humanBytes(uint64(stats.CopiedBytes)), humanBytes(uint64(max(stats.FreedBytes, 0))),
						stats.Duration.Round(time.Millisecond))
					return nil
				},
			},
			{
				Name:      "why",
				Usage:     "references whose closure holds KEY's tail",
				ArgsUsage: "KEY",
				Flags:     []cli.Flag{socketFlag(&socket)},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return fmt.Errorf("gc why requires exactly one KEY argument, got %d", c.NArg())
					}
					k, err := parseHexKey(c.Args().First())
					if err != nil {
						return err
					}
					names, err := client.New(socketpath.Resolve(socket)).GCWhy(c.Context, k)
					if err != nil {
						return err
					}
					if len(names) == 0 {
						fmt.Fprintln(c.App.Writer, "unreferenced")
						return nil
					}
					for _, n := range names {
						fmt.Fprintln(c.App.Writer, n)
					}
					return nil
				},
			},
		},
	}
}
