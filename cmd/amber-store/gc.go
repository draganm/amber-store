package main

import (
	"fmt"
	"time"

	"github.com/urfave/cli/v2"
)

func gcCommand() *cli.Command {
	return &cli.Command{
		Name:  "gc",
		Usage: "garbage collection: score packs, reap the mostly-dead ones",
		Subcommands: []*cli.Command{
			gcStatusCommand(),
			gcRunCommand(),
			gcWhyCommand(),
		},
	}
}

func gcStatusCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:  "status",
		Usage: "per-pack scores against a fresh mark, totals, the last cycle (walks every live tree)",
		Flags: []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("gc status takes no arguments, got %d", c.NArg())
			}
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			st, err := cl.GCStatus(c.Context)
			if err != nil {
				return err
			}
			w := c.App.Writer
			fmt.Fprintf(w, "%-16s  %-20s  %10s  %7s  %s\n", "PACK", "SEALED", "BYTES", "GARBAGE", "ELIGIBLE")
			for _, p := range st.Packs {
				fmt.Fprintf(w, "%016x  %-20s  %10s  %6.1f%%  %v\n",
					p.ID, p.Sealed.Format(time.RFC3339), humanBytes(uint64(p.Body)), 100*p.Garbage, p.Eligible)
			}
			fmt.Fprintf(w, "live %s, garbage %s; %d refs, %d live objects marked\n",
				humanBytes(uint64(st.LiveBytes)), humanBytes(uint64(max(st.GarbageBytes, 0))),
				st.Refs, st.Marked)
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
	}
}

func gcRunCommand() *cli.Command {
	var socket string
	var garbage float64
	return &cli.Command{
		Name:  "run",
		Usage: "run one cycle now: mark from every reference, reap packs above the garbage line",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.Float64Flag{
				Name:        "garbage",
				Usage:       "force the selection line (fraction; default: the daemon's policy — 0.5, or 0.1 under min-free pressure)",
				Destination: &garbage,
				Value:       -1,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("gc run takes no arguments, got %d", c.NArg())
			}
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			stats, err := cl.GCRun(c.Context, garbage)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%d packs scored, %d reaped, %d records (%s) copied, %s freed in %s (mark %s, sweep %s; %d objects marked)\n",
				stats.Scored, len(stats.Reaped), stats.CopiedRecords,
				humanBytes(uint64(stats.CopiedBytes)), humanBytes(uint64(max(stats.FreedBytes, 0))), stats.Duration.Round(time.Millisecond),
				stats.MarkDuration.Round(time.Millisecond), stats.SweepDuration.Round(time.Millisecond), stats.Marked)
			return nil
		},
	}
}

func gcWhyCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "why",
		Usage:     "list the references whose tree reaches KEY — why it is alive",
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
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			names, err := cl.GCWhy(c.Context, k)
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
	}
}
