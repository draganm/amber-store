package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

type loadConfig struct {
	socket string
}

func loadCommand() *cli.Command {
	cfg := &loadConfig{}
	return &cli.Command{
		Name:      "load",
		Usage:     "upload a prebuilt pack file to the daemon",
		ArgsUsage: "FILE",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
		},
		Action: func(c *cli.Context) error { return runLoad(c, cfg) },
	}
}

func runLoad(c *cli.Context, cfg *loadConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("load requires exactly one FILE argument, got %d", c.NArg())
	}
	f, err := os.Open(c.Args().First())
	if err != nil {
		return err
	}
	defer f.Close()

	cl, err := daemonClient(cfg.socket)
	if err != nil {
		return err
	}
	stats, err := cl.Ingest(c.Context, f)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "stored %d objects (%d deduped, %d bytes)\n",
		stats.ObjectsStored, stats.ObjectsDeduped, stats.BytesStored)
	return nil
}
