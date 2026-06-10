package main

import (
	"fmt"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

type contentKeysConfig struct {
	socket string
}

func contentKeysCommand() *cli.Command {
	cfg := &contentKeysConfig{}
	return &cli.Command{
		Name:      "content-keys",
		Usage:     "list the keys of every object reachable from KEY (fetched from the daemon), or from the subdirectory PATH within it — the set needed to fetch the whole content; accepts a reference as ref:NAME[@PATH]",
		ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
		},
		Action: func(c *cli.Context) error { return runContentKeys(c, cfg) },
	}
}

func runContentKeys(c *cli.Context, cfg *contentKeysConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("content-keys requires exactly one KEY[/PATH] argument, got %d", c.NArg())
	}
	cl := client.New(socketpath.Resolve(cfg.socket))
	k, path, err := resolveSpec(c.Context, cl, c.Args().First())
	if err != nil {
		return err
	}
	keys, err := cl.ContentKeys(c.Context, k, path)
	if err != nil {
		return err
	}
	for _, line := range keys {
		if _, err := fmt.Fprintln(c.App.Writer, line); err != nil {
			return err
		}
	}
	return nil
}
