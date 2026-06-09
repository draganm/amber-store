package main

import (
	"fmt"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/tarextract"
	"github.com/urfave/cli/v2"
)

type restoreConfig struct {
	socket string
}

func restoreCommand() *cli.Command {
	cfg := &restoreConfig{}
	return &cli.Command{
		Name:      "restore",
		Usage:     "restore the filesystem tree rooted at KEY (fetched from the daemon), or at the subdirectory PATH within it, into DIR",
		ArgsUsage: "KEY[/PATH] DIR",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
		},
		Action: func(c *cli.Context) error { return runRestore(c, cfg) },
	}
}

func runRestore(c *cli.Context, cfg *restoreConfig) error {
	if c.NArg() != 2 {
		return fmt.Errorf("restore requires exactly two arguments KEY[/PATH] and DIR, got %d", c.NArg())
	}
	k, path, err := parseKeyPath(c.Args().Get(0))
	if err != nil {
		return err
	}
	outDir := c.Args().Get(1)

	body, err := client.New(socketpath.Resolve(cfg.socket)).Tar(c.Context, k, path)
	if err != nil {
		return err
	}
	defer body.Close()

	return tarextract.Extract(body, outDir)
}
