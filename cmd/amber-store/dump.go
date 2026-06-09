package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/key"
	"github.com/urfave/cli/v2"
)

type dumpConfig struct {
	socket string
	output string
}

func dumpCommand() *cli.Command {
	cfg := &dumpConfig{}
	return &cli.Command{
		Name:      "dump",
		Usage:     "fetch the directory tar for KEY from the daemon",
		ArgsUsage: "KEY",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "output tar file (default: stdout)",
				Destination: &cfg.output,
			},
		},
		Action: func(c *cli.Context) error { return runDump(c, cfg) },
	}
}

func runDump(c *cli.Context, cfg *dumpConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("dump requires exactly one KEY argument, got %d", c.NArg())
	}
	k, err := parseHexKey(c.Args().First())
	if err != nil {
		return err
	}

	body, err := client.New(socketpath.Resolve(cfg.socket)).Tar(c.Context, k)
	if err != nil {
		return err
	}
	defer body.Close()

	if cfg.output != "" {
		f, err := os.Create(cfg.output)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, body); err != nil {
			f.Close()
			return err
		}
		return f.Close() // surface a flush/close error on the happy path
	}
	_, err = io.Copy(os.Stdout, body)
	return err
}

// parseHexKey decodes a lowercase-hex key argument into a validated key.
func parseHexKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	k, err := key.Parse(raw)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid key %q: %w", s, err)
	}
	return k, nil
}
