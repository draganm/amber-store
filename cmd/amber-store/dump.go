package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

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
		Usage:     "fetch the directory tar for KEY from the daemon, or for the subdirectory PATH within it; accepts a reference as ref:NAME[@PATH]",
		ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
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
		return fmt.Errorf("dump requires exactly one KEY[/PATH] argument, got %d", c.NArg())
	}
	cl := client.New(socketpath.Resolve(cfg.socket))
	k, path, err := resolveSpec(c.Context, cl, c.Args().First())
	if err != nil {
		return err
	}

	body, err := cl.Tar(c.Context, k, path)
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

// parseKeyPath splits a KEY[/PATH] argument at the first slash and decodes the
// key part. The returned path is empty when no slash follows the key.
func parseKeyPath(s string) (key.Key, string, error) {
	keyPart, path, _ := strings.Cut(s, "/")
	k, err := parseHexKey(keyPart)
	if err != nil {
		return key.Key{}, "", err
	}
	return k, path, nil
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
