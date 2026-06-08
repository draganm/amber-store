package main

import (
	"fmt"
	"os"

	"github.com/draganm/amber-store/chunkers"
	"github.com/urfave/cli/v2"
)

// Entry point of the application.
func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "amber-store: %v\n", err)
		os.Exit(1)
	}
}

// Creates and configures the CLI application.
func newApp() *cli.App {
	return &cli.App{
		Name:  "amber-store",
		Usage: "content-addressed filesystem tree store",
		Commands: []*cli.Command{
			{
				Name:      "pack",
				Usage:     "build the content-addressed tree for DIR and write chunks as a tar",
				ArgsUsage: "DIR",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "output tar file (default: stdout)",
					},
					&cli.IntFlag{
						Name:  "min",
						Usage: "ultracdc minimum chunk size in bytes",
					},
					&cli.IntFlag{
						Name:  "avg",
						Usage: "ultracdc average (normal) chunk size in bytes",
					},
					&cli.IntFlag{
						Name:  "max",
						Usage: "ultracdc maximum chunk size in bytes",
					},
					&cli.IntFlag{
						Name:  "item-bits",
						Value: 7,
						Usage: "item chunker average run = 2^bits",
					},
					&cli.IntFlag{
						Name:  "xattr-inline-max",
						Value: 256,
						Usage: "xattrs larger than this many bytes spill to an XattrSet",
					},
				},
				Action: runPack,
			},
		},
	}
}

// Handles the 'pack' command.
func runPack(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("pack requires exactly one DIR argument, got %d", c.NArg())
	}

	dir := c.Args().First()
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	var byteOpts *chunkers.ByteOpts
	min := c.Int("min")
	avg := c.Int("avg")
	max := c.Int("max")
	if min > 0 || avg > 0 || max > 0 {
		if min <= 0 || avg <= 0 || max <= 0 {
			return fmt.Errorf("--min, --avg and --max must all be set together")
		}
		byteOpts = &chunkers.ByteOpts{
			MinSize:    min,
			NormalSize: avg,
			MaxSize:    max,
		}
	}

	var out *os.File
	outputPath := c.String("output")
	if outputPath != "" {
		out, err = os.Create(outputPath)
		if err != nil {
			return err
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	itemBits := c.Int("item-bits")
	xattrInlineMax := c.Int("xattr-inline-max")
	ic := chunkers.NewItemChunker(itemBits)

	root, err := pack(dir, out, ic, byteOpts, xattrInlineMax)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", root.String())
	return nil
}
