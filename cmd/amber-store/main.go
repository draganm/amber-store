package main

import (
	"fmt"
	"os"

	"github.com/draganm/amber-store/chunkers"
	"github.com/urfave/cli/v2"
)

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "amber-store:", err)
		os.Exit(1)
	}
}

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
					&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Usage: "output tar file (default: stdout)"},
					&cli.IntFlag{Name: "min", Usage: "ultracdc minimum chunk size in bytes"},
					&cli.IntFlag{Name: "avg", Usage: "ultracdc average (normal) chunk size in bytes"},
					&cli.IntFlag{Name: "max", Usage: "ultracdc maximum chunk size in bytes"},
					&cli.IntFlag{Name: "item-bits", Value: 7, Usage: "item chunker average run = 2^bits"},
					&cli.IntFlag{Name: "xattr-inline-max", Value: 256, Usage: "xattrs larger than this many bytes spill to an XattrSet"},
				},
				Action: runPack,
			},
		},
	}
}

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
	if c.Int("min") > 0 || c.Int("avg") > 0 || c.Int("max") > 0 {
		if c.Int("min") <= 0 || c.Int("avg") <= 0 || c.Int("max") <= 0 {
			return fmt.Errorf("--min, --avg and --max must all be set together")
		}
		byteOpts = &chunkers.ByteOpts{
			MinSize:    c.Int("min"),
			NormalSize: c.Int("avg"),
			MaxSize:    c.Int("max"),
		}
	}

	var out *os.File
	if path := c.String("output"); path != "" {
		out, err = os.Create(path)
		if err != nil {
			return err
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	ic := chunkers.NewItemChunker(c.Int("item-bits"))
	root, err := pack(dir, out, ic, byteOpts, c.Int("xattr-inline-max"))
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, root.String())
	return nil
}
