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
			daemonCommand(),
			ingestCommand(),
			loadCommand(),
			dumpCommand(),
			restoreCommand(),
			lsCommand(),
			contentKeysCommand(),
		},
	}
}

// chunkConfig holds the content-defined-chunking parameters used by the ingest
// command when building the tree.
type chunkConfig struct {
	min            int
	avg            int
	max            int
	itemBits       int
	xattrInlineMax int
}

// byteOpts returns the ultracdc byte-chunker options, or nil for the library
// defaults. min/avg/max must all be set together or all left unset.
func (cc *chunkConfig) byteOpts() (*chunkers.ByteOpts, error) {
	if cc.min == 0 && cc.avg == 0 && cc.max == 0 {
		return nil, nil
	}
	if cc.min <= 0 || cc.avg <= 0 || cc.max <= 0 {
		return nil, fmt.Errorf("--min, --avg and --max must all be set together")
	}
	return &chunkers.ByteOpts{MinSize: cc.min, NormalSize: cc.avg, MaxSize: cc.max}, nil
}

// itemChunker builds the item chunker from the configured bit width.
func (cc *chunkConfig) itemChunker() chunkers.ItemChunker {
	return chunkers.NewItemChunker(cc.itemBits)
}

// chunkFlags returns the CLI flags that fill cc.
func chunkFlags(cc *chunkConfig) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "min",
			Usage:       "ultracdc minimum chunk size in bytes",
			Destination: &cc.min,
			Value:       32 << 10,
		},
		&cli.IntFlag{
			Name:        "avg",
			Usage:       "ultracdc average (normal) chunk size in bytes",
			Destination: &cc.avg,
			Value:       128 << 10,
		},
		&cli.IntFlag{
			Name:        "max",
			Usage:       "ultracdc maximum chunk size in bytes",
			Destination: &cc.max,
			Value:       256 << 10,
		},
		&cli.IntFlag{
			Name:        "item-bits",
			Value:       7,
			Usage:       "item chunker average run = 2^bits",
			Destination: &cc.itemBits,
		},
		&cli.IntFlag{
			Name:        "xattr-inline-max",
			Value:       256,
			Usage:       "xattrs larger than this many bytes spill to an XattrSet",
			Destination: &cc.xattrInlineMax,
		},
	}
}

// dirArg validates that the command received exactly one argument naming an
// existing directory, and returns it. cmd names the command for error messages.
func dirArg(c *cli.Context, cmd string) (string, error) {
	if c.NArg() != 1 {
		return "", fmt.Errorf("%s requires exactly one DIR argument, got %d", cmd, c.NArg())
	}
	dir := c.Args().First()
	info, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", dir)
	}
	return dir, nil
}
