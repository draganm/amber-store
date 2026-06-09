package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/diskstore"
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
			packCommand(),
			ingestCommand(),
			restoreCommand(),
		},
	}
}

// chunkConfig holds the content-defined-chunking parameters shared by every
// command that builds the tree (pack, ingest).
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

// packConfig is the fully-typed configuration for the pack command.
type packConfig struct {
	chunk  chunkConfig
	output string
}

// packCommand builds the pack command, wiring its flags into a packConfig.
func packCommand() *cli.Command {
	cfg := &packConfig{}
	flags := append(chunkFlags(&cfg.chunk),
		&cli.StringFlag{
			Name:        "output",
			Aliases:     []string{"o"},
			Usage:       "output tar file (default: stdout)",
			Destination: &cfg.output,
		},
	)
	return &cli.Command{
		Name:      "pack",
		Usage:     "build the content-addressed tree for DIR and write chunks as a tar",
		ArgsUsage: "DIR",
		Flags:     flags,
		Action:    func(c *cli.Context) error { return runPack(c, cfg) },
	}
}

// ingestConfig is the fully-typed configuration for the ingest command.
type ingestConfig struct {
	chunk           chunkConfig
	store           string
	inlineThreshold int
	sync            bool
	jobs            int
	writers         int
	noProgress      bool
}

// ingestCommand builds the ingest command, wiring its flags into an ingestConfig.
func ingestCommand() *cli.Command {
	cfg := &ingestConfig{}
	flags := append(chunkFlags(&cfg.chunk),
		&cli.StringFlag{
			Name:        "store",
			Aliases:     []string{"s"},
			Usage:       "diskstore directory (created if missing)",
			Required:    true,
			Destination: &cfg.store,
		},
		&cli.IntFlag{
			Name:        "inline-threshold",
			Value:       diskstore.DefaultInlineThreshold,
			Usage:       "objects larger than this many bytes are stored as external blob files",
			Destination: &cfg.inlineThreshold,
		},
		&cli.BoolFlag{
			Name:        "sync",
			Value:       true,
			Usage:       "fsync writes for crash durability (disable to speed bulk loads)",
			Destination: &cfg.sync,
		},
		&cli.IntFlag{
			Name:        "jobs",
			Aliases:     []string{"j"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "number of concurrent workers building the tree (default: number of CPUs)",
			Destination: &cfg.jobs,
		},
		&cli.IntFlag{
			Name:        "writers",
			Aliases:     []string{"w"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "number of concurrent batch writers committing to the store (default: number of CPUs)",
			Destination: &cfg.writers,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &cfg.noProgress,
		},
	)
	return &cli.Command{
		Name:      "ingest",
		Usage:     "build the content-addressed tree for DIR and store its objects in a diskstore",
		ArgsUsage: "DIR",
		Flags:     flags,
		Action:    func(c *cli.Context) error { return runIngest(c, cfg) },
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

// Handles the 'pack' command.
func runPack(c *cli.Context, cfg *packConfig) error {
	dir, err := dirArg(c, "pack")
	if err != nil {
		return err
	}

	byteOpts, err := cfg.chunk.byteOpts()
	if err != nil {
		return err
	}

	var out *os.File
	if cfg.output != "" {
		out, err = os.Create(cfg.output)
		if err != nil {
			return err
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	root, err := pack(dir, out, cfg.chunk.itemChunker(), byteOpts, cfg.chunk.xattrInlineMax)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", root.String())
	return nil
}
