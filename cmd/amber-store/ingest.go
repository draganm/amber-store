package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/internal/userconfig"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/urfave/cli/v2"
)

// errIngestStopped aborts the tree walk when the consumer stops pulling from
// the object iterator early. It never escapes ingestObjects.
var errIngestStopped = errors.New("ingest: consumer stopped")

// ingestObjects returns an iterator over every CAS object in the tree rooted at
// dir, yielding fstree.Objects for serialization into a pack-write stream. The
// tree is built concurrently by up to jobs workers (file reads, content-defined
// chunking and hashing run in parallel across sibling files and subdirectories);
// built objects stream to the consumer through a buffered channel, so production
// and serialization overlap. Object order is unspecified — the store is a flat
// content-addressed bag and dedups by key — but the resolved tree is identical
// to the sequential walk: per-file chunk order and per-directory entry order are
// preserved, so every object's key, and the root, are deterministic.
//
// Once the walk completes successfully the resolved root key is written to
// *root; a build error is yielded to the consumer instead.
func ingestObjects(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, p *Progress, root *key.Key) iter.Seq2[fstree.Object, error] {
	if jobs < 1 {
		jobs = 1
	}
	return func(yield func(fstree.Object, error) bool) {
		// ctx is cancelled only when the consumer stops pulling early (yield
		// returns false). Build errors propagate through the normal return
		// path instead, so they are never masked by stop signals.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan fstree.Object, jobs*2)
		var buildErr error

		go func() {
			defer close(ch)
			emit := func(o fstree.Object) error {
				select {
				case ch <- o:
					return nil
				case <-ctx.Done():
					return errIngestStopped
				}
			}
			b := &pbuilder{
				d:    &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax, p: p},
				emit: emit,
				sem:  make(chan struct{}, jobs),
			}
			rk, err := b.buildDir(dir, emit)
			if err != nil {
				// errIngestStopped means the consumer quit; not a real error.
				if !errors.Is(err, errIngestStopped) {
					buildErr = err
				}
				return
			}
			*root = rk
		}()

		for o := range ch {
			if !yield(o, nil) {
				cancel() // unblock producers parked on the channel send
				for range ch {
				}
				return
			}
		}
		if buildErr != nil {
			yield(fstree.Object{}, buildErr)
		}
	}
}

// pbuilder builds the CAS tree concurrently. It reuses the driver's per-entry
// and per-file logic but fans the directory walk out across a bounded pool: each
// directory entry's subtree (a file's chunks or a subdirectory) is built
// independently, then the directory's own leaf/index objects are assembled in
// the original sorted-entry order. emit is the (concurrency-safe) sink shared by
// all workers.
type pbuilder struct {
	d    *driver
	emit fstree.Emit
	// sem bounds the number of in-flight worker goroutines. Offloading uses a
	// non-blocking send: when the pool is full, the work runs inline on the
	// current goroutine, so a parent never blocks waiting for a slot held by
	// one of its own descendants — the recursion cannot deadlock.
	sem chan struct{}
}

// buildDir builds the directory at path and returns its root key. Sibling
// entries are built concurrently; the directory's leaf/index objects are then
// emitted in sorted-entry order, identical to the sequential walk.
func (b *pbuilder) buildDir(path string, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(path) // sorted bytewise by name
	if err != nil {
		return key.Key{}, err
	}

	entries := make([]fstree.Entry, len(ents))
	errs := make([]error, len(ents))
	var wg sync.WaitGroup

	for i, de := range ents {
		full := filepath.Join(path, de.Name())
		name := de.Name()
		build := func(i int, full, name string) {
			entries[i], errs[i] = b.d.buildEntry(full, name, b.emit, b.buildDir)
		}
		select {
		case b.sem <- struct{}{}:
			wg.Add(1)
			go func(i int, full, name string) {
				defer wg.Done()
				defer func() { <-b.sem }()
				build(i, full, name)
			}(i, full, name)
		default:
			build(i, full, name)
		}
	}
	wg.Wait()

	db := fstree.NewDirBuilder(b.d.ic)
	for i, e := range entries {
		if errs[i] != nil {
			return key.Key{}, errs[i]
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

type ingestConfig struct {
	chunk      chunkConfig
	socket     string
	output     string
	jobs       int
	noProgress bool
}

func ingestCommand() *cli.Command {
	cfg := &ingestConfig{}
	flags := append(chunkFlags(&cfg.chunk),
		&cli.StringFlag{
			Name:        "socket",
			Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
			Destination: &cfg.socket,
		},
		&cli.StringFlag{
			Name:        "output",
			Aliases:     []string{"o"},
			Usage:       "write the pack to FILE instead of streaming to the daemon",
			Destination: &cfg.output,
		},
		&cli.IntFlag{
			Name:        "jobs",
			Aliases:     []string{"j"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "concurrent workers building the tree (default: number of CPUs)",
			Destination: &cfg.jobs,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &cfg.noProgress,
		},
	)
	return &cli.Command{
		Name:      "ingest",
		Usage:     "build the content-addressed tree for DIR, store it via the daemon under reference NAME (or write a pack file with --output, no NAME)",
		ArgsUsage: "NAME DIR  (with --output: DIR)",
		Flags:     flags,
		Action:    func(c *cli.Context) error { return runIngest(c, cfg) },
	}
}

// writePack builds the tree at dir and serializes every object into dst as a
// pack-write stream, returning the resolved root key.
func writePack(dst io.Writer, dir string, cc *chunkConfig, jobs int, p *Progress) (key.Key, error) {
	byteOpts, err := cc.byteOpts()
	if err != nil {
		return key.Key{}, err
	}
	pw := amberpack.NewWriter(dst)
	var root key.Key
	for o, err := range ingestObjects(dir, cc.itemChunker(), byteOpts, cc.xattrInlineMax, jobs, p, &root) {
		if err != nil {
			return key.Key{}, err
		}
		if err := pw.Add(o); err != nil {
			return key.Key{}, err
		}
	}
	if err := pw.Close(); err != nil {
		return key.Key{}, err
	}
	return root, nil
}

// shellQuote renders s as a single POSIX-shell word, so a copy-pasteable
// command hint stays correct for names containing spaces, '$', or backticks.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// Handles the 'ingest' command.
func runIngest(c *cli.Context, cfg *ingestConfig) error {
	var refName, dir, user string
	if cfg.output != "" {
		d, err := dirArg(c, "ingest")
		if err != nil {
			return err
		}
		dir = d
	} else {
		if c.NArg() != 2 {
			return fmt.Errorf("ingest requires NAME DIR arguments, got %d", c.NArg())
		}
		refName = c.Args().Get(0)
		dir = c.Args().Get(1)
		if err := reference.ValidateName(refName); err != nil {
			return err
		}
		if err := checkDir(dir); err != nil {
			// Make the failing argument's role explicit: path-like strings are
			// valid reference names, so a swapped `ingest DIR NAME` otherwise
			// fails with a bare stat error on the name.
			return fmt.Errorf("DIR argument: %w", err)
		}
		ucfg, err := userconfig.Load()
		if err != nil {
			return err
		}
		user = ucfg.User
	}

	// Progress (client-side) is sized by a cheap pre-scan, unless disabled.
	var prog *Progress
	var pwg sync.WaitGroup
	ctx, cancel := context.WithCancel(c.Context)
	// LIFO: cancel() runs first to stop the progress goroutine, then pwg.Wait()
	// drains it — so every return path (including early errors) tears it down.
	defer pwg.Wait()
	defer cancel()
	if !cfg.noProgress {
		totalFiles, totalBytes, err := scanTree(dir, cfg.jobs)
		if err != nil {
			return err
		}
		prog = NewProgress(totalFiles, totalBytes)
		isTTY := isTerminal(os.Stderr)
		start := time.Now()
		pwg.Go(func() { prog.Run(ctx, os.Stderr, start, isTTY) })
	}

	var root key.Key
	if cfg.output != "" {
		// Offline: write the pack to a file; do not contact the daemon.
		f, err := os.Create(cfg.output)
		if err != nil {
			return err
		}
		root, err = writePack(f, dir, &cfg.chunk, cfg.jobs, prog)
		if err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	} else {
		// Stream the pack to the daemon: build into a pipe consumed as the request
		// body, capturing the build's root and error out-of-band.
		pr, pw := io.Pipe()
		type result struct {
			root key.Key
			err  error
		}
		resCh := make(chan result, 1)
		go func() {
			r, err := writePack(pw, dir, &cfg.chunk, cfg.jobs, prog)
			if err != nil {
				pw.CloseWithError(err)
			} else {
				pw.Close()
			}
			resCh <- result{r, err}
		}()
		cl := client.New(socketpath.Resolve(cfg.socket))
		_, ingErr := cl.Ingest(ctx, pr)
		res := <-resCh
		// A closed-pipe build error is a secondary effect: the transport closes
		// the body pipe when the daemon replies early (an error status), so the
		// request error is the cause and the pipe error just noise. Any other
		// build error is the true cause (it aborted the request via
		// CloseWithError), so it wins.
		if res.err != nil && !errors.Is(res.err, io.ErrClosedPipe) {
			return res.err
		}
		if ingErr != nil {
			return ingErr
		}
		if res.err != nil {
			return res.err
		}
		root = res.root

		// Create the reference. Use c.Context, not the progress ctx, which is
		// cancelled right after this branch.
		rec := reference.Reference{
			Name:      refName,
			Key:       root[:],
			User:      user,
			CreatedAt: time.Now().UnixNano(),
		}
		if err := cl.PutRef(c.Context, rec); err != nil {
			return fmt.Errorf("tree stored (root %s) but creating reference %q failed: %w\nretry with: amber-store ref create %s %s",
				root, refName, err, shellQuote(refName), root)
		}
	}

	cancel()
	pwg.Wait()
	fmt.Fprintf(c.App.Writer, "%s\n", root.String())
	return nil
}
