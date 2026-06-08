package main

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sync"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/urfave/cli/v2"
)

// errIngestStopped aborts the tree walk when the diskstore consumer stops
// pulling from the object iterator early. It never escapes ingestObjects.
var errIngestStopped = errors.New("ingest: consumer stopped")

// ingestObjects returns an iterator over every CAS object in the tree rooted at
// dir, shaped for diskstore.WriteBatch. The tree is built concurrently by up to
// jobs workers (file reads, content-defined chunking and hashing run in
// parallel across sibling files and subdirectories); built objects stream to
// the consumer through a buffered channel, so production and storage overlap.
// Object order is unspecified — diskstore is a flat content-addressed store and
// dedups by key — but the resolved tree is identical to the sequential pack
// walk: per-file chunk order and per-directory entry order are preserved, so
// every object's key, and the root, are deterministic.
//
// Once the walk completes successfully the resolved root key is written to
// *root; a build error is yielded to the consumer instead.
func ingestObjects(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, jobs int, root *key.Key) iter.Seq2[diskstore.Object, error] {
	if jobs < 1 {
		jobs = 1
	}
	return func(yield func(diskstore.Object, error) bool) {
		// ctx is cancelled only when the consumer stops pulling early (yield
		// returns false). Build errors propagate through the normal return
		// path instead, so they are never masked by stop signals.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan diskstore.Object, jobs*2)
		var buildErr error

		go func() {
			defer close(ch)
			emit := func(o fstree.Object) error {
				select {
				case ch <- diskstore.Object{Key: o.Key, Data: o.Bytes}:
					return nil
				case <-ctx.Done():
					return errIngestStopped
				}
			}
			b := &pbuilder{
				d:    &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax},
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
			yield(diskstore.Object{}, buildErr)
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

// Handles the 'ingest' command.
func runIngest(c *cli.Context, cfg *ingestConfig) error {
	dir, err := dirArg(c, "ingest")
	if err != nil {
		return err
	}

	byteOpts, err := cfg.chunk.byteOpts()
	if err != nil {
		return err
	}

	store, err := diskstore.Open(cfg.store,
		diskstore.WithInlineThreshold(cfg.inlineThreshold),
		diskstore.WithSync(cfg.sync),
	)
	if err != nil {
		return err
	}
	defer store.Close()

	var root key.Key
	seq := ingestObjects(dir, cfg.chunk.itemChunker(), byteOpts, cfg.chunk.xattrInlineMax, cfg.jobs, &root)
	if err := store.WriteBatch(seq); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", root.String())
	return nil
}
