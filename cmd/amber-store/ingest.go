package main

import (
	"errors"
	"fmt"
	"iter"
	"os"

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
// dir, in children-before-parents order, shaped for diskstore.WriteBatch. The
// tree is walked lazily as the consumer pulls. Once the walk completes
// successfully, the resolved root key is written to *root; a build error is
// yielded to the consumer instead.
func ingestObjects(dir string, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int, root *key.Key) iter.Seq2[diskstore.Object, error] {
	return func(yield func(diskstore.Object, error) bool) {
		d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}
		emit := func(o fstree.Object) error {
			if !yield(diskstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return errIngestStopped
			}
			return nil
		}
		rk, err := d.buildDir(dir, emit)
		if err != nil {
			if errors.Is(err, errIngestStopped) {
				return
			}
			yield(diskstore.Object{}, err)
			return
		}
		*root = rk
	}
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
	seq := ingestObjects(dir, cfg.chunk.itemChunker(), byteOpts, cfg.chunk.xattrInlineMax, &root)
	if err := store.WriteBatch(seq); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", root.String())
	return nil
}
