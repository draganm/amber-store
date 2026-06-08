package chunkers

import (
	"io"

	gocdc "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc" // register "ultracdc"
)

// ByteOpts configures the ultracdc byte chunker (re-exported so callers need not
// import the upstream package).
type ByteOpts = gocdc.ChunkerOpts

// SplitBytes runs the ultracdc content-defined chunker over r and calls fn once
// per chunk, in order. Each chunk passed to fn is a fresh copy, so fn may retain
// it (the underlying chunker reuses its read buffer between calls). A nil opts
// uses ultracdc's default sizes (min 2 KiB, normal 10 KiB, max 64 KiB). An empty
// reader yields zero chunks.
func SplitBytes(r io.Reader, opts *ByteOpts, fn func(chunk []byte) error) error {
	c, err := gocdc.NewChunker("ultracdc", r, opts)
	if err != nil {
		return err
	}
	for {
		chunk, err := c.Next()
		if len(chunk) > 0 {
			cp := make([]byte, len(chunk))
			copy(cp, chunk)
			if ferr := fn(cp); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
