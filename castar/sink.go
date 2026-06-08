// Package castar writes content-addressed objects into a tar archive. Each
// object becomes one member named by the hex of its key. Put deduplicates
// (each key written once); PutRoot writes unconditionally so the root object is
// the final member.
package castar

import (
	"archive/tar"
	"io"
	"time"

	"github.com/draganm/amber-store/key"
)

// Sink writes objects to a tar archive with deduplication.
type Sink struct {
	tw   *tar.Writer
	seen map[key.Key]struct{}
}

// NewSink returns a Sink writing tar members to w.
func NewSink(w io.Writer) *Sink {
	return &Sink{tw: tar.NewWriter(w), seen: make(map[key.Key]struct{})}
}

// Put writes the object unless its key has already been written.
func (s *Sink) Put(k key.Key, data []byte) error {
	if _, ok := s.seen[k]; ok {
		return nil
	}
	s.seen[k] = struct{}{}
	return s.write(k, data)
}

// PutRoot writes the object unconditionally (bypassing dedup) so it is the last
// member of the archive.
func (s *Sink) PutRoot(k key.Key, data []byte) error {
	return s.write(k, data)
}

func (s *Sink) write(k key.Key, data []byte) error {
	h := &tar.Header{
		Name:     k.String(),
		Mode:     0o644,
		Size:     int64(len(data)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0).UTC(),
		Format:   tar.FormatUSTAR,
	}
	if err := s.tw.WriteHeader(h); err != nil {
		return err
	}
	_, err := s.tw.Write(data)
	return err
}

// Close flushes and closes the tar archive.
func (s *Sink) Close() error { return s.tw.Close() }
