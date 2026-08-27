package remotesync_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/remotesync"
	"golang.org/x/sync/errgroup"
)

// memSource serves a chunked tree from memory, counting the bytes that
// cross the wire.
type memSource struct {
	keys  []key.Key
	objs  map[key.Key][]byte
	wire  atomic.Int64
	total int64 // payload bytes of the whole tree
}

func newMemSource(b *testing.B, size int) (*memSource, key.Key) {
	b.Helper()
	s := &memSource{objs: map[key.Key][]byte{}}
	emit := func(o fstree.Object) error {
		if _, ok := s.objs[o.Key]; !ok {
			s.objs[o.Key] = o.Bytes
			s.keys = append(s.keys, o.Key)
			s.total += int64(len(o.Bytes))
		}
		return nil
	}
	content := make([]byte, size)
	rand.Read(content)
	ib := fstree.NewFileIndexBuilder(chunkers.NewItemChunker(7))
	err := chunkers.SplitBytes(bytes.NewReader(content), nil, func(c []byte) error {
		o, err := fstree.EncodeBlob(c)
		if err != nil {
			return err
		}
		if err := emit(o); err != nil {
			return err
		}
		return ib.AddChild(emit, o.Key, nil)
	})
	if err != nil {
		b.Fatal(err)
	}
	root, err := ib.Finish(emit)
	if err != nil {
		b.Fatal(err)
	}
	return s, root
}

func (s *memSource) ReachableKeys(ctx context.Context, root key.Key) ([]key.Key, error) {
	return s.keys, nil
}

func (s *memSource) FetchObjects(ctx context.Context, keys []key.Key, onBytes func(int)) ([]fstree.Object, error) {
	var objs []fstree.Object
	for _, k := range keys {
		objs = append(objs, fstree.Object{Key: k, Bytes: s.objs[k]})
		s.wire.Add(int64(len(s.objs[k])))
	}
	return objs, nil
}

// BenchmarkConcurrentOverlappingPulls: four pulls of the same tree into
// one store. wire/size reports duplicate downloads: 1.0 means the
// in-flight registry made the overlap travel once; without it the four
// pulls would approach 4.0.
func BenchmarkConcurrentOverlappingPulls(b *testing.B) {
	const pullers = 4
	src, root := newMemSource(b, 64<<20)
	local, err := packstore.Open(b.TempDir(), packstore.WithSync(false))
	if err != nil {
		b.Fatal(err)
	}
	defer local.Close()
	b.SetBytes(64 << 20)
	var wire, runs int64
	for b.Loop() {
		src.wire.Store(0)
		var eg errgroup.Group
		for range pullers {
			eg.Go(func() error {
				_, err := remotesync.Pull(context.Background(), local, src, root, remotesync.Opts{})
				return err
			})
		}
		if err := eg.Wait(); err != nil {
			b.Fatal(err)
		}
		wire += src.wire.Load()
		runs++
		b.StopTimer()
		if err := local.Wipe(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(wire)/float64(runs)/float64(src.total), "wire/size")
}
