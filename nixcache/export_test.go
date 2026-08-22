package nixcache

import (
	"context"
	"io"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/keylist"
)

// SetMidMark installs a hook running between GC's mark and sweep.
func SetMidMark(n *Node, f func()) { n.midMark = f }

// SyncOnce runs one sync-loop tick: catalog, peers, gated seed pass.
func SyncOnce(n *Node, ctx context.Context) { n.syncOnce(ctx) }

// Probe exposes the peer narinfo probe for protocol tests.
func Probe(src *PeerSource, hashpart string, trusted trustedKeys) (Narinfo, error) {
	return src.probe(context.Background(), hashpart, trusted)
}

// Unindex drops hashpart from the index, leaving its objects in the store.
func Unindex(n *Node, hashpart string) error { return n.publish(nil, []string{hashpart}) }

// StalledObjectsRequest sends an objects request and never reads the
// answer, returning the stream to close when done.
func StalledObjectsRequest(p *PeerSource, keys []key.Key) (io.Closer, error) {
	c, err := p.Swarm.Conn(context.Background(), p.ID)
	if err != nil {
		return nil, err
	}
	s, err := c.OpenStreamSync(context.Background())
	if err != nil {
		return nil, err
	}
	if _, err := s.Write(request{kind: reqObjects, keys: keylist.Flatten(keys)}.encode()); err != nil {
		return nil, err
	}
	return streamCloser{s}, s.Close()
}

type streamCloser struct {
	s interface{ CancelRead(uint64) }
}

func (c streamCloser) Close() error { c.s.CancelRead(0); return nil }

// EncodeRequest exposes the wire encoding for golden tests.
func EncodeRequest(kind byte, keys []byte, root [32]byte, hashpart string) []byte {
	return request{kind: kind, keys: keys, root: root, hashpart: hashpart}.encode()
}

// DecodeRequestRoundTrip decodes b and re-encodes it.
func DecodeRequestRoundTrip(b []byte) ([]byte, error) {
	r, err := decodeRequest(b)
	if err != nil {
		return nil, err
	}
	return r.encode(), nil
}
