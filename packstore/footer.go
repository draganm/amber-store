package packstore

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"

	"github.com/draganm/amber-store/key"
)

const (
	fanoutSize     = 256 * 4 // 256 cumulative u32 counts on the key's last byte
	indexEntrySize = 32 + 8 + 4
	trailerSize    = 64
)

// indexEntry is one sealed-segment index row: a key and where its record
// starts, plus the stored payload length as a readahead hint.
type indexEntry struct {
	k    key.Key
	off  uint64 // file offset of the record header
	slen uint32
}

// compareEntries orders by (last key byte, full key): the fanout is on the
// last byte because byte 0 is type/length-size and clusters, while the hash
// tail is uniformly distributed.
func compareEntries(a, b indexEntry) int {
	if c := cmp.Compare(a.k[key.Size-1], b.k[key.Size-1]); c != 0 {
		return c
	}
	return bytes.Compare(a.k[:], b.k[:])
}

// buildIndexSection serializes the index section (fanout + sorted entries).
// It does not mutate entries.
func buildIndexSection(entries []indexEntry) []byte {
	es := slices.Clone(entries)
	slices.SortFunc(es, compareEntries)

	out := make([]byte, fanoutSize+len(es)*indexEntrySize)
	var counts [256]uint32
	for _, e := range es {
		counts[e.k[key.Size-1]]++
	}
	cum := uint32(0)
	for b := 0; b < 256; b++ {
		cum += counts[b]
		binary.BigEndian.PutUint32(out[b*4:], cum)
	}
	off := fanoutSize
	for _, e := range es {
		copy(out[off:off+32], e.k[:])
		binary.BigEndian.PutUint64(out[off+32:], e.off)
		binary.BigEndian.PutUint32(out[off+40:], e.slen)
		off += indexEntrySize
	}
	return out
}

// parseIndexSection splits an index section into a decoded fanout table and
// the raw entry bytes, validating lengths and fanout monotonicity.
func parseIndexSection(b []byte, keyCount uint64) (*[256]uint32, []byte, error) {
	want := fanoutSize + int(keyCount)*indexEntrySize
	if len(b) != want {
		return nil, nil, fmt.Errorf("%w: index section is %d bytes, want %d", ErrCorrupt, len(b), want)
	}
	var fanout [256]uint32
	prev := uint32(0)
	for i := 0; i < 256; i++ {
		fanout[i] = binary.BigEndian.Uint32(b[i*4:])
		if fanout[i] < prev {
			return nil, nil, fmt.Errorf("%w: fanout not monotonic at byte %#x", ErrCorrupt, i)
		}
		prev = fanout[i]
	}
	if uint64(fanout[255]) != keyCount {
		return nil, nil, fmt.Errorf("%w: fanout total %d != key count %d", ErrCorrupt, fanout[255], keyCount)
	}
	return &fanout, b[fanoutSize:], nil
}

// searchIndex finds k in a parsed index section: fanout bucket on the last
// byte, then binary search on the full key within the bucket.
func searchIndex(fanout *[256]uint32, entries []byte, k key.Key) (off uint64, slen uint32, ok bool) {
	b := k[key.Size-1]
	lo := uint32(0)
	if b > 0 {
		lo = fanout[b-1]
	}
	n := int(fanout[b] - lo)
	i := sort.Search(n, func(i int) bool {
		e := entries[(int(lo)+i)*indexEntrySize:]
		return bytes.Compare(e[:32], k[:]) >= 0
	})
	if i >= n {
		return 0, 0, false
	}
	e := entries[(int(lo)+i)*indexEntrySize:]
	if !bytes.Equal(e[:32], k[:]) {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(e[32:40]), binary.BigEndian.Uint32(e[40:44]), true
}
