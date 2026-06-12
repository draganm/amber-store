package packstore

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/FastFilter/xorfilter"
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
// It does not mutate entries. Callers must pass entries with distinct keys
// (the write path dedups; with duplicate keys the relative order of their
// rows is unspecified).
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
	if keyCount > math.MaxUint32 {
		return nil, nil, fmt.Errorf("%w: key count %d exceeds format limit", ErrCorrupt, keyCount)
	}
	want := uint64(fanoutSize) + keyCount*indexEntrySize
	if uint64(len(b)) != want {
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

const (
	filterHeaderSize       = 29
	filterTypeBinaryFuse16 = 1
)

// filterKey is the filter input for k: the last 8 bytes of the key, which lie
// in the uniformly distributed truncated-hash region.
func filterKey(k key.Key) uint64 {
	return binary.BigEndian.Uint64(k[key.Size-8:])
}

// buildFilterSection builds and serializes a binary fuse filter over the
// entries' keys. Duplicate 8-byte tails are deduplicated before the build.
func buildFilterSection(entries []indexEntry) ([]byte, error) {
	tails := make([]uint64, 0, len(entries))
	for _, e := range entries {
		tails = append(tails, filterKey(e.k))
	}
	slices.Sort(tails)
	tails = slices.Compact(tails)
	f, err := xorfilter.NewBinaryFuse[uint16](tails)
	if err != nil {
		return nil, fmt.Errorf("packstore: building fuse filter: %w", err)
	}
	out := make([]byte, filterHeaderSize+2*len(f.Fingerprints))
	out[0] = filterTypeBinaryFuse16
	binary.BigEndian.PutUint64(out[1:9], f.Seed)
	binary.BigEndian.PutUint32(out[9:13], f.SegmentLength)
	binary.BigEndian.PutUint32(out[13:17], f.SegmentLengthMask)
	binary.BigEndian.PutUint32(out[17:21], f.SegmentCount)
	binary.BigEndian.PutUint32(out[21:25], f.SegmentCountLength)
	binary.BigEndian.PutUint32(out[25:29], uint32(len(f.Fingerprints)))
	for i, fp := range f.Fingerprints {
		binary.BigEndian.PutUint16(out[filterHeaderSize+2*i:], fp)
	}
	return out, nil
}

// parseFilterSection deserializes a filter section, copying fingerprints out
// of b (which may be a read-only mmap) into RAM.
func parseFilterSection(b []byte) (*xorfilter.BinaryFuse[uint16], error) {
	if len(b) < filterHeaderSize {
		return nil, fmt.Errorf("%w: filter section too short: %d bytes", ErrCorrupt, len(b))
	}
	if b[0] != filterTypeBinaryFuse16 {
		return nil, fmt.Errorf("%w: unknown filter type %d", ErrCorrupt, b[0])
	}
	fpCount := binary.BigEndian.Uint32(b[25:29])
	if len(b) != filterHeaderSize+2*int(fpCount) {
		return nil, fmt.Errorf("%w: filter section is %d bytes, want %d", ErrCorrupt, len(b), filterHeaderSize+2*int(fpCount))
	}
	f := &xorfilter.BinaryFuse[uint16]{
		Seed:               binary.BigEndian.Uint64(b[1:9]),
		SegmentLength:      binary.BigEndian.Uint32(b[9:13]),
		SegmentLengthMask:  binary.BigEndian.Uint32(b[13:17]),
		SegmentCount:       binary.BigEndian.Uint32(b[17:21]),
		SegmentCountLength: binary.BigEndian.Uint32(b[21:25]),
		Fingerprints:       make([]uint16, fpCount),
	}
	for i := range f.Fingerprints {
		f.Fingerprints[i] = binary.BigEndian.Uint16(b[filterHeaderSize+2*i:])
	}
	return f, nil
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
