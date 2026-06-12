package packstore

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/draganm/amber-store/key"
)

// testEntries builds n index entries with distinct keys and synthetic offsets.
func testEntries(t *testing.T, n int) []indexEntry {
	t.Helper()
	entries := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		data := append(incompressible(64), byte(i), byte(i>>8), byte(i>>16))
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, indexEntry{k: k, off: uint64(8 + i*100), slen: uint32(i + 1)})
	}
	return entries
}

func TestIndexSectionLookup(t *testing.T) {
	entries := testEntries(t, 1000)
	idx := buildIndexSection(entries)
	if len(idx) != fanoutSize+len(entries)*indexEntrySize {
		t.Fatalf("index length %d", len(idx))
	}
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok {
			t.Fatalf("key %s not found", e.k)
		}
		if off != e.off || slen != e.slen {
			t.Fatalf("key %s: got (%d,%d) want (%d,%d)", e.k, off, slen, e.off, e.slen)
		}
	}
}

func TestIndexSectionAbsentKey(t *testing.T) {
	entries := testEntries(t, 100)
	idx := buildIndexSection(entries)
	fanout, entryBytes, err := parseIndexSection(idx, uint64(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	absent := blobObj(t, []byte("definitely not stored")).Key
	if _, _, ok := searchIndex(fanout, entryBytes, absent); ok {
		t.Fatal("absent key reported present")
	}
}

func TestIndexSectionSingleEntryAndEdgeBuckets(t *testing.T) {
	// Force last bytes 0x00 and 0xFF to cover the b==0 lower bound and the
	// final bucket.
	for _, last := range []byte{0x00, 0xFF, 0x80} {
		e := testEntries(t, 1)[0]
		e.k[31] = last
		idx := buildIndexSection([]indexEntry{e})
		fanout, entryBytes, err := parseIndexSection(idx, 1)
		if err != nil {
			t.Fatal(err)
		}
		off, slen, ok := searchIndex(fanout, entryBytes, e.k)
		if !ok || off != e.off || slen != e.slen {
			t.Fatalf("last=%#x: ok=%v off=%d slen=%d", last, ok, off, slen)
		}
		miss := e.k
		miss[30] ^= 0xFF
		if _, _, ok := searchIndex(fanout, entryBytes, miss); ok {
			t.Fatalf("last=%#x: absent key found", last)
		}
	}
}

func TestIndexSectionDoesNotMutateInput(t *testing.T) {
	entries := testEntries(t, 50)
	orig := slices.Clone(entries)
	_ = buildIndexSection(entries)
	if !slices.Equal(entries, orig) {
		t.Fatal("buildIndexSection mutated its input")
	}
}

func TestParseIndexSectionRejectsCorruption(t *testing.T) {
	entries := testEntries(t, 10)
	idx := buildIndexSection(entries)

	t.Run("wrong length", func(t *testing.T) {
		if _, _, err := parseIndexSection(idx[:len(idx)-1], 10); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("non-monotonic fanout", func(t *testing.T) {
		bad := slices.Clone(idx)
		// Make fanout[1] < fanout[0] by forcing fanout[0] huge.
		binary.BigEndian.PutUint32(bad[0:4], 0xFFFFFFFF)
		if _, _, err := parseIndexSection(bad, 10); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("fanout total mismatch", func(t *testing.T) {
		// Keep the section length correct for keyCount=1 but zero the whole
		// fanout: monotonic, total 0 != 1 — must hit the total check, not the
		// length check.
		one := buildIndexSection(testEntries(t, 1))
		bad := slices.Clone(one)
		for i := 0; i < fanoutSize; i++ {
			bad[i] = 0
		}
		_, _, err := parseIndexSection(bad, 1)
		if err == nil {
			t.Fatal("want error")
		}
		if !strings.Contains(err.Error(), "fanout total") {
			t.Fatalf("wrong branch: %v", err)
		}
	})
	t.Run("huge keyCount does not wrap", func(t *testing.T) {
		// (2^64-984)/44 made the old int arithmetic compute want==40, so a
		// 40-byte section passed the length check and the fanout loop
		// panicked. The fixed code must reject it as corrupt instead.
		huge := (math.MaxUint64 - uint64(983)) / indexEntrySize
		_, _, err := parseIndexSection(idx[:40], huge)
		if err == nil {
			t.Fatal("want error, not a panic")
		}
		if !strings.Contains(err.Error(), "exceeds format limit") {
			t.Fatalf("wrong branch: %v", err)
		}
	})
}

func TestIndexSectionEmptyAndEmptyBucket(t *testing.T) {
	// Empty section round-trips and misses cleanly.
	idx := buildIndexSection(nil)
	fanout, entryBytes, err := parseIndexSection(idx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := searchIndex(fanout, entryBytes, blobObj(t, []byte("x")).Key); ok {
		t.Fatal("found key in empty index")
	}

	// Deterministic empty-bucket miss: one entry with last byte 0x10,
	// search a key with last byte 0x20 (a guaranteed-empty bucket).
	e := testEntries(t, 1)[0]
	e.k[31] = 0x10
	idx = buildIndexSection([]indexEntry{e})
	fanout, entryBytes, err = parseIndexSection(idx, 1)
	if err != nil {
		t.Fatal(err)
	}
	probe := e.k
	probe[31] = 0x20
	if _, _, ok := searchIndex(fanout, entryBytes, probe); ok {
		t.Fatal("found key in empty bucket")
	}
}

func TestFilterSectionMembership(t *testing.T) {
	entries := testEntries(t, 5000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !f.Contains(filterKey(e.k)) {
			t.Fatalf("false negative for %s", e.k)
		}
	}
}

func TestFilterSectionFalsePositiveRate(t *testing.T) {
	entries := testEntries(t, 1000)
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	// 16-bit fingerprints: FP rate ~2^-16. Expect ~1.5 hits in 100k probes;
	// 50 leaves astronomical margin while still catching a broken filter.
	fp := 0
	for i := uint64(0); i < 100_000; i++ {
		if f.Contains(0xDEAD_0000_0000_0000 + i) {
			fp++
		}
	}
	if fp > 50 {
		t.Fatalf("false positive rate too high: %d/100000", fp)
	}
}

func TestFilterSectionDuplicateTails(t *testing.T) {
	// Two entries with an identical 8-byte tail must not break the build.
	entries := testEntries(t, 2)
	copy(entries[1].k[24:32], entries[0].k[24:32])
	sec, err := buildFilterSection(entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parseFilterSection(sec)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Contains(filterKey(entries[0].k)) || !f.Contains(filterKey(entries[1].k)) {
		t.Fatal("false negative on duplicate tails")
	}
}

func TestParseFilterSectionRejectsCorruption(t *testing.T) {
	sec, err := buildFilterSection(testEntries(t, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("short", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:10]); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("bad type", func(t *testing.T) {
		bad := slices.Clone(sec)
		bad[0] = 99
		if _, err := parseFilterSection(bad); err == nil {
			t.Fatal("want error")
		}
	})
	t.Run("length mismatch", func(t *testing.T) {
		if _, err := parseFilterSection(sec[:len(sec)-2]); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestIndexSectionGoldenBytes(t *testing.T) {
	// Pin the on-disk encoding against symmetric encode/decode bugs: two
	// fixed entries, exact expected bytes.
	var k1, k2 key.Key
	k1[0] = 0x01 // Blob, 1-byte length field
	k1[1] = 0x05 // length 5
	k1[31] = 0x02
	k2[0] = 0x01
	k2[1] = 0x07
	k2[31] = 0x01 // sorts before k1 (last byte)
	idx := buildIndexSection([]indexEntry{
		{k: k1, off: 0x1122334455667788, slen: 0xAABBCCDD},
		{k: k2, off: 8, slen: 1},
	})

	// fanout: bytes 0x00 → 0, 0x01 → 1, 0x02..0xFF → 2 (cumulative, BE)
	if got := binary.BigEndian.Uint32(idx[0:4]); got != 0 {
		t.Fatalf("fanout[0] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[4:8]); got != 1 {
		t.Fatalf("fanout[1] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[8:12]); got != 2 {
		t.Fatalf("fanout[2] = %d", got)
	}
	if got := binary.BigEndian.Uint32(idx[1020:1024]); got != 2 {
		t.Fatalf("fanout[255] = %d", got)
	}

	// First entry must be k2 (last byte 0x01): key bytes, then off/slen BE.
	e0 := idx[fanoutSize : fanoutSize+indexEntrySize]
	if !bytes.Equal(e0[:32], k2[:]) {
		t.Fatalf("entry 0 key = %x", e0[:32])
	}
	if !bytes.Equal(e0[32:40], []byte{0, 0, 0, 0, 0, 0, 0, 8}) {
		t.Fatalf("entry 0 off bytes = %x", e0[32:40])
	}
	if !bytes.Equal(e0[40:44], []byte{0, 0, 0, 1}) {
		t.Fatalf("entry 0 slen bytes = %x", e0[40:44])
	}

	e1 := idx[fanoutSize+indexEntrySize : fanoutSize+2*indexEntrySize]
	if !bytes.Equal(e1[:32], k1[:]) {
		t.Fatalf("entry 1 key = %x", e1[:32])
	}
	if !bytes.Equal(e1[32:40], []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}) {
		t.Fatalf("entry 1 off bytes = %x", e1[32:40])
	}
	if !bytes.Equal(e1[40:44], []byte{0xAA, 0xBB, 0xCC, 0xDD}) {
		t.Fatalf("entry 1 slen bytes = %x", e1[40:44])
	}
}
