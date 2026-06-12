package packstore

import (
	"slices"
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
