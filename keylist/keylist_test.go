package keylist_test

import (
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/keylist"
	"github.com/draganm/amber-store/key"
)

func testKeys(t *testing.T) []key.Key {
	t.Helper()
	var out []key.Key
	for _, data := range [][]byte{[]byte("one"), []byte("two two"), []byte("three three three")} {
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o.Key)
	}
	return out
}

func TestFlattenParseRoundTrip(t *testing.T) {
	keys := testKeys(t)
	b := keylist.Flatten(keys)
	if len(b) != len(keys)*key.Size {
		t.Fatalf("flattened length = %d, want %d", len(b), len(keys)*key.Size)
	}
	got, err := keylist.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("parsed %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("key %d = %s, want %s", i, got[i], keys[i])
		}
	}
}

func TestParseRejectsBadLength(t *testing.T) {
	if _, err := keylist.Parse(make([]byte, key.Size+1)); err == nil {
		t.Fatal("want error for length not a multiple of 32")
	}
}

func TestParseRejectsNonCanonicalKey(t *testing.T) {
	bad := make([]byte, key.Size) // header 0x00 with zero length byte: non-canonical
	bad[0] = 0x01                 // length-field size 2, but first length byte is 0x00
	if _, err := keylist.Parse(bad); err == nil {
		t.Fatal("want error for non-canonical key")
	}
}

func TestEmptyList(t *testing.T) {
	if b := keylist.Flatten(nil); len(b) != 0 {
		t.Fatalf("Flatten(nil) = %d bytes, want 0", len(b))
	}
	got, err := keylist.Parse(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("Parse(nil) = %v, %v; want empty, nil", got, err)
	}
}
