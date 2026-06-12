package diskstore_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/draganm/amber-store/key"
)

func TestMissingReturnsAbsentSubsetInOrder(t *testing.T) {
	s := openTemp(t)
	// Enough keys to span several parallel chunks.
	const n = 500
	keys := make([]key.Key, 0, n+1)
	var want []key.Key
	for i := range n {
		data := fmt.Appendf(nil, "object %d", i)
		k := mkKey(t, data)
		keys = append(keys, k)
		if i%3 == 0 {
			if err := s.Put(k, data); err != nil {
				t.Fatalf("Put: %v", err)
			}
		} else {
			want = append(want, k)
		}
	}
	// A duplicated absent key is reported once per occurrence.
	keys = append(keys, keys[1])
	want = append(want, keys[1])

	got, err := s.Missing(keys)
	if err != nil {
		t.Fatalf("Missing: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Missing = %d keys, want %d absent keys in request order", len(got), len(want))
	}
}

func TestMissingEmptyInput(t *testing.T) {
	s := openTemp(t)
	got, err := s.Missing(nil)
	if err != nil {
		t.Fatalf("Missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Missing = %v, want empty", got)
	}
}
