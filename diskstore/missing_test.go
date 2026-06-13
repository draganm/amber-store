package diskstore_test

import (
	"fmt"
	"runtime"
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

func TestMissingManyWorkersChunkClamp(t *testing.T) {
	// GOMAXPROCS=67 with 4289 keys made worker 66 compute keys[4290:4289]
	// and panic before the low bound was clamped.
	old := runtime.GOMAXPROCS(67)
	defer runtime.GOMAXPROCS(old)

	s := openTemp(t)
	keys := make([]key.Key, 0, 4289)
	for i := 0; i < 4289; i++ {
		data := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0xA5}
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	got, err := s.Missing(keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("Missing returned %d keys, want %d", len(got), len(keys))
	}
}
