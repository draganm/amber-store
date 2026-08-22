package nixcache

import (
	"fmt"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

type mapStore map[key.Key][]byte

func (m mapStore) emit(obj fstree.Object) error {
	m[obj.Key] = obj.Bytes
	return nil
}

func (m mapStore) anyKey() key.Key {
	for k := range m {
		return k
	}
	return key.Key{}
}

func (m mapStore) get(k key.Key) ([]byte, error) {
	if b, ok := m[k]; ok {
		return b, nil
	}
	return nil, fstree.ErrNotFound
}

func benchIndex(b *testing.B, n int) (key.Key, mapStore) {
	b.Helper()
	st := mapStore{}
	blob, err := fstree.EncodeBlob([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	pis := make([]PathInfo, n)
	for i := range pis {
		pis[i] = PathInfo{
			StorePath: fmt.Sprintf("/nix/store/%032d-p", i),
			RootKey:   blob.Key,
			NarHash:   [32]byte{1},
			NarSize:   1 << 20,
		}
	}
	root, err := Merge(key.Key{}, pis, nil, st.get, st.emit)
	if err != nil {
		b.Fatal(err)
	}
	return root, st
}

func BenchmarkIndexPublish(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			root, st := benchIndex(b, n)
			pi := PathInfo{
				StorePath: "/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-q",
				RootKey:   st.anyKey(), NarHash: [32]byte{1}, NarSize: 1 << 20,
			}
			b.ResetTimer()
			for b.Loop() {
				if _, err := Merge(root, []PathInfo{pi}, nil, st.get, st.emit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkIndexUnpublish(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			root, st := benchIndex(b, n)
			victim := fmt.Sprintf("%032d", n/2)
			b.ResetTimer()
			for b.Loop() {
				if _, err := Merge(root, nil, []string{victim}, st.get, st.emit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEvictChurn(b *testing.B) {
	p := newEvictPolicy(100_000 << 20) // 100k one-MiB paths resident
	i := 0
	for ; int64(i)<<20 < p.budget; i++ {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
	}
	b.ResetTimer()
	for b.Loop() {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
		i++
		p.Evict()
	}
}

func BenchmarkEvictTouch(b *testing.B) {
	p := newEvictPolicy(1_000 << 20)
	for i := range 1000 {
		p.Admit(fmt.Sprintf("%032d", i), 1<<20)
	}
	b.ResetTimer()
	for b.Loop() {
		p.Touch("00000000000000000000000000000500")
	}
}
