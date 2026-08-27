package nixcache_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/nixcache"
)

type memStore map[key.Key][]byte

func (m memStore) emit(o fstree.Object) error { m[o.Key] = o.Bytes; return nil }
func (m memStore) get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

const alphabet = "0123456789abcdfghijklmnpqrsvwxyz"

func hashPart(i int) string {
	b := make([]byte, 32)
	n := i
	for j := range b {
		b[j] = alphabet[n%len(alphabet)]
		n /= len(alphabet)
	}
	return string(b)
}

func info(i int) nixcache.PathInfo {
	var nh [32]byte
	nh[0] = byte(i)
	return nixcache.PathInfo{
		StorePath:  fmt.Sprintf("/nix/store/%s-pkg-%d", hashPart(i), i),
		RootKey:    key.Key{0x20, byte(i)}, // DirLeaf type nibble
		NarHash:    nh,
		NarSize:    uint64(1000 + i),
		References: []string{hashPart(i) + "-pkg-" + fmt.Sprint(i)},
		Sigs:       []string{"cache.nixos.org-1:c2lnbmF0dXJl"},
		IngestedAt: 1700000000,
	}
}

func TestRecordRoundTrip(t *testing.T) {
	pi := info(1)
	pi.Deriver = hashPart(2) + "-pkg.drv"
	pi.UpstreamCompression = "zstd"
	b, err := nixcache.EncodeRecord(pi)
	if err != nil {
		t.Fatal(err)
	}
	got, err := nixcache.DecodeRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", pi) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, pi)
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	for name, mut := range map[string]func(*nixcache.PathInfo){
		"bad prefix":    func(p *nixcache.PathInfo) { p.StorePath = "/tmp/x-y" },
		"short hash":    func(p *nixcache.PathInfo) { p.StorePath = "/nix/store/abc-y" },
		"bad base32":    func(p *nixcache.PathInfo) { p.StorePath = "/nix/store/EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE-y" },
		"no name":       func(p *nixcache.PathInfo) { p.StorePath = "/nix/store/" + hashPart(1) },
		"zero root key": func(p *nixcache.PathInfo) { p.RootKey = key.Key{} },
	} {
		t.Run(name, func(t *testing.T) {
			pi := info(1)
			mut(&pi)
			if _, err := nixcache.EncodeRecord(pi); err == nil {
				t.Fatal("encoded invalid PathInfo")
			}
		})
	}
}

func TestMergeAndLookup(t *testing.T) {
	st := memStore{}
	var infos []nixcache.PathInfo
	for i := 0; i < 500; i++ {
		infos = append(infos, info(i))
	}
	root, err := nixcache.Merge(key.Key{}, infos, nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		got, err := nixcache.Lookup(root, nixcache.HashPart(infos[i].StorePath), st.get)
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if got.StorePath != infos[i].StorePath {
			t.Fatalf("lookup %d: got %s", i, got.StorePath)
		}
	}
	if _, err := nixcache.Lookup(root, hashPart(1000), st.get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("missing path: %v", err)
	}
}

func TestMergeUpdatesAndDeletes(t *testing.T) {
	st := memStore{}
	var infos []nixcache.PathInfo
	for i := 0; i < 100; i++ {
		infos = append(infos, info(i))
	}
	root, err := nixcache.Merge(key.Key{}, infos, nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}

	upd := info(7)
	upd.NarSize = 99999
	root2, err := nixcache.Merge(root, []nixcache.PathInfo{upd, info(200)},
		[]string{nixcache.HashPart(info(13).StorePath)}, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}

	got, err := nixcache.Lookup(root2, nixcache.HashPart(upd.StorePath), st.get)
	if err != nil {
		t.Fatal(err)
	}
	if got.NarSize != 99999 {
		t.Fatalf("update not applied: %d", got.NarSize)
	}
	if _, err := nixcache.Lookup(root2, nixcache.HashPart(info(200).StorePath), st.get); err != nil {
		t.Fatalf("insert missing: %v", err)
	}
	if _, err := nixcache.Lookup(root2, nixcache.HashPart(info(13).StorePath), st.get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("delete not applied: %v", err)
	}
	if _, err := nixcache.Lookup(root2, nixcache.HashPart(info(42).StorePath), st.get); err != nil {
		t.Fatalf("untouched path lost: %v", err)
	}
}

func TestMergeDeterministic(t *testing.T) {
	st := memStore{}
	var infos []nixcache.PathInfo
	for i := 0; i < 300; i++ {
		infos = append(infos, info(i))
	}
	all, err := nixcache.Merge(key.Key{}, infos, nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	first, err := nixcache.Merge(key.Key{}, infos[:150], nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	incr, err := nixcache.Merge(first, infos[150:], nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	if incr != all {
		t.Fatalf("incremental root %s != batch root %s", incr, all)
	}
}

func TestEmptyIndex(t *testing.T) {
	st := memStore{}
	root, err := nixcache.Merge(key.Key{}, nil, nil, st.get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nixcache.Lookup(root, hashPart(1), st.get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("empty index lookup: %v", err)
	}
}
