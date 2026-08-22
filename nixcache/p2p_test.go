package nixcache_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

// TestRequestWireGolden pins the postcard encoding of each request variant:
// varint enum tag, then the variant's fields (bytes = varint len + data,
// [u8;32] = 32 raw bytes, String = varint len + utf8).
func TestRequestWireGolden(t *testing.T) {
	var root [32]byte
	for i := range root {
		root[i] = byte(i)
	}
	keys := bytes.Repeat([]byte{0xab}, 64)
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"objects", nixcache.EncodeRequest(0, keys, root, ""), "00" + "40" + hex.EncodeToString(keys)},
		{"keys", nixcache.EncodeRequest(1, nil, root, ""), "01" + hex.EncodeToString(root[:])},
		{"index", nixcache.EncodeRequest(2, nil, root, ""), "02"},
		{"narinfo", nixcache.EncodeRequest(3, nil, root, hashPart(7)), "03" + "20" + hex.EncodeToString([]byte(hashPart(7)))},
	}
	for _, c := range cases {
		if hex.EncodeToString(c.got) != c.want {
			t.Errorf("%s: got %x, want %s", c.name, c.got, c.want)
		}
		rt, err := nixcache.DecodeRequestRoundTrip(c.got)
		if err != nil || !bytes.Equal(rt, c.got) {
			t.Errorf("%s: round trip %x, %v", c.name, rt, err)
		}
	}
	for _, bad := range []string{"", "04", "0201", "01" + "00", "00" + "05" + "00", "03" + "ff"} {
		b, _ := hex.DecodeString(bad)
		if _, err := nixcache.DecodeRequestRoundTrip(b); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestParsePeers(t *testing.T) {
	sw := testSwarm(t)
	spec := nixcache.PeerSpec(sw.Addr())
	got, err := nixcache.ParsePeers([]string{spec})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != sw.ID() || len(got[0].IPAddrs()) != 1 {
		t.Fatalf("parsed %v from %q", got, spec)
	}
	named, err := nixcache.ParsePeers([]string{sw.ID().String() + "@localhost:8322"})
	if err != nil || len(named[0].IPAddrs()) == 0 {
		t.Fatalf("hostname form: %v %v", named, err)
	}
	for _, bad := range []string{sw.ID().String(), "nope@127.0.0.1:1", sw.ID().String() + "@"} {
		if _, err := nixcache.ParsePeers([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
