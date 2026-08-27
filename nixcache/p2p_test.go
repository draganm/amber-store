package nixcache_test

import (
	"bytes"
	"encoding/hex"
	"net/netip"
	"strings"
	"testing"

	ikey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"

	"github.com/draganm/amber-store/nixcache"
	"github.com/tmc/go-iroh/relay"
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

func TestRelayMode(t *testing.T) {
	if _, err := nixcache.RelayMode([]string{"https://r.example"}, true, nil); err == nil {
		t.Fatal("--relay with --no-relay accepted")
	}
	if _, err := nixcache.RelayMode([]string{"not a url"}, false, nil); err == nil {
		t.Fatal("bad relay URL accepted")
	}
	m, err := nixcache.RelayMode([]string{"https://r.example./"}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if urls := m.Map().URLs(); len(urls) != 1 || urls[0].String() != "https://r.example./" {
		t.Fatalf("urls = %v", urls)
	}
	if m, _ := nixcache.RelayMode(nil, false, nil); len(m.Map().URLs()) == 0 {
		t.Fatal("default mode has no relays")
	}
	if m, _ := nixcache.RelayMode(nil, true, nil); len(m.Map().URLs()) != 0 {
		t.Fatal("disabled mode has relays")
	}
	rh, err := nixcache.ServeRelay(nixcache.RelayOpts{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer rh.Close()
	if _, err := nixcache.RelayMode(nil, true, rh); err == nil {
		t.Fatal("--serve-relay with --no-relay accepted")
	}
	m, _ = nixcache.RelayMode(nil, false, rh)
	if urls := m.Map().URLs(); len(urls) < 2 || !m.Map().Contains(rh.URL()) {
		t.Fatalf("own relay plus defaults expected: %v", urls)
	}
	sk, _ := ikey.GenerateSecretKey()
	peers := []netaddr.EndpointAddr{netaddr.NewEndpointAddr(sk.Public().EndpointID()).WithIP(netip.MustParseAddrPort("127.0.0.1:1"))}
	got := nixcache.WithSwarmRelays(peers, []string{"https://seeder:3340"}, rh)
	if len(got[0].RelayURLs()) != 2 || len(got[0].IPAddrs()) != 1 {
		t.Fatalf("WithSwarmRelays = %v", got[0])
	}
	if again := nixcache.WithSwarmRelays(got, []string{"https://other"}, nil); len(again[0].RelayURLs()) != 2 {
		t.Fatalf("peer with relays modified: %v", again[0])
	}
	m, _ = nixcache.RelayMode([]string{"https://seeder:3340", "https://r.example"}, false, nil)
	for _, c := range m.Map().Configs() {
		want := uint16(relay.DefaultQUICPort)
		if strings.Contains(c.URL.String(), "seeder") {
			want = 3340
		}
		if c.QUIC == nil || c.QUIC.Port != want {
			t.Fatalf("%s: QUIC = %+v, want port %d", c.URL, c.QUIC, want)
		}
	}
}
