package nixcache

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/draganm/amber-store/key"
)

func TestPeerRecord(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{trusted: trustedKeys{"k": pub}}
	hp := strings.Repeat("a", 32)
	ni := Narinfo{StorePath: storeDir + hp + "-pkg", NarSize: 7, References: []string{hp + "-pkg"}}
	ni.NarHash[0] = 1
	sig := "k:" + base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(ni.Fingerprint())))

	for _, tc := range []struct {
		name     string
		hashpart string
		mod      func(*PathInfo)
		ok       bool
	}{
		{"genuine", hp, nil, true},
		{"other hashpart", strings.Repeat("b", 32), nil, false},
		{"no trusted sig", hp, func(p *PathInfo) { p.Sigs = []string{"evil:AAAA"} }, false},
		{"injected deriver line", hp, func(p *PathInfo) { p.Deriver = "x.drv\nSig: injected" }, false},
		{"bad reference", hp, func(p *PathInfo) { p.References = []string{"../etc"} }, false},
		{"valid deriver", hp, func(p *PathInfo) { p.Deriver = strings.Repeat("c", 32) + "-pkg.drv" }, true},
	} {
		pi := ni.pathInfo(key.Key{})
		pi.Sigs = []string{sig, "evil:AAAA"}
		if tc.mod != nil {
			tc.mod(&pi)
		}
		got, ok := n.peerRecord(pi, tc.hashpart)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v, want %v", tc.name, ok, tc.ok)
		}
		if ok && len(got.Sigs) != 1 {
			t.Fatalf("%s: unsigned fields kept: %+v", tc.name, got)
		}
	}
}
