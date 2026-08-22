package nixcache_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

func TestNixBase32RoundTrip(t *testing.T) {
	for size := 1; size <= 64; size++ {
		b := make([]byte, size)
		rand.Read(b)
		enc := nixcache.EncodeNixBase32(b)
		dec, err := nixcache.DecodeNixBase32(enc)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if !bytes.Equal(dec, b) {
			t.Fatalf("size %d: round trip mismatch", size)
		}
	}
}

func TestNixBase32GoldenNixHash(t *testing.T) {
	nixHash, err := exec.LookPath("nix-hash")
	if err != nil {
		t.Skip("nix-hash not on PATH")
	}
	h := sha256.Sum256([]byte("amber"))
	out, err := exec.Command(nixHash, "--to-base32", "--type", "sha256", hex.EncodeToString(h[:])).Output()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.TrimSpace(string(out))
	if got := nixcache.EncodeNixBase32(h[:]); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestNixBase32RejectsInvalid(t *testing.T) {
	for _, s := range []string{"e", "abc", strings.Repeat("z", 52)} {
		if _, err := nixcache.DecodeNixBase32(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestNarinfoFormatParse(t *testing.T) {
	pi := info(3)
	pi.Deriver = hashPart(4) + "-pkg.drv"
	pi.Sigs = []string{"cache.nixos.org-1:AAAA", "extra-1:BBBB"}
	doc := nixcache.FormatNarinfo(pi)

	n, err := nixcache.ParseNarinfo(doc)
	if err != nil {
		t.Fatal(err)
	}
	if n.StorePath != pi.StorePath || n.NarHash != pi.NarHash || n.NarSize != pi.NarSize ||
		n.Deriver != pi.Deriver || len(n.Sigs) != 2 || n.Compression != "zstd" {
		t.Fatalf("parse mismatch: %+v", n)
	}
	k, err := nixcache.NarURLKey(n.URL)
	if err != nil {
		t.Fatal(err)
	}
	if k != pi.RootKey {
		t.Fatalf("URL key %s != root key %s", k, pi.RootKey)
	}
}

func TestParseUpstreamNarinfo(t *testing.T) {
	h := sha256.Sum256([]byte("nar"))
	doc := fmt.Sprintf(`StorePath: /nix/store/%s-hello-2.12.1
URL: nar/1w1fff338fvdw53sqgamddn1b2xgds473pv6y13gizdbqjv4i5p3.nar.xz
Compression: xz
FileHash: sha256:1w1fff338fvdw53sqgamddn1b2xgds473pv6y13gizdbqjv4i5p3
FileSize: 50160
NarHash: sha256:%s
NarSize: 226504
References: %s-hello-2.12.1 %s-glibc-2.38
Deriver: %s-hello-2.12.1.drv
Sig: cache.nixos.org-1:sig1
Sig: other-1:sig2
`, hashPart(1), nixcache.EncodeNixBase32(h[:]), hashPart(1), hashPart(2), hashPart(3))

	n, err := nixcache.ParseNarinfo([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if n.Compression != "xz" || n.NarSize != 226504 || len(n.References) != 2 ||
		len(n.Sigs) != 2 || n.NarHash != h {
		t.Fatalf("parse mismatch: %+v", n)
	}
}

func TestParseNarinfoRejects(t *testing.T) {
	for name, doc := range map[string]string{
		"no store path": "NarHash: sha256:x\n",
		"bad hash":      fmt.Sprintf("StorePath: /nix/store/%s-x\nNarHash: md5:abc\nNarSize: 1\n", hashPart(1)),
		"no size":       fmt.Sprintf("StorePath: /nix/store/%s-x\nNarHash: sha256:%s\n", hashPart(1), nixcache.EncodeNixBase32(make([]byte, 32))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := nixcache.ParseNarinfo([]byte(doc)); err == nil {
				t.Fatal("accepted malformed narinfo")
			}
		})
	}
}

func TestSigVerify(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("nar"))
	n := nixcache.Narinfo{
		StorePath:  fmt.Sprintf("/nix/store/%s-hello-2.12.1", hashPart(1)),
		NarHash:    h,
		NarSize:    226504,
		References: []string{hashPart(1) + "-hello-2.12.1", hashPart(2) + "-glibc-2.38"},
	}
	sig := "test-1:" + base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(n.Fingerprint())))
	trusted := map[string]ed25519.PublicKey{"test-1": pub}

	if !n.VerifySig(sig, trusted) {
		t.Fatal("valid signature rejected")
	}
	if n.VerifySig("unknown-1:"+strings.SplitN(sig, ":", 2)[1], trusted) {
		t.Fatal("unknown key accepted")
	}
	tampered := n
	tampered.NarSize++
	if tampered.VerifySig(sig, trusted) {
		t.Fatal("tampered narinfo accepted")
	}
	name, key2, err := nixcache.ParseTrustedKey("test-1:" + base64.StdEncoding.EncodeToString(pub))
	if err != nil || name != "test-1" || !key2.Equal(pub) {
		t.Fatalf("ParseTrustedKey: %v", err)
	}
}

func TestFingerprintMatchesNixFormat(t *testing.T) {
	h := sha256.Sum256([]byte("nar"))
	n := nixcache.Narinfo{
		StorePath:  "/nix/store/" + hashPart(1) + "-x",
		NarHash:    h,
		NarSize:    5,
		References: []string{hashPart(2) + "-a", hashPart(3) + "-b"},
	}
	want := "1;" + n.StorePath + ";sha256:" + nixcache.EncodeNixBase32(h[:]) + ";5;" +
		"/nix/store/" + hashPart(2) + "-a,/nix/store/" + hashPart(3) + "-b"
	if got := n.Fingerprint(); got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
}
