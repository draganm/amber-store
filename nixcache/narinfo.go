package nixcache

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/draganm/amber-store/key"
)

// Narinfo is the wire form of a .narinfo document.
type Narinfo struct {
	StorePath   string
	URL         string
	Compression string
	NarHash     [32]byte
	NarSize     uint64
	References  []string
	Deriver     string
	Sigs        []string
}

// FormatNarinfo renders the narinfo we serve for p: URL by root key,
// Compression zstd, upstream sigs verbatim, no FileHash/FileSize.
func FormatNarinfo(p PathInfo) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "StorePath: %s\n", p.StorePath)
	fmt.Fprintf(&b, "URL: nar/%x.nar.zst\n", p.RootKey[:])
	b.WriteString("Compression: zstd\n")
	fmt.Fprintf(&b, "NarHash: sha256:%s\n", EncodeNixBase32(p.NarHash[:]))
	fmt.Fprintf(&b, "NarSize: %d\n", p.NarSize)
	fmt.Fprintf(&b, "References: %s\n", strings.Join(p.References, " "))
	if p.Deriver != "" {
		fmt.Fprintf(&b, "Deriver: %s\n", p.Deriver)
	}
	for _, s := range p.Sigs {
		fmt.Fprintf(&b, "Sig: %s\n", s)
	}
	return b.Bytes()
}

// ParseNarinfo parses a .narinfo document, ignoring unknown fields.
func ParseNarinfo(data []byte) (Narinfo, error) {
	var n Narinfo
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		field, val, ok := strings.Cut(line, ": ")
		if !ok {
			if field, ok = strings.CutSuffix(line, ":"); !ok {
				return Narinfo{}, fmt.Errorf("nixcache: malformed narinfo line %q", line)
			}
			val = ""
		}
		switch field {
		case "StorePath":
			n.StorePath = val
		case "URL":
			n.URL = val
		case "Compression":
			n.Compression = val
		case "NarHash":
			hash, ok := strings.CutPrefix(val, "sha256:")
			if !ok {
				return Narinfo{}, fmt.Errorf("nixcache: unsupported NarHash %q", val)
			}
			h, err := DecodeNixBase32(hash)
			if err != nil || len(h) != 32 {
				return Narinfo{}, fmt.Errorf("nixcache: bad NarHash %q", val)
			}
			copy(n.NarHash[:], h)
		case "NarSize":
			size, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return Narinfo{}, fmt.Errorf("nixcache: NarSize %q: %w", val, err)
			}
			n.NarSize = size
		case "References":
			if val != "" {
				n.References = strings.Fields(val)
			}
		case "Deriver":
			n.Deriver = val
		case "Sig":
			n.Sigs = append(n.Sigs, val)
		}
	}
	if err := sc.Err(); err != nil {
		return Narinfo{}, err
	}
	if HashPart(n.StorePath) == "" {
		return Narinfo{}, fmt.Errorf("nixcache: narinfo store path %q", n.StorePath)
	}
	if n.NarHash == ([32]byte{}) || n.NarSize == 0 {
		return Narinfo{}, fmt.Errorf("nixcache: narinfo for %s missing NarHash/NarSize", n.StorePath)
	}
	return n, nil
}

// Fingerprint returns the string nix signs for a path.
func (n Narinfo) Fingerprint() string {
	refs := make([]string, len(n.References))
	for i, r := range n.References {
		refs[i] = storeDir + r
	}
	return fmt.Sprintf("1;%s;sha256:%s;%d;%s",
		n.StorePath, EncodeNixBase32(n.NarHash[:]), n.NarSize, strings.Join(refs, ","))
}

// VerifySig checks one "name:base64" signature against trusted
// "name:base64" ed25519 public keys.
func (n Narinfo) VerifySig(sig string, trusted map[string]ed25519.PublicKey) bool {
	name, b64, ok := strings.Cut(sig, ":")
	if !ok {
		return false
	}
	pub, ok := trusted[name]
	if !ok {
		return false
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, []byte(n.Fingerprint()), raw)
}

// ParseTrustedKey parses a "name:base64" ed25519 public key.
func ParseTrustedKey(s string) (string, ed25519.PublicKey, error) {
	name, b64, ok := strings.Cut(s, ":")
	if !ok {
		return "", nil, fmt.Errorf("nixcache: malformed trusted key %q", s)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return "", nil, fmt.Errorf("nixcache: malformed trusted key %q", s)
	}
	return name, ed25519.PublicKey(raw), nil
}

// NarURLKey parses the root key out of a "nar/<hex>.nar.zst" URL.
func NarURLKey(url string) (key.Key, error) {
	malformed := fmt.Errorf("nixcache: malformed nar URL %q", url)
	hexpart, ok := strings.CutPrefix(url, "nar/")
	if !ok {
		return key.Key{}, malformed
	}
	hexpart, ok = strings.CutSuffix(hexpart, ".nar.zst")
	if !ok || len(hexpart) != 2*key.Size {
		return key.Key{}, malformed
	}
	raw, err := hex.DecodeString(hexpart)
	if err != nil {
		return key.Key{}, malformed
	}
	return key.Parse(raw)
}
