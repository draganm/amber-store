package nixcache

import (
	"fmt"
	"strings"
)

// EncodeNixBase32 encodes b in nix's base32: little-endian 5-bit groups,
// most significant group first, alphabet without e/o/t/u.
func EncodeNixBase32(b []byte) string {
	n := (len(b)*8 + 4) / 5
	out := make([]byte, 0, n)
	for i := n - 1; i >= 0; i-- {
		bit := i * 5
		j, r := bit/8, bit%8
		c := b[j] >> r
		if j+1 < len(b) {
			c |= b[j+1] << (8 - r)
		}
		out = append(out, nixBase32[c&0x1f])
	}
	return string(out)
}

// DecodeNixBase32 is the inverse of EncodeNixBase32.
func DecodeNixBase32(s string) ([]byte, error) {
	nbytes := len(s) * 5 / 8
	if len(s) != (nbytes*8+4)/5 {
		return nil, fmt.Errorf("nixcache: invalid nix-base32 length %d", len(s))
	}
	out := make([]byte, nbytes)
	for i := 0; i < len(s); i++ {
		c := s[len(s)-1-i]
		d := strings.IndexByte(nixBase32, c)
		if d < 0 {
			return nil, fmt.Errorf("nixcache: invalid nix-base32 char %q", c)
		}
		bit := i * 5
		j, r := bit/8, bit%8
		out[j] |= byte(d) << r
		carry := byte(d) >> (8 - r) // zero whenever r <= 3, since d < 32
		switch {
		case carry == 0:
		case j+1 < nbytes:
			out[j+1] |= carry
		default:
			return nil, fmt.Errorf("nixcache: invalid nix-base32 trailing bits")
		}
	}
	return out, nil
}
