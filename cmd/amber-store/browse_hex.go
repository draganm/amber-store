package main

import (
	"fmt"
	"strings"
)

// hexDumpLine renders up to 16 bytes as one xxd-style line: an 8-digit hex
// offset, 16 hex byte columns (an extra space after the 8th, blanks where the
// run is short), and an ASCII gutter with non-printable bytes shown as '.'.
func hexDumpLine(offset int, b []byte) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%08x  ", offset)
	for i := 0; i < 16; i++ {
		if i < len(b) {
			fmt.Fprintf(&sb, "%02x ", b[i])
		} else {
			sb.WriteString("   ")
		}
		if i == 7 {
			sb.WriteByte(' ')
		}
	}
	sb.WriteString(" |")
	for _, c := range b {
		if c >= 0x20 && c < 0x7f {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('.')
		}
	}
	sb.WriteByte('|')
	return sb.String()
}
