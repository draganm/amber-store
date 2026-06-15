package inbox

import (
	"bytes"
	"testing"
)

func TestMetaHeaderRoundTrip(t *testing.T) {
	root := make([]byte, 32)
	for i := range root {
		root[i] = byte(i)
	}
	in := Meta{Ref: "site", Root: root, ReceivedAt: 1234567890}
	var buf bytes.Buffer
	if err := writeMetaHeader(&buf, in); err != nil {
		t.Fatalf("writeMetaHeader: %v", err)
	}
	// Append a fake body; readMetaHeader must stop right after the header.
	buf.WriteString("BODYBYTES")
	out, err := readMetaHeader(&buf)
	if err != nil {
		t.Fatalf("readMetaHeader: %v", err)
	}
	if out.Ref != in.Ref || out.ReceivedAt != in.ReceivedAt || !bytes.Equal(out.Root, in.Root) {
		t.Fatalf("round-trip mismatch: got %+v want %+v", out, in)
	}
	if rest := buf.String(); rest != "BODYBYTES" {
		t.Fatalf("reader left body wrong: %q", rest)
	}
}
