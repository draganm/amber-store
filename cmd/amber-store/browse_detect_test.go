package main

import (
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestDetectMode_Text(t *testing.T) {
	if got := detectMode([]byte("hello\nworld\n")); got != modeText {
		t.Fatalf("got %v, want modeText", got)
	}
}

func TestDetectMode_BinaryNonCBOR(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x7f, 0x80}
	if got := detectMode(data); got != modeHex {
		t.Fatalf("got %v, want modeHex", got)
	}
}

func TestDetectMode_CBOR(t *testing.T) {
	// A CBOR map of small ints — not valid UTF-8 text, decodes cleanly.
	data, err := cbor.Marshal(map[string]int{"x": 1, "y": 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := detectMode(data); got != modeJSON {
		t.Fatalf("got %v, want modeJSON", got)
	}
}

func TestViewModeString(t *testing.T) {
	for m, want := range map[viewMode]string{modeText: "TEXT", modeHex: "HEX", modeJSON: "JSON"} {
		if got := m.String(); got != want {
			t.Fatalf("%d.String() = %q, want %q", m, got, want)
		}
	}
}
