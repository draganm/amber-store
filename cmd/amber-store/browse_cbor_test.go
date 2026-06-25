package main

import (
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestCborToJSON_Map(t *testing.T) {
	in, err := cbor.Marshal(map[string]any{"a": 1, "b": "x"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatalf("cborToJSON: %v", err)
	}
	if !strings.Contains(out, `"a": 1`) || !strings.Contains(out, `"b": "x"`) {
		t.Fatalf("unexpected JSON:\n%s", out)
	}
}

func TestCborToJSON_ByteStringToHex(t *testing.T) {
	in, err := cbor.Marshal(map[string]any{"k": []byte{0xde, 0xad}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"dead"`) {
		t.Fatalf("byte string not hex-encoded:\n%s", out)
	}
}

func TestCborToJSON_IntKey(t *testing.T) {
	in, err := cbor.Marshal(map[int]string{7: "v"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cborToJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"7": "v"`) {
		t.Fatalf("int key not stringified:\n%s", out)
	}
}

func TestCborToJSON_TrailingDataErrors(t *testing.T) {
	one, err := cbor.Marshal(1)
	if err != nil {
		t.Fatal(err)
	}
	in := append(one, one...) // two CBOR values back-to-back
	if _, err := cborToJSON(in); err == nil {
		t.Fatal("expected error on trailing data")
	}
}
