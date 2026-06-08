package chunkers

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSplitBytes_ReassemblesInput(t *testing.T) {
	in := make([]byte, 5*1024*1024)
	if _, err := rand.Read(in); err != nil {
		t.Fatal(err)
	}
	var got []byte
	n := 0
	err := SplitBytes(bytes.NewReader(in), nil, func(chunk []byte) error {
		n++
		got = append(got, chunk...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, in) {
		t.Fatal("reassembled bytes differ from input")
	}
	if n < 2 {
		t.Fatalf("expected multiple chunks for 5 MiB, got %d", n)
	}
}

func TestSplitBytes_EmptyInputYieldsNoChunks(t *testing.T) {
	n := 0
	err := SplitBytes(bytes.NewReader(nil), nil, func([]byte) error { n++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("empty input must yield 0 chunks, got %d", n)
	}
}

func TestSplitBytes_ChunksAreCopiesSafeToRetain(t *testing.T) {
	in := make([]byte, 2*1024*1024)
	for i := range in {
		in[i] = byte(i)
	}
	var retained [][]byte
	err := SplitBytes(bytes.NewReader(in), nil, func(chunk []byte) error {
		retained = append(retained, chunk) // retain without copying
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, c := range retained {
		got = append(got, c...)
	}
	if !bytes.Equal(got, in) {
		t.Fatal("retained chunks were mutated by later Next() calls — SplitBytes must copy")
	}
}
