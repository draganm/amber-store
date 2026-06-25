package main

import "testing"

func TestHexDumpLine_Full(t *testing.T) {
	b := []byte("ABCDEFGHIJKLMNOP") // 16 printable bytes
	got := hexDumpLine(0, b)
	want := "00000000  41 42 43 44 45 46 47 48  49 4a 4b 4c 4d 4e 4f 50  |ABCDEFGHIJKLMNOP|"
	if got != want {
		t.Fatalf("\ngot:  %q\nwant: %q", got, want)
	}
}

func TestHexDumpLine_ShortWithNonPrintable(t *testing.T) {
	b := []byte{0x00, 0x41, 0xff}
	got := hexDumpLine(16, b)
	want := "00000010  00 41 ff                                          |.A.|"
	if got != want {
		t.Fatalf("\ngot:  %q\nwant: %q", got, want)
	}
}
