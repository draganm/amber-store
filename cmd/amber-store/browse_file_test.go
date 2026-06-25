package main

import (
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestNewFileView_DefaultModeAndTextLines(t *testing.T) {
	fv := newFileView("a.txt", 12, []byte("l1\nl2\nl3\n"), false)
	if fv.mode != modeText {
		t.Fatalf("mode = %v, want modeText", fv.mode)
	}
	// "l1\nl2\nl3\n" splits into l1,l2,l3,"" — trailing empty line.
	if fv.lineCount() != 4 {
		t.Fatalf("lineCount = %d, want 4", fv.lineCount())
	}
}

func TestFileView_SetModeJSON(t *testing.T) {
	data, err := cbor.Marshal(map[string]int{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	fv := newFileView("o.cbor", uint64(len(data)), data, false)
	if fv.mode != modeJSON {
		t.Fatalf("auto mode = %v, want modeJSON", fv.mode)
	}
	out := fv.render(10)
	if !strings.Contains(out, `"x": 1`) {
		t.Fatalf("json not rendered:\n%s", out)
	}
}

func TestFileView_JSONErrorOnTruncated(t *testing.T) {
	fv := newFileView("big", 999, []byte{0xa1, 0x61}, true) // truncated, undecodable
	fv.setMode(modeJSON)
	out := fv.render(10)
	if !strings.Contains(out, "truncated at cap") {
		t.Fatalf("expected truncation notice in JSON mode:\n%s", out)
	}
}

func TestFileView_ScrollClamp(t *testing.T) {
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, "x")
	}
	fv := newFileView("a", 0, []byte(strings.Join(lines, "\n")), false)
	fv.scroll(1000, 10) // far past end
	maxTop := fv.lineCount() - (10 - 1)
	if fv.top != maxTop {
		t.Fatalf("top = %d, want clamped %d", fv.top, maxTop)
	}
	fv.scroll(-1000, 10)
	if fv.top != 0 {
		t.Fatalf("top = %d, want 0", fv.top)
	}
}

func TestFileView_RenderHexWindow(t *testing.T) {
	fv := newFileView("b", 0, []byte{0x00, 0x01, 0x02, 0xff}, false) // binary -> hex
	if fv.mode != modeHex {
		t.Fatalf("mode = %v, want modeHex", fv.mode)
	}
	out := fv.render(10)
	if !strings.Contains(out, "00000000  00 01 02 ff") {
		t.Fatalf("hex not rendered:\n%s", out)
	}
}
