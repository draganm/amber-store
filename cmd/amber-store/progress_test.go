package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestProgressRunTTYDrawsAndClears verifies the TTY render loop draws an
// immediate in-place frame and erases it on exit (so the root prints clean).
func TestProgressRunTTYDrawsAndClears(t *testing.T) {
	p := NewProgress(2, 100)
	p.AddBytes(100)
	p.FileDone()
	p.FileDone()

	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx, &buf, time.Now(), true); close(done) }()
	time.Sleep(20 * time.Millisecond) // the immediate draw lands before any tick
	cancel()
	<-done

	out := buf.String()
	if !strings.Contains(out, "ingest ") {
		t.Fatalf("Run drew no frame; output=%q", out)
	}
	if !strings.Contains(out, "files 2/2") {
		t.Fatalf("frame missing file counts; output=%q", out)
	}
	if !strings.HasSuffix(out, "\r\x1b[K") {
		t.Fatalf("Run did not clear the in-place bar on exit; output=%q", out)
	}
}

// TestProgressRunNonTTYLeavesFinalLine verifies the non-TTY path uses newline
// frames (no carriage returns) and leaves a final summary line on exit, so even
// a fast piped ingest shows a start and a 100% line.
func TestProgressRunNonTTYLeavesFinalLine(t *testing.T) {
	p := NewProgress(1, 10)
	p.AddBytes(10)
	p.FileDone()

	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx, &buf, time.Now(), false); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	out := buf.String()
	if strings.Contains(out, "\r") {
		t.Fatalf("non-TTY output must not use carriage returns; output=%q", out)
	}
	if n := strings.Count(out, "\n"); n < 2 {
		t.Fatalf("want at least a start and a final line, got %d: %q", n, out)
	}
	if !strings.Contains(out, "100.0%") {
		t.Fatalf("final line should show 100%%; output=%q", out)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestFmtDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{23 * time.Second, "0:23"},
		{62 * time.Second, "1:02"},
		{3723 * time.Second, "1:02:03"},
		{-5 * time.Second, "0:00"},
	}
	for _, c := range cases {
		if got := fmtDuration(c.d); got != c.want {
			t.Errorf("fmtDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestProgressRender(t *testing.T) {
	p := NewProgress(10, 1000)
	p.AddBytes(500)
	for range 5 {
		p.FileDone()
	}
	line := p.render(10 * time.Second)
	for _, want := range []string{"50.0%", "files 5/10", "50 B/s", "eta 0:10"} {
		if !strings.Contains(line, want) {
			t.Errorf("render = %q, want substring %q", line, want)
		}
	}
}

func TestProgressRenderFinalizing(t *testing.T) {
	p := NewProgress(1, 100)
	p.AddBytes(100)
	line := p.render(5 * time.Second)
	if !strings.Contains(line, "finalizing") {
		t.Errorf("render at 100%% = %q, want finalizing state", line)
	}
}

func TestProgressNilSafe(t *testing.T) {
	var p *Progress
	p.AddBytes(5) // must not panic
	p.FileDone()  // must not panic
}

func TestBar(t *testing.T) {
	cases := []struct {
		pct   float64
		width int
		want  string
	}{
		{0, 4, "[░░░░]"},
		{50, 4, "[██░░]"},
		{100, 4, "[████]"},
		{150, 4, "[████]"}, // clamps to 100
		{-10, 4, "[░░░░]"}, // clamps to 0
	}
	for _, c := range cases {
		if got := bar(c.pct, c.width); got != c.want {
			t.Errorf("bar(%v, %d) = %q, want %q", c.pct, c.width, got, c.want)
		}
	}
}
