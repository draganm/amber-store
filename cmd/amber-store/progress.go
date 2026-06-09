package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Progress tracks ingest progress — bytes read from source content and regular
// files completed — against totals from the pre-scan, and renders a one-line
// bar. A nil *Progress is a no-op, so callers that disable progress pass nil.
type Progress struct {
	totalFiles int64
	totalBytes int64
	bytesDone  atomic.Uint64
	filesDone  atomic.Uint64
}

// NewProgress returns a Progress sized by the pre-scan totals.
func NewProgress(totalFiles, totalBytes int64) *Progress {
	return &Progress{totalFiles: totalFiles, totalBytes: totalBytes}
}

// AddBytes records n bytes read from source content. nil-safe.
func (p *Progress) AddBytes(n int) {
	if p == nil {
		return
	}
	p.bytesDone.Add(uint64(n))
}

// FileDone records one completed regular file. nil-safe.
func (p *Progress) FileDone() {
	if p == nil {
		return
	}
	p.filesDone.Add(1)
}

// render builds the progress line for the given elapsed time. It is pure (no
// clock, no I/O), so the render loop and tests share it. Throughput and ETA use
// the overall average rate (bytes/elapsed), which is smooth and deterministic.
// When every byte is read but the call has not yet returned, it shows a
// "finalizing" suffix.
func (p *Progress) render(elapsed time.Duration) string {
	done := p.bytesDone.Load()
	files := p.filesDone.Load()
	total := uint64(p.totalBytes)

	secs := elapsed.Seconds()
	var rate float64
	if secs > 0 {
		rate = float64(done) / secs
	}

	pct := 100.0
	if total > 0 {
		pct = float64(done) / float64(total) * 100
		if pct > 100 {
			pct = 100
		}
	}

	eta := "--:--"
	if total > 0 && done > 0 && done < total && rate > 0 {
		remain := time.Duration(float64(total-done)/rate) * time.Second
		eta = fmtDuration(remain)
	}

	tail := ""
	if total > 0 && done >= total {
		tail = "  finalizing…"
	}

	return fmt.Sprintf("ingest %5.1f%% %s  %s/%s  %s/s  elapsed %s  eta %s  files %d/%d%s",
		pct,
		bar(pct, 16),
		humanBytes(done), humanBytes(total),
		humanBytes(uint64(rate)),
		fmtDuration(elapsed), eta,
		files, p.totalFiles,
		tail,
	)
}

// Run renders progress to w until ctx is cancelled, using start as t0. On a TTY
// it redraws one line in place and clears it on exit; on a non-TTY it prints a
// plain line at a slower cadence. An initial frame is drawn immediately so even
// a sub-interval ingest shows something, and on exit a non-TTY run prints a
// final summary line (a TTY clears its in-place bar so the root prints clean).
// start is injected so the caller owns the clock.
func (p *Progress) Run(ctx context.Context, w io.Writer, start time.Time, isTTY bool) {
	interval := 150 * time.Millisecond
	if !isTTY {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	draw := func() {
		line := p.render(time.Since(start))
		if isTTY {
			fmt.Fprintf(w, "\r\033[K%s", line)
		} else {
			fmt.Fprintf(w, "%s\n", line)
		}
	}
	draw() // immediate feedback, before the first tick
	for {
		select {
		case <-ctx.Done():
			if isTTY {
				fmt.Fprint(w, "\r\033[K") // erase the in-place bar
			} else {
				draw() // leave a final summary line in the log
			}
			return
		case <-t.C:
			draw()
		}
	}
}

// bar renders a width-cell progress bar for pct in [0,100].
func bar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

// humanBytes formats n with binary (KiB/MiB/…) units.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	val := float64(n) / float64(div)
	// Promote to the next unit when rounding to one decimal would otherwise
	// display at the boundary, e.g. 1048575 as "1024.0 KiB" instead of "1.0 MiB".
	if val >= 1023.95 && exp < len("KMGTPE")-1 {
		div *= unit
		exp++
		val = float64(n) / float64(div)
	}
	return fmt.Sprintf("%.1f %ciB", val, "KMGTPE"[exp])
}

// fmtDuration formats d as M:SS or H:MM:SS, clamping negatives to zero.
func fmtDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

// isTerminal reports whether f is a character device (a terminal), used to pick
// between an in-place redraw and plain progress lines.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
