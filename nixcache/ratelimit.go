package nixcache

import (
	"io"
	"sync"
	"time"
)

// byteLimiter is a token bucket over bytes with burst = one second of rate.
type byteLimiter struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newByteLimiter(bytesPerSec int64) *byteLimiter {
	return &byteLimiter{rate: float64(bytesPerSec), tokens: float64(bytesPerSec), last: time.Now()}
}

// wait blocks until n bytes may pass. n may exceed the burst. The deficit
// is simply slept off, so callers need no chunking.
func (l *byteLimiter) wait(n int) {
	l.mu.Lock()
	now := time.Now()
	l.tokens = min(l.rate, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	l.tokens -= float64(n)
	var sleep time.Duration
	if l.tokens < 0 {
		sleep = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	time.Sleep(sleep)
}

type limitedWriter struct {
	w io.Writer
	l *byteLimiter
}

func (lw *limitedWriter) Write(b []byte) (int, error) {
	lw.l.wait(len(b))
	return lw.w.Write(b)
}
