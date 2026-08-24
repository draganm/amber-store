// Package gc implements the collector of architecture/simple-gc.md: per-root
// closure files of key tails under <store>/closures/, their union in RAM, a
// removal lock serializing reference writes against pack deletion, upload
// leases, and the scoring/reaping cycle. Wire protocol and pack formats are
// unchanged; the packstore stays exactly as it is.
package gc

import (
	"encoding/binary"
	"runtime"
	"time"

	"github.com/draganm/amber-store/key"
)

// Tail is key[24:32] as a big-endian u64 — the last 8 bytes of a key,
// always inside the uniformly distributed hash, and already what the
// packstore's filters hash. Two keys share a tail with probability 2⁻⁶⁴;
// for membership tests that is exact.
func Tail(k key.Key) uint64 {
	return binary.BigEndian.Uint64(k[key.Size-8:])
}

const (
	// DefaultGrace is the minimum age of a sealed pack before it can be
	// reaped, and the upload-lease idle timeout.
	DefaultGrace = time.Hour
	// DefaultGarbage is the selection line: an eligible pack with more
	// garbage than this is reaped.
	DefaultGarbage = 0.5
	// minFreeGarbage is the selection line under free-space pressure.
	minFreeGarbage = 0.1
)

// Options configure a Collector. The zero value means: 1 h grace, 0.5
// garbage line, min-free at 5 % of the filesystem, unlimited copy rate, no
// background cycles, GOMAXPROCS parallelism, fsync on.
type Options struct {
	Grace    time.Duration // pack eligibility age; lease idle timeout
	Garbage  float64       // reap packs with more garbage than this
	MinFree  uint64        // free-space floor in bytes; 0 = 5 % of the filesystem
	Rate     int64         // copier bandwidth cap in bytes/s; 0 = unlimited
	Interval time.Duration // time between background cycles; 0 = none
	Jobs     int           // walk and score parallelism; <= 0 = GOMAXPROCS
	NoSync   bool          // skip fsyncs (mirrors packstore.WithSync(false); tests)
}

func (o Options) withDefaults() Options {
	if o.Grace <= 0 {
		o.Grace = DefaultGrace
	}
	if o.Garbage <= 0 {
		o.Garbage = DefaultGarbage
	}
	if o.Jobs <= 0 {
		o.Jobs = runtime.GOMAXPROCS(0)
	}
	return o
}
