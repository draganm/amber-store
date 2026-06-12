package packstore

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/draganm/amber-store/key"
	"golang.org/x/sys/unix"
)

// ErrNotFound is returned by Get for a key that is not present in the store.
var ErrNotFound = errors.New("packstore: object not found")

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("packstore: store closed")

// DefaultSegmentSize is the default rotation threshold: the active segment is
// sealed once it reaches this many bytes.
const DefaultSegmentSize = 256 << 20 // 256 MiB

const (
	sealedSuffix = ".seg"
	activeSuffix = ".seg.active"
)

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	segmentSize int64
	sync        bool
}

func defaultConfig() config {
	return config{segmentSize: DefaultSegmentSize, sync: true}
}

// WithSegmentSize sets the rotation threshold in bytes. A single oversized
// record may push one segment past it.
func WithSegmentSize(n int64) Option {
	return func(c *config) { c.segmentSize = n }
}

// WithSync controls whether writes are fsynced for crash durability. Default
// is true; disabling it speeds bulk loads and tests.
func WithSync(b bool) Option {
	return func(c *config) { c.sync = b }
}

// activeSegment is the single append-only segment accepting writes.
type activeSegment struct {
	id    uint64
	path  string
	f     *os.File
	size  int64 // accessed only under appendMu
	index map[key.Key]activeLoc
}

// Store is an on-disk content-addressable store over segment files. It is
// safe for concurrent use. Lock ordering: appendMu before mu, never the
// reverse. appendMu serializes the write path (append, fsync, seal, Close);
// mu guards sealed/active/closed for readers.
type Store struct {
	dir  string
	dirF *os.File // holds the flock; also used for directory fsyncs
	cfg  config

	appendMu sync.Mutex
	mu       sync.RWMutex
	sealed   []*sealedSegment // ascending id; newest last
	active   *activeSegment   // nil until the first write of a session
	nextID   uint64
	closed   bool
}

// Open opens (creating if necessary) a store rooted at dir. Only one Store
// may have a given dir open at a time (flock on the directory). Sealed
// segments are mmap'd and validated; the active segment, if any, is
// tail-scanned and truncated to its last valid record.
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("packstore: creating %s: %w", dir, err)
	}
	dirF, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(dirF.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		dirF.Close()
		return nil, fmt.Errorf("packstore: %s is already open: %w", dir, err)
	}
	s := &Store{dir: dir, dirF: dirF, cfg: cfg, nextID: 1}
	if err := s.load(); err != nil {
		s.releaseDir()
		return nil, err
	}
	return s, nil
}

func (s *Store) releaseDir() {
	for _, seg := range s.sealed {
		seg.close()
	}
	s.dirF.Close() // releases the flock
}

// load scans the directory: sealed segments are opened and validated, the
// active segment (at most one) is recovered.
func (s *Store) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var activePaths []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, activeSuffix):
			activePaths = append(activePaths, name)
		case strings.HasSuffix(name, sealedSuffix):
			id, err := parseSegmentID(name, sealedSuffix)
			if err != nil {
				return err
			}
			seg, err := openSealed(filepath.Join(s.dir, name), id)
			if err != nil {
				return err
			}
			s.sealed = append(s.sealed, seg)
			if id >= s.nextID {
				s.nextID = id + 1
			}
		}
		// Anything else (e.g. .DS_Store) is ignored.
	}
	slices.SortFunc(s.sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })

	if len(activePaths) > 1 {
		return fmt.Errorf("%w: %d active segments, want at most one: %v", ErrCorrupt, len(activePaths), activePaths)
	}
	if len(activePaths) == 0 {
		return nil
	}
	return s.recoverActive(activePaths[0])
}

func parseSegmentID(name, suffix string) (uint64, error) {
	hex := strings.TrimSuffix(name, suffix)
	id, err := strconv.ParseUint(hex, 16, 64)
	if err != nil || len(hex) != 16 {
		return 0, fmt.Errorf("%w: bad segment file name %q", ErrCorrupt, name)
	}
	return id, nil
}

// recoverActive tail-scans name, then either completes a crashed seal-rename
// or truncates the file to its valid prefix and resumes it as the active
// segment.
func (s *Store) recoverActive(name string) error {
	id, err := parseSegmentID(name, activeSuffix)
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, name)
	res, err := scanActive(path)
	if err != nil {
		return err
	}
	if id >= s.nextID {
		s.nextID = id + 1
	}
	if res.sealed {
		// Crash between footer-write and rename: complete the rename.
		sealedPath := strings.TrimSuffix(path, ".active")
		if err := os.Rename(path, sealedPath); err != nil {
			return err
		}
		if err := s.dirF.Sync(); err != nil {
			return err
		}
		seg, err := openSealed(sealedPath, id)
		if err != nil {
			return err
		}
		s.sealed = append(s.sealed, seg)
		slices.SortFunc(s.sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })
		return nil
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	size := res.size
	if size < int64(len(magicHeader)) {
		// Header never became durable: reset to a fresh header.
		// This is deliberate and silent; nothing acknowledged is lost.
		if err := f.Truncate(0); err != nil {
			f.Close()
			return err
		}
		if _, err := f.WriteAt(magicHeader, 0); err != nil {
			f.Close()
			return err
		}
		size = int64(len(magicHeader))
	} else {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return err
		}
	}
	s.active = &activeSegment{id: id, path: path, f: f, size: size, index: res.index}
	return nil
}

// createActive opens the next-numbered active segment. Called under appendMu.
func (s *Store) createActive() error {
	id := s.nextID
	s.nextID++
	path := filepath.Join(s.dir, fmt.Sprintf("%016x%s", id, activeSuffix))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(magicHeader, 0); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := s.dirF.Sync(); err != nil {
		f.Close()
		return err
	}
	a := &activeSegment{id: id, path: path, f: f, size: int64(len(magicHeader)), index: make(map[key.Key]activeLoc)}
	s.mu.Lock()
	s.active = a
	s.mu.Unlock()
	return nil
}

// append writes one encoded record to the active segment (creating it if
// needed), publishes it in the active index, optionally fsyncs, and seals the
// segment if it reached the rotation threshold.
func (s *Store) append(k key.Key, rec []byte, syncNow bool) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if s.active == nil {
		if err := s.createActive(); err != nil {
			return err
		}
	}
	a := s.active
	off := a.size
	if _, err := a.f.WriteAt(rec, off); err != nil {
		return err
	}
	loc := activeLoc{
		off:   off,
		flags: rec[33],
		ulen:  binary.BigEndian.Uint32(rec[34:38]),
		slen:  binary.BigEndian.Uint32(rec[38:42]),
	}
	s.mu.Lock()
	a.index[k] = loc
	s.mu.Unlock()
	a.size = off + int64(len(rec))

	if syncNow && s.cfg.sync {
		if err := a.f.Sync(); err != nil {
			return err
		}
	}
	if a.size >= s.cfg.segmentSize {
		return s.sealActiveLocked()
	}
	return nil
}

// syncActive fsyncs the active segment, if syncing is enabled and one exists.
func (s *Store) syncActive() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	if !s.cfg.sync || s.active == nil {
		return nil
	}
	return s.active.f.Sync()
}

// sealActiveLocked seals the active segment: footer, fsync, rename to .seg,
// directory fsync, mmap. Called under appendMu. Implemented in Task 7; until
// then it is a stub that never triggers (tests use the default 256 MiB
// threshold).
func (s *Store) sealActiveLocked() error {
	return errors.New("packstore: sealing not implemented yet")
}

// Put stores a single object under k, deduplicating against existing content.
func (s *Store) Put(k key.Key, data []byte) error {
	has, err := s.Has(k)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	rec, err := encodeRecord(k, data)
	if err != nil {
		return err
	}
	return s.append(k, rec, true)
}

// Get returns the bytes stored under k, or ErrNotFound if k is absent. The
// returned slice is caller-owned.
func (s *Store) Get(k key.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.active != nil {
		if loc, ok := s.active.index[k]; ok {
			stored := make([]byte, loc.slen)
			if _, err := s.active.f.ReadAt(stored, loc.off+recHeaderSize); err != nil {
				return nil, err
			}
			return decodePayload(loc.flags, loc.ulen, stored)
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		data, found, err := s.sealed[i].get(k)
		if err != nil {
			return nil, err
		}
		if found {
			return data, nil
		}
	}
	return nil, ErrNotFound
}

// Has reports whether an object is stored under k.
func (s *Store) Has(k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if s.active != nil {
		if _, ok := s.active.index[k]; ok {
			return true, nil
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// Close fsyncs and closes the active segment (without sealing it), unmaps all
// sealed segments, and releases the directory lock.
func (s *Store) Close() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if s.active != nil {
		if err := s.active.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.active.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.active = nil
	}
	for _, seg := range s.sealed {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.sealed = nil
	if err := s.dirF.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
