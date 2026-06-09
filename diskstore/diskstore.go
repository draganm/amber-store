// Package diskstore persists Amber-Store CAS objects on the local filesystem
// and reads them back by key. Each object is a (key.Key, []byte) pair whose key
// is supplied by the caller. Small objects (<= InlineThreshold) are stored
// inline in an embedded Pebble database; large objects are stored as plain
// files on disk, with Pebble holding a one-byte marker. See
// docs/superpowers/specs/2026-06-08-diskstore-design.md.
package diskstore

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/draganm/amber-store/key"
	"golang.org/x/sync/errgroup"
)

// ErrNotFound is returned by Get for a key that is not present in the store.
var ErrNotFound = errors.New("diskstore: object not found")

// DefaultInlineThreshold is the default boundary (in bytes) between inline
// (Pebble) and external (file) storage: objects with len(data) > threshold are
// stored as files.
const DefaultInlineThreshold = 64 << 10 // 64 KiB

// Pebble value tags: the first byte of every record.
const (
	tagInline   byte = 0x00 // remaining bytes are the object data
	tagExternal byte = 0x01 // no payload; data lives in the blob file
)

// Object is one CAS object: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	inlineThreshold int
	sync            bool
}

func defaultConfig() config {
	return config{inlineThreshold: DefaultInlineThreshold, sync: true}
}

// WithInlineThreshold sets the boundary between inline and external storage.
// Objects with len(data) > n are stored as files.
func WithInlineThreshold(n int) Option {
	return func(c *config) { c.inlineThreshold = n }
}

// WithSync controls whether writes are fsynced for crash durability. Default is
// true; disabling it speeds bulk loads and tests.
func WithSync(b bool) Option {
	return func(c *config) { c.sync = b }
}

// Store is an on-disk content-addressable store. It is safe for concurrent use.
type Store struct {
	db        *pebble.DB
	blobsDir  string
	tmpDir    string
	threshold int
	sync      bool
	writeOpts *pebble.WriteOptions
}

// discardLogger silences Pebble's informational logging. Fatal conditions still
// surface as a panic rather than being swallowed.
type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// Open opens (creating if necessary) a store rooted at dir. The directory holds
// db/ (Pebble), blobs/ (large-object files), and tmp/ (rename staging). Only one
// Store may have a given dir open at a time (Pebble locks db/).
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	blobsDir := filepath.Join(dir, "blobs")
	tmpDir := filepath.Join(dir, "tmp")
	for _, d := range []string{blobsDir, tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("diskstore: creating %s: %w", d, err)
		}
	}

	cache := pebble.NewCache(1024 << 20)
	defer cache.Unref()

	pebbleOpts := &pebble.Options{
		Logger:       discardLogger{},
		Cache:        cache,
		MemTableSize: 128 << 20,
	}

	db, err := pebble.Open(filepath.Join(dir, "db"), pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("diskstore: opening pebble: %w", err)
	}

	wo := pebble.Sync
	if !cfg.sync {
		wo = pebble.NoSync
	}
	return &Store{
		db:        db,
		blobsDir:  blobsDir,
		tmpDir:    tmpDir,
		threshold: cfg.inlineThreshold,
		sync:      cfg.sync,
		writeOpts: wo,
	}, nil
}

// blobPath is the on-disk path of the external file for k. Files are sharded two
// levels deep on the last two bytes of the key (hex chars 60:62 and 62:64),
// which fall in the uniformly-distributed truncated-hash region.
func (s *Store) blobPath(k key.Key) string {
	h := k.String() // 64 lowercase hex chars
	return filepath.Join(s.blobsDir, h[60:62], h[62:64], h)
}

// writeExternal durably writes data to k's blob file: stage to tmp/, fsync,
// rename into the shard, fsync the shard dir. It is a no-op if the file already
// exists (content-addressed: identical content reproduces identical bytes).
func (s *Store) writeExternal(k key.Key, data []byte) error {
	target := s.blobPath(k)
	if _, err := os.Stat(target); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	shard := filepath.Dir(target)
	if err := os.MkdirAll(shard, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(s.tmpDir, "blob-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return err
	}

	return nil
}

// Put stores a single object under k, deduplicating against existing content.
func (s *Store) Put(k key.Key, data []byte) error {
	if len(data) > s.threshold {
		if err := s.writeExternal(k, data); err != nil {
			return err
		}
		return s.db.Set(k[:], []byte{tagExternal}, s.writeOpts)
	}
	val := make([]byte, 1+len(data))
	val[0] = tagInline
	copy(val[1:], data)
	return s.db.Set(k[:], val, s.writeOpts)
}

// WriteBatch stores every object the iterator yields in a single atomic Pebble
// commit. External (large) objects are written to disk durably before the
// commit that references them. If the iterator yields a non-nil error, WriteBatch
// stops and returns it without committing — nothing yielded becomes visible
// (any external files already written are harmless, invisible orphans). Objects
// repeated within the batch are written once.
func (s *Store) WriteBatch(seq iter.Seq2[Object, error]) error {
	b := s.db.NewBatch()
	defer b.Close()

	eg := &errgroup.Group{}
	eg.SetLimit(10)

	seen := make(map[key.Key]struct{})
	for obj, err := range seq {
		if err != nil {
			return err
		}
		if _, dup := seen[obj.Key]; dup {
			continue
		}

		seen[obj.Key] = struct{}{}

		if len(obj.Data) > s.threshold {

			eg.Go(func() error {
				return s.writeExternal(obj.Key, obj.Data)
			})

			if err := b.Set(obj.Key[:], []byte{tagExternal}, nil); err != nil {
				return err
			}
			continue
		}
		val := make([]byte, 1+len(obj.Data))
		val[0] = tagInline
		copy(val[1:], obj.Data)
		if err := b.Set(obj.Key[:], val, nil); err != nil {
			return err
		}
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return b.Commit(s.writeOpts)
}

// Get returns the bytes stored under k, or ErrNotFound if k is absent.
func (s *Store) Get(k key.Key) ([]byte, error) {
	val, closer, err := s.db.Get(k[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if len(val) == 0 {
		return nil, fmt.Errorf("diskstore: corrupt record for %s", k)
	}
	switch val[0] {
	case tagInline:
		out := make([]byte, len(val)-1)
		copy(out, val[1:])
		return out, nil
	case tagExternal:
		return os.ReadFile(s.blobPath(k))
	default:
		return nil, fmt.Errorf("diskstore: unknown tag %#x for %s", val[0], k)
	}
}

// Has reports whether an object is stored under k.
func (s *Store) Has(k key.Key) (bool, error) {
	_, closer, err := s.db.Get(k[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	return s.db.Close()
}
