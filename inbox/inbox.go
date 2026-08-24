package inbox

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/zeebo/blake3"
)

// Inbox receives packs, persists them durably, and drains them into a
// packstore from a worker pool. It is safe for concurrent use.
type Inbox struct {
	dir     string
	tmpDir  string
	failDir string
	store   *packstore.Store
	log     *slog.Logger

	mu     sync.Mutex
	cond   *sync.Cond
	work   []workItem
	groups map[key.Key]int // root -> count of unprocessed entries
	closed bool

	// Upload leases (architecture/simple-gc.md §Safety): one per root with
	// packs in flight, taken on the first pack, refreshed per pack, released
	// by the reference PUT (ReleaseLease). An abandoned upload's lease is
	// never released here — it expires on the collector's idle timeout.
	leaser func(root key.Key) Lease
	leases map[key.Key]Lease

	wg sync.WaitGroup
}

// Lease is the inbox's view of a gc upload lease (*gc.Lease satisfies it).
type Lease interface {
	Refresh()
	Release()
}

// An Option configures Open.
type Option func(*Inbox)

// WithLeaser installs the collector hook that covers in-flight uploads
// against the reaping horizon. It must be given to Open, not installed
// later: leases live only in memory, so an entry recovered from a previous
// run gets its lease exactly once — inside Open, before any worker can
// drain it. Installing the hook after workers start would leave a
// recovered root that drained first covered by grace alone.
func WithLeaser(lease func(root key.Key) Lease) Option {
	return func(ib *Inbox) { ib.leaser = lease }
}

// LeaserOf adapts a concrete lease constructor (gc.Collector.Lease) to the
// WithLeaser hook.
func LeaserOf[L Lease](lease func(root key.Key) L) func(root key.Key) Lease {
	return func(root key.Key) Lease { return lease(root) }
}

// Leased reports whether the inbox takes upload leases.
func (ib *Inbox) Leased() bool { return ib.leaser != nil }

// leaseLocked takes or refreshes root's lease. Callers hold mu.
func (ib *Inbox) leaseLocked(root key.Key) {
	if ib.leaser == nil {
		return
	}
	if l, ok := ib.leases[root]; ok {
		l.Refresh()
		return
	}
	ib.leases[root] = ib.leaser(root)
}

// ReleaseLease drops root's upload lease; the reference PUT handler calls
// it once the reference naming root is committed. Unknown roots are a
// no-op (a ref PUT needs no preceding push).
func (ib *Inbox) ReleaseLease(root key.Key) {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	if l, ok := ib.leases[root]; ok {
		l.Release()
		delete(ib.leases, root)
	}
}

type workItem struct {
	name string
	root key.Key
}

// Open prepares the inbox directory tree, recovers entries left by a previous
// run — leasing their roots when a leaser is configured — and starts
// `workers` processing goroutines. workers <= 0 means runtime.GOMAXPROCS(0).
// A nil log discards.
func Open(dir string, store *packstore.Store, workers int, log *slog.Logger, opts ...Option) (*Inbox, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	ib := &Inbox{
		dir:     dir,
		tmpDir:  filepath.Join(dir, "tmp"),
		failDir: filepath.Join(dir, "failed"),
		store:   store,
		log:     log,
		groups:  map[key.Key]int{},
		leases:  map[key.Key]Lease{},
	}
	for _, opt := range opts {
		opt(ib)
	}
	ib.cond = sync.NewCond(&ib.mu)
	for _, d := range []string{ib.dir, ib.tmpDir, ib.failDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := ib.recover(); err != nil {
		return nil, err
	}
	// Recovered entries bypass Commit, so lease their roots here — before
	// the first worker can process one and drop its group. This is the only
	// chance: leases do not survive a restart.
	for root := range ib.groups {
		ib.leaseLocked(root)
	}
	for range workers {
		ib.wg.Add(1)
		go ib.processLoop()
	}
	return ib, nil
}

// Stage writes meta and streams body into a fresh tmp file, returning the tmp
// path, blake3 of the body bytes (only the body feeds the hash), and the body
// length. The caller authorizes the request against the hash, then calls
// Commit or Discard.
func (ib *Inbox) Stage(meta Meta, body io.Reader) (tmpPath string, bodyHash []byte, n int64, err error) {
	f, err := os.CreateTemp(ib.tmpDir, "stage-*")
	if err != nil {
		return "", nil, 0, err
	}
	tmpPath = f.Name()
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(tmpPath)
		}
	}()
	if err := writeMetaHeader(f, meta); err != nil {
		return "", nil, 0, err
	}
	h := blake3.New()
	n, err = io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		return "", nil, 0, err
	}
	if err := f.Sync(); err != nil {
		return "", nil, 0, err
	}
	if err := f.Close(); err != nil {
		return "", nil, 0, err
	}
	committed = true
	return tmpPath, h.Sum(nil), n, nil
}

// Discard removes a staged tmp file (authorization failed or oversize).
func (ib *Inbox) Discard(tmpPath string) {
	_ = os.Remove(tmpPath)
}

// Commit publishes a staged tmp file under its content-addressed name and
// enqueues it. It is idempotent: if an entry with the same body already exists,
// the tmp file is discarded and added is false. root updates the barrier
// accounting and must equal the root staged into the file.
func (ib *Inbox) Commit(tmpPath string, bodyHash []byte, root key.Key) (added bool, err error) {
	name := hex.EncodeToString(bodyHash) + ".pack"
	dst := filepath.Join(ib.dir, name)
	switch _, statErr := os.Stat(dst); {
	case statErr == nil:
		ib.Discard(tmpPath)
		// The upload is alive even when it re-sends a pack (a resumed
		// push): the lease refreshes per received pack, not per new entry.
		ib.mu.Lock()
		ib.leaseLocked(root)
		ib.mu.Unlock()
		return false, nil
	case !errors.Is(statErr, os.ErrNotExist):
		return false, statErr
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return false, err
	}
	if err := syncDir(ib.dir); err != nil {
		return false, err
	}
	ib.mu.Lock()
	ib.groups[root]++
	ib.leaseLocked(root)
	ib.work = append(ib.work, workItem{name: name, root: root})
	ib.cond.Broadcast()
	ib.mu.Unlock()
	return true, nil
}

// WaitFor blocks until no entries tagged with root remain unprocessed. With an
// empty group it returns immediately.
func (ib *Inbox) WaitFor(root key.Key) {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	for ib.groups[root] > 0 {
		ib.cond.Wait()
	}
}

// Close stops accepting new work, drains what is already queued, and waits for
// the processing goroutines to exit. Staged-but-uncommitted tmp files are left
// for the next Open to sweep.
func (ib *Inbox) Close() error {
	ib.mu.Lock()
	ib.closed = true
	ib.cond.Broadcast()
	ib.mu.Unlock()
	ib.wg.Wait()
	// Committed-but-unreferenced entries survive on disk and re-take their
	// leases at the next Open; holding these would outlive the collector.
	ib.mu.Lock()
	for root, l := range ib.leases {
		l.Release()
		delete(ib.leases, root)
	}
	ib.mu.Unlock()
	return nil
}

// recover sweeps partial transfers and enqueues committed entries from a
// previous run.
func (ib *Inbox) recover() error {
	tmps, err := os.ReadDir(ib.tmpDir)
	if err != nil {
		return err
	}
	for _, e := range tmps {
		_ = os.Remove(filepath.Join(ib.tmpDir, e.Name()))
	}
	entries, err := os.ReadDir(ib.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		root, err := ib.readRoot(e.Name())
		if err != nil {
			ib.log.Error("inbox: unreadable entry on recovery, quarantining", "name", e.Name(), "error", err)
			_ = os.Rename(filepath.Join(ib.dir, e.Name()), filepath.Join(ib.failDir, e.Name()))
			continue
		}
		ib.groups[root]++
		ib.work = append(ib.work, workItem{name: e.Name(), root: root})
	}
	return nil
}

// readRoot reads just the meta header of an entry and returns its root key.
func (ib *Inbox) readRoot(name string) (key.Key, error) {
	f, err := os.Open(filepath.Join(ib.dir, name))
	if err != nil {
		return key.Key{}, err
	}
	defer f.Close()
	m, err := readMetaHeader(f)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(m.Root)
}

func (ib *Inbox) processLoop() {
	defer ib.wg.Done()
	for {
		ib.mu.Lock()
		for len(ib.work) == 0 && !ib.closed {
			ib.cond.Wait()
		}
		if len(ib.work) == 0 && ib.closed {
			ib.mu.Unlock()
			return
		}
		item := ib.work[0]
		ib.work = ib.work[1:]
		ib.mu.Unlock()

		ib.process(item.name)

		ib.mu.Lock()
		if n := ib.groups[item.root]; n > 0 {
			if n == 1 {
				delete(ib.groups, item.root)
			} else {
				ib.groups[item.root] = n - 1
			}
		}
		ib.cond.Broadcast()
		ib.mu.Unlock()
	}
}

// process ingests one entry into the store. On success the file is removed; on
// a decode/verify error it is quarantined under failed/.
func (ib *Inbox) process(name string) {
	path := filepath.Join(ib.dir, name)
	f, err := os.Open(path)
	if err != nil {
		ib.log.Error("inbox: opening entry failed", "name", name, "error", err)
		return
	}
	if _, err := readMetaHeader(f); err != nil {
		f.Close()
		ib.quarantine(name, err)
		return
	}
	rd := amberpack.NewReader(f) // positioned at the body
	seq := func(yield func(packstore.Object, error) bool) {
		for o, err := range rd.All() {
			if err != nil {
				yield(packstore.Object{}, err)
				return
			}
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return
			}
		}
	}
	_, werr := ib.store.WriteParallel(seq, packstore.WriteOpts{Verify: true})
	f.Close()
	if werr != nil {
		ib.quarantine(name, werr)
		return
	}
	if err := os.Remove(path); err != nil {
		ib.log.Error("inbox: removing processed entry failed", "name", name, "error", err)
	}
}

func (ib *Inbox) quarantine(name string, cause error) {
	ib.log.Error("inbox: entry failed processing, quarantining", "name", name, "error", cause)
	if err := os.Rename(filepath.Join(ib.dir, name), filepath.Join(ib.failDir, name)); err != nil {
		ib.log.Error("inbox: quarantine rename failed", "name", name, "error", err)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
