// Command packbench reproduces the pack-assembly half of a push against a
// daemon's on-disk store, without any network or have/want negotiation: it
// resolves a ref to its root, walks every reachable object (treating the whole
// set as missing), bins them into byte-balanced packs exactly as remotesync
// does, and writes each pack to a file — timing each phase so a slow push can
// be attributed to walking, reading, or writing.
//
// It opens the packstore directly, which takes the store's exclusive directory
// lock: the daemon must be stopped first.
//
//	packbench -store /path/to/daemon-store -ref main -out /tmp/packs
//	packbench -store /path/to/daemon-store -ref main -out /tmp/packs -jobs 8
//	packbench -store /path/to/daemon-store -ref main -write=false   # read-only timing
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remotesync"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "packbench: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		storeDir   string
		ref        string
		outDir     string
		jobs       int
		batchBytes uint64
		write      bool
		diskOrder  bool
		passes     int
	)
	flag.StringVar(&storeDir, "store", "", "daemon store directory (the one holding packstore/ and refs/)")
	flag.StringVar(&ref, "ref", "", "ref name to assemble packs for")
	flag.StringVar(&outDir, "out", "", "directory to write pack files into (created if needed)")
	flag.IntVar(&jobs, "jobs", remotesync.DefaultJobs, "concurrent record reads per pack")
	flag.Uint64Var(&batchBytes, "batch-bytes", remotesync.DefaultBatchBytes, "per-pack stored-byte target")
	flag.BoolVar(&write, "write", true, "write pack files to -out; set false to time reads only")
	flag.BoolVar(&diskOrder, "disk-order", true, "read in on-disk layout order (set false to read in reachable order)")
	flag.IntVar(&passes, "passes", 1, "repeat the read phase N times (read-only); a warm pass 2+ exposes the page-cache ceiling vs cold disk")
	flag.Parse()

	if storeDir == "" || ref == "" {
		flag.Usage()
		return fmt.Errorf("both -store and -ref are required")
	}
	if jobs < 1 {
		jobs = 1
	}
	if write && outDir == "" {
		return fmt.Errorf("-out is required unless -write=false")
	}

	// Resolve the ref's root from the refstore (Pebble, no flock).
	refs, err := refstore.Open(filepath.Join(storeDir, "refs"), false)
	if err != nil {
		return fmt.Errorf("opening refs: %w", err)
	}
	defer refs.Close()
	root, err := resolveRef(refs, ref)
	if err != nil {
		return err
	}

	// Open the CAS store. This takes the exclusive directory lock.
	tOpen := time.Now()
	store, err := packstore.Open(filepath.Join(storeDir, "packstore"), packstore.WithSync(false))
	if err != nil {
		return fmt.Errorf("opening packstore (is the daemon still running? stop it first): %w", err)
	}
	defer store.Close()
	openDur := time.Since(tOpen)

	if write {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return fmt.Errorf("creating out dir: %w", err)
		}
	}

	fmt.Printf("ref %q -> root %s\n", ref, root)
	fmt.Printf("opened store in %s\n", openDur.Round(time.Millisecond))

	// Phase 1: walk every reachable object. ReachableKeys needs decoded node
	// bytes to find children, so it reads through Get (which decompresses).
	tWalk := time.Now()
	keys, err := fstree.ReachableKeys(root, store.Get)
	if err != nil {
		return fmt.Errorf("walking reachable objects: %w", err)
	}
	walkDur := time.Since(tWalk)
	fmt.Printf("walked %d reachable objects in %s\n", len(keys), walkDur.Round(time.Millisecond))

	// Order reads by on-disk layout so each pack sweeps the segment files
	// sequentially, matching remotesync.Push. -disk-order=false reproduces the
	// old reachable-order (random) access for comparison.
	if diskOrder {
		tSort := time.Now()
		store.SortByLocation(keys)
		fmt.Printf("sorted by disk layout in %s\n", time.Since(tSort).Round(time.Millisecond))
	} else {
		fmt.Println("disk-order off: reading in reachable order")
	}

	// Phase 2: bin into byte-balanced packs, exactly as remotesync.Push does.
	tBatch := time.Now()
	batches := remotesync.Batches(keys, batchBytes, remotesync.PushSizer(store))
	batchDur := time.Since(tBatch)
	fmt.Printf("planned %d packs (target %s/pack) in %s\n",
		len(batches), humanBytes(batchBytes), batchDur.Round(time.Millisecond))

	// Phase 3: assemble each pack — parallel reads, then a serial write — and
	// time reads and writes separately so the bottleneck is visible.
	var (
		totalBytes int64
		readWall   time.Duration
		writeWall  time.Duration
	)
	tAssemble := time.Now()
	for i, batch := range batches {
		recs, dur, err := readPack(store, batch, jobs)
		if err != nil {
			return fmt.Errorf("pack %d: %w", i, err)
		}
		readWall += dur
		var packBytes int64
		for _, r := range recs {
			packBytes += int64(len(r))
		}
		totalBytes += packBytes

		wd, err := writePack(outDir, i, recs, write)
		if err != nil {
			return fmt.Errorf("pack %d: %w", i, err)
		}
		writeWall += wd
	}
	assembleDur := time.Since(tAssemble)

	fmt.Println("---")
	fmt.Printf("objects:        %d\n", len(keys))
	fmt.Printf("packs:          %d\n", len(batches))
	fmt.Printf("stored bytes:   %s\n", humanBytes(uint64(totalBytes)))
	fmt.Printf("read wall:      %s (%s)\n", readWall.Round(time.Millisecond), throughput(totalBytes, readWall))
	if write {
		fmt.Printf("write wall:     %s (%s)\n", writeWall.Round(time.Millisecond), throughput(totalBytes, writeWall))
	}
	fmt.Printf("assemble total: %s (%s)\n", assembleDur.Round(time.Millisecond), throughput(totalBytes, assembleDur))

	// Extra read-only passes over the same keys. If pass 2+ is much faster than
	// pass 1, the first pass paid cold-disk misses (working set exceeds RAM); if
	// passes are all similar, the data is warm and the floor is per-object/CPU
	// overhead, not disk.
	for p := 2; p <= passes; p++ {
		var rw time.Duration
		for _, batch := range batches {
			_, dur, err := readPack(store, batch, jobs)
			if err != nil {
				return fmt.Errorf("pass %d: %w", p, err)
			}
			rw += dur
		}
		fmt.Printf("read pass %d:    %s (%s)\n", p, rw.Round(time.Millisecond), throughput(totalBytes, rw))
	}
	return nil
}

// resolveRef looks up ref's root key, listing available refs on a miss.
func resolveRef(refs *refstore.Store, ref string) (key.Key, error) {
	data, err := refs.Get(ref)
	if err != nil {
		if names, lerr := refNames(refs); lerr == nil {
			return key.Key{}, fmt.Errorf("ref %q not found; available: %v", ref, names)
		}
		return key.Key{}, fmt.Errorf("ref %q not found: %w", ref, err)
	}
	r, err := reference.Decode(data)
	if err != nil {
		return key.Key{}, fmt.Errorf("decoding ref %q: %w", ref, err)
	}
	return key.Parse(r.Key)
}

func refNames(refs *refstore.Store) ([]string, error) {
	recs, err := refs.All()
	if err != nil {
		return nil, err
	}
	names := make([]string, len(recs))
	for i, r := range recs {
		names[i] = r.Name
	}
	return names, nil
}

// readPack reads every record of a pack into order, up to jobs reads at once,
// and returns the records plus the wall time spent reading.
func readPack(store *packstore.Store, batch []key.Key, jobs int) ([][]byte, time.Duration, error) {
	recs := make([][]byte, len(batch))
	t := time.Now()
	g := new(errgroup.Group)
	g.SetLimit(jobs)
	for i, k := range batch {
		g.Go(func() error {
			rec, err := store.GetRecord(k)
			if err != nil {
				return fmt.Errorf("reading %s: %w", k, err)
			}
			recs[i] = rec
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, err
	}
	return recs, time.Since(t), nil
}

// writePack serializes records into one amberpack and returns the wall time
// spent writing. With write=false it serializes to the bit bucket, timing the
// framing without disk cost.
func writePack(outDir string, idx int, recs [][]byte, write bool) (time.Duration, error) {
	t := time.Now()
	if !write {
		w := amberpack.NewWriter(discard{})
		for _, r := range recs {
			if err := w.AddRecord(r); err != nil {
				return 0, err
			}
		}
		if err := w.Close(); err != nil {
			return 0, err
		}
		return time.Since(t), nil
	}
	path := filepath.Join(outDir, fmt.Sprintf("pack-%05d.amberpack", idx))
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	w := amberpack.NewWriter(f)
	for _, r := range recs {
		if err := w.AddRecord(r); err != nil {
			f.Close()
			return 0, err
		}
	}
	if err := w.Close(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return time.Since(t), nil
}

// discard is an io.Writer that throws bytes away (avoids importing io just for
// io.Discard's interface, and keeps intent explicit).
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

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
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func throughput(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "n/a"
	}
	mbps := float64(bytes) / (1024 * 1024) / d.Seconds()
	return fmt.Sprintf("%.0f MiB/s", mbps)
}
