package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/key"
)

// TestIngestObjects_ParityWithPack asserts that ingesting a directory stores
// exactly the objects pack would emit: every tar member is retrievable from the
// store by key with identical bytes, and the resolved root matches pack's root.
func TestIngestObjects_ParityWithPack(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Reference: pack the same tree to a tar.
	var buf bytes.Buffer
	packRoot, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	members, _ := readTar(t, buf.Bytes())

	// Ingest the same tree into a diskstore.
	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var root key.Key
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, 4, nil, &root)
	if err := store.WriteBatch(seq); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if root != packRoot {
		t.Fatalf("ingest root = %s, want pack root %s", root, packRoot)
	}

	for name, want := range members {
		raw, err := hex.DecodeString(name)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("object %s: stored bytes differ from packed bytes", k)
		}
	}
}

// cliDefaultByteOpts returns the ultracdc parameters the ingest CLI applies by
// default (see chunkFlags in main.go: 32K/128K/256K). The CLI never passes nil
// byteOpts, so a reference pack compared against a CLI ingest must use these
// explicit sizes — nil would select the library defaults (2K/10K/64K) and large
// files would chunk differently, diverging the root.
func cliDefaultByteOpts() *chunkers.ByteOpts {
	return &chunkers.ByteOpts{MinSize: 32 << 10, NormalSize: 128 << 10, MaxSize: 256 << 10}
}

// TestRunIngest_StoresRoot drives the CLI ingest command end to end and checks
// that the resulting store contains the root object pack would produce with the
// CLI's default chunk sizes.
func TestRunIngest_StoresRoot(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()

	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", "--store", storeDir, src}); err != nil {
		t.Fatal(err)
	}

	// The root pack would produce (with the CLI's default chunk sizes) must be
	// present in the store.
	var buf bytes.Buffer
	root, err := pack(src, &buf, chunkers.NewItemChunker(7), cliDefaultByteOpts(), 256)
	if err != nil {
		t.Fatal(err)
	}
	store, err := diskstore.Open(storeDir, diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	has, err := store.Has(root)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("store is missing root object %s", root)
	}
}

// TestIngestObjects_ParallelParityDeepTree ingests a deep, wide tree containing
// large multi-chunk files (forcing external blobs and multi-level file indexes)
// with many concurrent jobs, and asserts the parallel build produces exactly the
// objects and root pack would. Run with -race, the concurrent build's shared
// state (sink, semaphore, entry collection) is exercised here.
func TestIngestObjects_ParallelParityDeepTree(t *testing.T) {
	dir := t.TempDir()
	writeDeepTree(t, dir)

	// Reference: pack the same tree.
	var buf bytes.Buffer
	packRoot, err := pack(dir, &buf, chunkers.NewItemChunker(7), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	members, _ := readTar(t, buf.Bytes())

	// Ingest with maximal concurrency. A small inline threshold forces many
	// chunks of the large files into external blob files.
	store, err := diskstore.Open(t.TempDir(),
		diskstore.WithSync(false),
		diskstore.WithInlineThreshold(4<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var root key.Key
	jobs := max(runtime.NumCPU(), 4)
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, jobs, nil, &root)
	if err := store.WriteBatch(seq); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if root != packRoot {
		t.Fatalf("parallel ingest root = %s, want pack root %s", root, packRoot)
	}

	if len(members) < 50 {
		t.Fatalf("deep tree produced only %d objects; expected a large fan-out to stress concurrency", len(members))
	}

	for name, want := range members {
		raw, err := hex.DecodeString(name)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("object %s: stored bytes differ from packed bytes", k)
		}
	}
}

// TestRunIngest_JobsFlag drives the CLI ingest command with an explicit --jobs
// value and checks the resulting store contains the root pack would produce with
// the CLI's default chunk sizes.
func TestRunIngest_JobsFlag(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)
	storeDir := t.TempDir()

	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", "--store", storeDir, "--jobs", "8", src}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	root, err := pack(src, &buf, chunkers.NewItemChunker(7), cliDefaultByteOpts(), 256)
	if err != nil {
		t.Fatal(err)
	}
	store, err := diskstore.Open(storeDir, diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	has, err := store.Has(root)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("store is missing root object %s", root)
	}
}

// writeDeepTree creates a deterministic, deep and wide directory tree: nested
// subdirectories each holding several small files plus one large multi-chunk
// file, so ingestion fans out across many files and subtrees.
func writeDeepTree(t *testing.T, root string) {
	t.Helper()
	var build func(dir string, depth, seed int)
	build = func(dir string, depth, seed int) {
		// A large file forces CDC into many chunks and a multi-level file index.
		large := make([]byte, 256<<10)
		fillPseudoRandom(large, uint64(seed)*1_000_003+7)
		if err := os.WriteFile(filepath.Join(dir, "large.bin"), large, 0o644); err != nil {
			t.Fatal(err)
		}
		// Several small files create multiple DirLeaf/DirNode objects.
		for i := range 6 {
			name := fmt.Sprintf("file-%02d.txt", i)
			content := fmt.Appendf(nil, "depth=%d seed=%d index=%d payload", depth, seed, i)
			if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if depth == 0 {
			return
		}
		for i := range 3 {
			sub := filepath.Join(dir, fmt.Sprintf("sub-%d", i))
			if err := os.Mkdir(sub, 0o755); err != nil {
				t.Fatal(err)
			}
			build(sub, depth-1, seed*10+i+1)
		}
	}
	build(root, 3, 1)
}

// fillPseudoRandom fills b with a deterministic byte stream (a splitmix64-style
// generator) so the large test files have enough entropy for content-defined
// chunking to find boundaries, while remaining reproducible.
func fillPseudoRandom(b []byte, seed uint64) {
	x := seed
	for i := 0; i+8 <= len(b); i += 8 {
		x += 0x9E3779B97F4A7C15
		z := x
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z = z ^ (z >> 31)
		binary.LittleEndian.PutUint64(b[i:], z)
	}
}

func TestRunIngest_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", "--store", t.TempDir(), f}); err == nil {
		t.Errorf("expected error ingesting a non-directory")
	}
}

func TestRunIngest_RequiresStoreFlag(t *testing.T) {
	app := newApp()
	if err := app.Run([]string{"amber-store", "ingest", t.TempDir()}); err == nil {
		t.Errorf("expected error without --store flag")
	}
}

// TestIngestObjects_ReportsProgress checks the instrumented build feeds the
// Progress tracker: bytesDone equals total regular-file bytes and filesDone
// equals the file count.
func TestIngestObjects_ReportsProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}

	store, err := diskstore.Open(t.TempDir(), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	p := NewProgress(2, 11)
	var root key.Key
	seq := ingestObjects(dir, chunkers.NewItemChunker(7), nil, 256, 2, p, &root)
	if err := store.WriteBatch(seq); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if got := p.bytesDone.Load(); got != 11 {
		t.Errorf("bytesDone = %d, want 11", got)
	}
	if got := p.filesDone.Load(); got != 2 {
		t.Errorf("filesDone = %d, want 2", got)
	}
}

// TestRunIngest_PrintsRootToStdout asserts the ingest command writes the root
// key (and only the root key) to the app writer (stdout).
func TestRunIngest_PrintsRootToStdout(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	if err := app.Run([]string{"amber-store", "ingest", "--store", t.TempDir(), "--no-progress", src}); err != nil {
		t.Fatal(err)
	}

	var pb bytes.Buffer
	root, err := pack(src, &pb, chunkers.NewItemChunker(7), cliDefaultByteOpts(), 256)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(buf.String()); got != root.String() {
		t.Fatalf("stdout = %q, want root %q", got, root.String())
	}
}

// TestRunIngest_DeterministicAcrossWriters asserts the resolved root is
// independent of writer-pool size.
func TestRunIngest_DeterministicAcrossWriters(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)

	roots := make([]string, 0, 2)
	for _, w := range []string{"1", "8"} {
		var buf bytes.Buffer
		app := newApp()
		app.Writer = &buf
		args := []string{"amber-store", "ingest", "--store", t.TempDir(), "--no-progress", "--writers", w, src}
		if err := app.Run(args); err != nil {
			t.Fatalf("--writers %s: %v", w, err)
		}
		roots = append(roots, strings.TrimSpace(buf.String()))
	}
	if roots[0] != roots[1] {
		t.Fatalf("root differs across --writers: %q vs %q", roots[0], roots[1])
	}
}
