package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/amberignore"
	"github.com/draganm/amber-store/key"
)

// collectSequential builds the tree at dir with the sequential driver and returns
// the root plus a map of every emitted object's key to its bytes. It is the
// reference oracle the parallel build and the CLI are checked against.
func collectSequential(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	emit := func(o fstree.Object) error {
		objs[o.Key] = append([]byte(nil), o.Bytes...)
		return nil
	}
	d := &driver{ic: ic, byteOpts: byteOpts, xattrInlineMax: xattrInlineMax}
	root, err := d.buildDir(dir, ign, emit)
	if err != nil {
		t.Fatalf("sequential build: %v", err)
	}
	return root, objs
}

// collectParallel drains ingestObjects into a key->bytes map and returns the root.
func collectParallel(t *testing.T, dir string, ign *amberignore.Matcher, ic chunkers.ItemChunker, byteOpts *chunkers.ByteOpts, xattrInlineMax, jobs int) (key.Key, map[key.Key][]byte) {
	t.Helper()
	objs := map[key.Key][]byte{}
	var root key.Key
	for o, err := range ingestObjects(dir, ign, ic, byteOpts, xattrInlineMax, jobs, nil, &root) {
		if err != nil {
			t.Fatalf("parallel build: %v", err)
		}
		objs[o.Key] = append([]byte(nil), o.Bytes...)
	}
	return root, objs
}

func assertSameObjects(t *testing.T, want, got map[key.Key][]byte) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("object count: want %d, got %d", len(want), len(got))
	}
	for k, wb := range want {
		gb, ok := got[k]
		if !ok {
			t.Errorf("missing object %s", k)
			continue
		}
		if !bytes.Equal(wb, gb) {
			t.Errorf("object %s bytes differ", k)
		}
	}
}

func TestIngestObjects_ParityWithSequential(t *testing.T) {
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

	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, nil, ic, nil, 256)
	parRoot, parObjs := collectParallel(t, dir, nil, ic, nil, 256, 4)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	assertSameObjects(t, seqObjs, parObjs)
}

func TestIngestObjects_ParallelParityDeepTree(t *testing.T) {
	dir := t.TempDir()
	writeDeepTree(t, dir)
	ic := chunkers.NewItemChunker(7)
	seqRoot, seqObjs := collectSequential(t, dir, nil, ic, nil, 256)
	jobs := max(runtime.NumCPU(), 4)
	parRoot, parObjs := collectParallel(t, dir, nil, ic, nil, 256, jobs)
	if seqRoot != parRoot {
		t.Fatalf("parallel root %s != sequential root %s", parRoot, seqRoot)
	}
	if len(seqObjs) < 50 {
		t.Fatalf("deep tree produced only %d objects; expected a large fan-out", len(seqObjs))
	}
	assertSameObjects(t, seqObjs, parObjs)
}

// packKeys decodes a pack file and returns the set of object keys it contains.
func packKeys(t *testing.T, path string) map[key.Key]bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	keys := map[key.Key]bool{}
	for o, err := range amberpack.NewReader(f).All() {
		if err != nil {
			t.Fatalf("reading pack: %v", err)
		}
		keys[o.Key] = true
	}
	return keys
}

func TestRunIngest_OutputFileContainsRoot(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "tree.amberpack")

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, src}); err != nil {
		t.Fatal(err)
	}
	rootHex := strings.TrimSpace(buf.String())
	raw, err := hex.DecodeString(rootHex)
	if err != nil {
		t.Fatalf("root not hex: %v", err)
	}
	root, err := key.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !packKeys(t, out)[root] {
		t.Errorf("pack file does not contain the printed root %s", root)
	}
}

func TestRunIngest_DeterministicAcrossJobs(t *testing.T) {
	src := t.TempDir()
	writeDeepTree(t, src)
	roots := make([]string, 0, 2)
	for _, j := range []string{"1", "8"} {
		var buf bytes.Buffer
		app := newApp()
		app.Writer = &buf
		out := filepath.Join(t.TempDir(), "p.amberpack")
		args := []string{"amber-store", "ingest", "--no-progress", "--jobs", j, "-o", out, src}
		if err := app.Run(args); err != nil {
			t.Fatalf("--jobs %s: %v", j, err)
		}
		roots = append(roots, strings.TrimSpace(buf.String()))
	}
	if roots[0] != roots[1] {
		t.Fatalf("root differs across --jobs: %q vs %q", roots[0], roots[1])
	}
}

func TestRunIngest_RejectsNonDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := newApp()
	out := filepath.Join(t.TempDir(), "p.amberpack")
	if err := app.Run([]string{"amber-store", "ingest", "--no-progress", "-o", out, f}); err == nil {
		t.Errorf("expected error ingesting a non-directory")
	}
}

// TestRunIngest_ServerErrorNotMaskedByClosedPipe reproduces a daemon rejecting
// an upload mid-stream: the transport closes the request-body pipe, the
// still-running build hits io.ErrClosedPipe, and the CLI must surface the
// server's error — not the secondary closed-pipe artifact.
func TestRunIngest_ServerErrorNotMaskedByClosedPipe(t *testing.T) {
	configureTestUser(t, "pipeuser")
	sockDir, err := os.MkdirTemp("", "amber-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	// Reject immediately without reading the body, like a verify failure would.
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "object verification failed", http.StatusUnprocessableEntity)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	// A tree large enough that the build is still streaming when the 422 lands.
	src := t.TempDir()
	writeDeepTree(t, src)

	app := newApp()
	err = app.Run([]string{"amber-store", "ingest", "--no-progress", "--socket", sock, "deep/tree", src})
	if err == nil {
		t.Fatal("expected ingest to fail")
	}
	if !strings.Contains(err.Error(), "object verification failed") {
		t.Errorf("error %q does not contain the server's message", err)
	}
	if strings.Contains(err.Error(), "closed pipe") {
		t.Errorf("error %q surfaces the closed-pipe artifact instead of the cause", err)
	}
}

func TestIngestObjects_ReportsProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil { // 5
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("bravo!"), 0o644); err != nil { // 6
		t.Fatal(err)
	}
	p := NewProgress(2, 11)
	var root key.Key
	for _, err := range ingestObjects(dir, nil, chunkers.NewItemChunker(7), nil, 256, 2, p, &root) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := p.bytesDone.Load(); got != 11 {
		t.Errorf("bytesDone = %d, want 11", got)
	}
	if got := p.filesDone.Load(); got != 2 {
		t.Errorf("filesDone = %d, want 2", got)
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
