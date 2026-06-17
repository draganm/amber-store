//go:build linux

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func ingestObjs(t *testing.T, cl *client.Client, objs ...fstree.Object) {
	t.Helper()
	var buf bytes.Buffer
	w := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := w.Add(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Ingest(context.Background(), &buf); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

// TestFuseMount ingests a small tree, mounts it, and exercises the read,
// symlink, directory-listing, read-only and passthrough behaviors of the mount.
func TestFuseMount(t *testing.T) {
	sock := startDaemon(t)
	cl := client.New(sock)

	enc := func(o fstree.Object, err error) fstree.Object {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return o
	}

	const mtime = 1_600_000_000_000_000_000 // arbitrary fixed ns timestamp
	hello := enc(fstree.EncodeBlob([]byte("hello, amber\n")))

	// A multi-chunk file, to exercise FileNode reconstruction across blobs.
	c1 := enc(fstree.EncodeBlob([]byte("chunk-one-")))
	c2 := enc(fstree.EncodeBlob([]byte("chunk-two-")))
	c3 := enc(fstree.EncodeBlob([]byte("chunk-three")))
	big := enc(fstree.EncodeFileNode([]key.Key{c1.Key, c2.Key, c3.Key}))
	const bigWant = "chunk-one-chunk-two-chunk-three"

	nested := enc(fstree.EncodeBlob([]byte("nested body")))
	sub := enc(fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("nested.txt"), Mode: 0o100644, Mtime: mtime, ContentKey: nested.Key[:]},
	}))

	// Entries must be in name order (prolly-tree leaf invariant).
	root := enc(fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("big.bin"), Mode: 0o100644, Mtime: mtime, ContentKey: big.Key[:]},
		{Name: []byte("hello.txt"), Mode: 0o100644, Mtime: mtime, ContentKey: hello.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, Mtime: mtime, LinkTarget: []byte("hello.txt")},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: mtime, ContentKey: sub.Key[:]},
	}))

	ingestObjs(t, cl, hello, c1, c2, c3, big, nested, sub, root)

	mnt := t.TempDir()
	cfg := &fuseConfig{cacheBytes: 1 << 20} // enable idle reuse so re-opens hit the cache
	server, fsys, err := mountAmber(cl, root.Key, mnt, cfg)
	if err != nil {
		t.Skipf("cannot mount FUSE in this environment: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Unmount()
		fsys.cache.closeAll()
	})
	passthrough := server.KernelSettings().Flags64()&fuse.CAP_PASSTHROUGH != 0
	t.Logf("FUSE passthrough supported by kernel: %v", passthrough)

	// Regular file content.
	if got, err := os.ReadFile(filepath.Join(mnt, "hello.txt")); err != nil {
		t.Fatalf("read hello.txt: %v", err)
	} else if string(got) != "hello, amber\n" {
		t.Fatalf("hello.txt = %q, want %q", got, "hello, amber\n")
	}

	// Multi-chunk file reconstruction.
	if got, err := os.ReadFile(filepath.Join(mnt, "big.bin")); err != nil {
		t.Fatalf("read big.bin: %v", err)
	} else if string(got) != bigWant {
		t.Fatalf("big.bin = %q, want %q", got, bigWant)
	}

	// Subdirectory traversal.
	if got, err := os.ReadFile(filepath.Join(mnt, "sub", "nested.txt")); err != nil {
		t.Fatalf("read sub/nested.txt: %v", err)
	} else if string(got) != "nested body" {
		t.Fatalf("sub/nested.txt = %q, want %q", got, "nested body")
	}

	// Symlink: both the link target and following it.
	if tgt, err := os.Readlink(filepath.Join(mnt, "link")); err != nil {
		t.Fatalf("readlink link: %v", err)
	} else if tgt != "hello.txt" {
		t.Fatalf("link target = %q, want hello.txt", tgt)
	}
	if got, err := os.ReadFile(filepath.Join(mnt, "link")); err != nil {
		t.Fatalf("read through link: %v", err)
	} else if string(got) != "hello, amber\n" {
		t.Fatalf("link content = %q, want %q", got, "hello, amber\n")
	}

	// Directory listing.
	des, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	names := make([]string, len(des))
	for i, de := range des {
		names[i] = de.Name()
	}
	sort.Strings(names)
	want := []string{"big.bin", "hello.txt", "link", "sub"}
	if len(names) != len(want) {
		t.Fatalf("dir entries = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("dir entries = %v, want %v", names, want)
		}
	}

	// Re-open exercises the idle backing cache (acquire after release).
	if got, err := os.ReadFile(filepath.Join(mnt, "hello.txt")); err != nil || string(got) != "hello, amber\n" {
		t.Fatalf("re-read hello.txt = %q, %v", got, err)
	}

	// Read-only: writing must fail.
	if err := os.WriteFile(filepath.Join(mnt, "hello.txt"), []byte("nope"), 0o644); err == nil {
		t.Fatal("write to read-only mount succeeded, want error")
	}

	// Passthrough engages only with a capable kernel AND real CAP_SYS_ADMIN (the
	// kernel's fuse_backing_open checks the init namespace, so a userns fake-root
	// is not enough). It cannot be forced on in a typical test environment, so we
	// observe the outcome rather than require it: zero fallback reads means the
	// kernel served everything via passthrough; otherwise reads were served from
	// the same in-RAM copy through the fallback path — both must be correct, which
	// the content assertions above already checked.
	if n := fsys.fallbackReads.Load(); n == 0 {
		t.Logf("FUSE passthrough engaged: kernel served all reads directly")
	} else {
		t.Logf("FUSE passthrough did not engage (%d fallback read(s)); needs init-namespace CAP_SYS_ADMIN. Content served correctly from RAM.", n)
	}
}

// TestAmberFile_PassthroughFdAndFallbackRead verifies the open-file handle hands
// the kernel the memfd descriptor for passthrough and, when the kernel falls
// back, serves correct bytes from that same descriptor. This exercises the
// passthrough plumbing without needing the CAP_SYS_ADMIN the kernel demands to
// actually register a backing file.
func TestAmberFile_PassthroughFdAndFallbackRead(t *testing.T) {
	f, err := newMemfd("unit", 11, strings.NewReader("hello world"))
	if err != nil {
		t.Fatalf("newMemfd: %v", err)
	}
	defer f.Close()

	fsys := &amberFS{}
	h := &amberFile{fsys: fsys, b: &backing{f: f, size: 11}}

	fd, ok := h.PassthroughFd()
	if !ok {
		t.Fatal("PassthroughFd returned ok=false")
	}
	if fd != int(f.Fd()) {
		t.Fatalf("PassthroughFd = %d, want %d", fd, f.Fd())
	}

	dest := make([]byte, 5)
	res, errno := h.Read(context.Background(), dest, 6) // "world"
	if errno != 0 {
		t.Fatalf("Read errno = %v", errno)
	}
	got, st := res.Bytes(dest)
	if st != fuse.OK {
		t.Fatalf("ReadResult status = %v", st)
	}
	if string(got) != "world" {
		t.Fatalf("Read at offset 6 = %q, want %q", got, "world")
	}
	if n := fsys.fallbackReads.Load(); n != 1 {
		t.Fatalf("fallbackReads = %d, want 1", n)
	}
}

// TestBackingCache_DedupAndEviction checks that the cache dedups identical
// content by key, refcounts open handles, frees on last release when no idle
// budget is set, and retains then reuses an idle backing when a budget is set.
func TestBackingCache_DedupAndEviction(t *testing.T) {
	sock := startDaemon(t)
	cl := client.New(sock)
	enc := func(o fstree.Object, err error) fstree.Object {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	blob := enc(fstree.EncodeBlob([]byte("payload-bytes")))
	ingestObjs(t, cl, blob)
	ctx := context.Background()

	// maxIdle == 0: the backing is freed on its last release.
	c := newBackingCache(cl, 0)
	b1, err := c.acquire(ctx, blob.Key)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	b2, err := c.acquire(ctx, blob.Key)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if b1 != b2 {
		t.Fatal("acquire of identical content returned distinct backings; expected dedup")
	}
	if b1.refs != 2 {
		t.Fatalf("refs after two acquires = %d, want 2", b1.refs)
	}
	buf := make([]byte, b1.size)
	if _, err := b1.f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read memfd: %v", err)
	}
	if string(buf) != "payload-bytes" {
		t.Fatalf("materialized content = %q, want %q", buf, "payload-bytes")
	}
	c.release(b1)
	if b1.refs != 1 {
		t.Fatalf("refs after one release = %d, want 1", b1.refs)
	}
	c.release(b2)
	if len(c.byKey) != 0 {
		t.Fatalf("byKey holds %d entries after last release with maxIdle=0, want 0", len(c.byKey))
	}

	// maxIdle large: the backing is retained idle and reused on re-acquire.
	c2 := newBackingCache(cl, 1<<20)
	a1, err := c2.acquire(ctx, blob.Key)
	if err != nil {
		t.Fatal(err)
	}
	c2.release(a1)
	if len(c2.byKey) != 1 {
		t.Fatalf("idle cache dropped the backing; byKey = %d, want 1", len(c2.byKey))
	}
	a2, err := c2.acquire(ctx, blob.Key)
	if err != nil {
		t.Fatal(err)
	}
	if a2 != a1 {
		t.Fatal("re-acquire did not reuse the idle backing")
	}
	c2.release(a2)
	c2.closeAll()
	if len(c2.byKey) != 0 {
		t.Fatalf("closeAll left %d entries, want 0", len(c2.byKey))
	}
}
