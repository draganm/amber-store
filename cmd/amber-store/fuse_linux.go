//go:build linux

package main

import (
	"container/list"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/socketpath"
	"github.com/draganm/amber-store/key"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

type fuseConfig struct {
	socket      string
	allowOther  bool
	maxFileSize int64
	cacheBytes  int64
	debug       bool
}

// fuseCommand mounts a tag as a read-only FUSE filesystem. It exists only on
// Linux (see fuse_other.go for the stub that keeps the command off other
// platforms): the implementation reconstructs each file into an anonymous,
// RAM-backed memfd at open time and hands the kernel its descriptor as a FUSE
// passthrough backing file, so reads and mmaps bypass the daemon entirely.
func fuseCommand() *cli.Command {
	cfg := &fuseConfig{}
	return &cli.Command{
		Name:      "fuse",
		Usage:     "mount KEY[/PATH] or ref:NAME[@PATH] as a read-only FUSE filesystem (Linux only); files are reconstructed into RAM at open and served through kernel passthrough",
		ArgsUsage: "(KEY[/PATH] | ref:NAME[@PATH]) MOUNTPOINT",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.BoolFlag{
				Name:        "allow-other",
				Usage:       "let other users access the mount (needs user_allow_other in /etc/fuse.conf)",
				Destination: &cfg.allowOther,
			},
			&cli.Int64Flag{
				Name:        "max-file-size",
				Usage:       "refuse to open regular files larger than this many bytes (0 = unlimited); guards RAM, since each open file is materialized in memory",
				Destination: &cfg.maxFileSize,
			},
			&cli.Int64Flag{
				Name:        "cache-bytes",
				Usage:       "keep up to this many bytes of closed files materialized in RAM for faster re-open (0 = free each file on its last close)",
				Destination: &cfg.cacheBytes,
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "log FUSE protocol traffic to stderr",
				Destination: &cfg.debug,
			},
		},
		Action: func(c *cli.Context) error { return runFuse(c, cfg) },
	}
}

func runFuse(c *cli.Context, cfg *fuseConfig) error {
	if c.NArg() != 2 {
		return fmt.Errorf("fuse requires exactly two arguments SPEC and MOUNTPOINT, got %d", c.NArg())
	}
	spec := c.Args().Get(0)
	mountpoint := c.Args().Get(1)
	if err := checkDir(mountpoint); err != nil {
		return err
	}

	cl := client.New(socketpath.Resolve(cfg.socket))
	rootKey, subpath, err := resolveSpec(c.Context, cl, spec)
	if err != nil {
		return err
	}
	rootKey, err = resolveDirKey(c.Context, cl, rootKey, subpath)
	if err != nil {
		return err
	}
	if t := rootKey.Type(); t != key.DirLeaf && t != key.DirNode {
		return fmt.Errorf("%s is not a directory; only directories can be mounted", spec)
	}

	server, fsys, err := mountAmber(cl, rootKey, mountpoint, cfg)
	if err != nil {
		return err
	}

	// Passthrough needs both a capable kernel and CAP_SYS_ADMIN to register the
	// backing descriptors (the kernel's fuse_backing_open enforces this). When it
	// is unavailable the mount still works: reads are served from the same
	// in-memory copy through the FUSE daemon, just with one extra userspace hop.
	switch {
	case server.KernelSettings().Flags64()&fuse.CAP_PASSTHROUGH == 0:
		fmt.Fprintln(c.App.ErrWriter, "amber-store fuse: kernel does not support FUSE passthrough; reads will be served from RAM through the FUSE daemon")
	case !hasCapSysAdmin():
		fmt.Fprintln(c.App.ErrWriter, "amber-store fuse: FUSE passthrough needs CAP_SYS_ADMIN (run as root or grant the capability); without it, reads are served from RAM through the FUSE daemon")
	}

	// Unmount on Ctrl-C / SIGTERM. fusermount returns EBUSY when anything is
	// still using the mount (a shell cd'd into it, an editor, a file indexer —
	// common for a git repo under $HOME), so we must surface that and keep a way
	// out: a second signal retries and, if it still won't unmount, force-detaches
	// and exits. A buffered channel (not signal.NotifyContext) is used precisely
	// so repeated Ctrl-C is not swallowed while the first unmount is failing.
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		<-sigs
		fmt.Fprintf(c.App.ErrWriter, "\namber-store fuse: unmounting %s\n", mountpoint)
		if err := server.Unmount(); err == nil {
			return // server.Wait() returns and runFuse exits cleanly
		} else {
			fmt.Fprintf(c.App.ErrWriter,
				"amber-store fuse: unmount failed: %v\n"+
					"  %s is still in use (a shell cd'd into it, an open file, or a file indexer).\n"+
					"  Free it and press Ctrl-C to retry; if it still won't unmount it will be force-detached and the process will exit.\n",
				err, mountpoint)
		}
		for {
			<-sigs
			if err := server.Unmount(); err == nil {
				return
			}
			if err := lazyUnmount(mountpoint); err != nil {
				fmt.Fprintf(c.App.ErrWriter, "amber-store fuse: force-detach failed: %v; %s may stay mounted (run: fusermount -u %s)\n", err, mountpoint, mountpoint)
			} else {
				fmt.Fprintf(c.App.ErrWriter, "amber-store fuse: force-detached %s\n", mountpoint)
			}
			os.Exit(1)
		}
	}()

	fmt.Fprintf(c.App.Writer, "mounted %s at %s (Ctrl-C to unmount)\n", spec, mountpoint)
	server.Wait()
	fsys.cache.closeAll()
	return nil
}

// lazyUnmount detaches a busy FUSE mount so the process can exit even when a
// normal unmount fails with EBUSY. fusermount's lazy (-z) unmount needs no
// privilege; the kernel tears the FUSE connection down once the last reference
// to the detached mount goes away.
func lazyUnmount(mountpoint string) error {
	for _, bin := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(bin); err == nil {
			return exec.Command(p, "-u", "-z", mountpoint).Run()
		}
	}
	// No fusermount on PATH; fall back to the syscall (usually needs privilege).
	return unix.Unmount(mountpoint, unix.MNT_DETACH)
}

// mountAmber builds the filesystem rooted at rootKey (which must be a directory
// object) and mounts it read-only at mountpoint. It returns the running server
// and the shared filesystem state; the caller drives server.Wait()/Unmount()
// and fsys.cache.closeAll() on teardown.
func mountAmber(cl *client.Client, rootKey key.Key, mountpoint string, cfg *fuseConfig) (*fuse.Server, *amberFS, error) {
	fsys := &amberFS{
		cl:          cl,
		cache:       newBackingCache(cl, cfg.cacheBytes),
		maxFileSize: cfg.maxFileSize,
		dirCache:    map[key.Key][]client.Entry{},
	}
	root := &amberNode{
		fsys: fsys,
		ent: client.Entry{
			Mode: unix.S_IFDIR | 0o755,
			UID:  uint64(os.Getuid()),
			GID:  uint64(os.Getgid()),
			Key:  rootKey.String(),
		},
	}

	opts := &fs.Options{}
	opts.MountOptions.FsName = "amber-store"
	opts.MountOptions.Name = "amber"
	opts.MountOptions.AllowOther = cfg.allowOther
	opts.MountOptions.Debug = cfg.debug
	// The tree is content-addressed and immutable, so the kernel can cache
	// attributes and directory entries indefinitely.
	const forever = 365 * 24 * time.Hour
	timeout := forever
	opts.EntryTimeout = &timeout
	opts.AttrTimeout = &timeout

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mounting at %s: %w", mountpoint, err)
	}
	return server, fsys, nil
}

// resolveDirKey walks subpath (slash-separated, possibly empty) from root,
// listing each directory by key, and returns the content key of the directory
// the path names. It is used once at mount time to turn a KEY/PATH or ref@PATH
// spec into the directory key exposed as the FUSE root.
func resolveDirKey(ctx context.Context, cl *client.Client, root key.Key, subpath string) (key.Key, error) {
	k := root
	for _, comp := range strings.Split(subpath, "/") {
		if comp == "" {
			continue
		}
		if t := k.Type(); t != key.DirLeaf && t != key.DirNode {
			return key.Key{}, fmt.Errorf("path component %q: parent is not a directory", comp)
		}
		entries, err := cl.Ls(ctx, k, "")
		if err != nil {
			return key.Key{}, err
		}
		next, found := key.Key{}, false
		for _, e := range entries {
			if e.Name == comp {
				ck, err := parseContentKey(e.Key)
				if err != nil {
					return key.Key{}, fmt.Errorf("path component %q: %w", comp, err)
				}
				next, found = ck, true
				break
			}
		}
		if !found {
			return key.Key{}, fmt.Errorf("path %q not found: no entry %q", subpath, comp)
		}
		k = next
	}
	return k, nil
}

// amberFS holds the state shared by every node of one mount: the daemon client,
// the materialized-file cache, and a memoized directory-listing cache (safe to
// keep forever because directory objects are immutable).
type amberFS struct {
	cl          *client.Client
	cache       *backingCache
	maxFileSize int64

	dirMu    sync.Mutex
	dirCache map[key.Key][]client.Entry

	// fallbackReads counts reads served by amberFile.Read — i.e. reads the
	// kernel did NOT satisfy via passthrough. Zero after a passthrough read.
	fallbackReads atomic.Int64
}

func (fsys *amberFS) entries(ctx context.Context, dirKey key.Key) ([]client.Entry, error) {
	fsys.dirMu.Lock()
	es, ok := fsys.dirCache[dirKey]
	fsys.dirMu.Unlock()
	if ok {
		return es, nil
	}
	es, err := fsys.cl.Ls(ctx, dirKey, "")
	if err != nil {
		return nil, err
	}
	fsys.dirMu.Lock()
	fsys.dirCache[dirKey] = es
	fsys.dirMu.Unlock()
	return es, nil
}

// amberNode is one filesystem object. ent carries the metadata the parent
// directory recorded for it (synthesized for the root); for directories and
// regular files ent.Key is the content key used to list children or fetch bytes.
type amberNode struct {
	fs.Inode
	fsys *amberFS
	ent  client.Entry
}

var (
	_ fs.NodeLookuper   = (*amberNode)(nil)
	_ fs.NodeReaddirer  = (*amberNode)(nil)
	_ fs.NodeGetattrer  = (*amberNode)(nil)
	_ fs.NodeOpener     = (*amberNode)(nil)
	_ fs.NodeReadlinker = (*amberNode)(nil)
)

func (n *amberNode) contentKey() (key.Key, error) {
	return parseContentKey(n.ent.Key)
}

func (n *amberNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dirKey, err := n.contentKey()
	if err != nil {
		return nil, syscall.EIO
	}
	entries, err := n.fsys.entries(ctx, dirKey)
	if err != nil {
		return nil, syscall.EIO
	}
	for i := range entries {
		e := entries[i]
		if e.Name != name {
			continue
		}
		fillAttr(&out.Attr, e)
		child := &amberNode{fsys: n.fsys, ent: e}
		stable := fs.StableAttr{Mode: uint32(e.Mode) & unix.S_IFMT}
		return n.NewInode(ctx, child, stable), 0
	}
	return nil, syscall.ENOENT
}

func (n *amberNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dirKey, err := n.contentKey()
	if err != nil {
		return nil, syscall.EIO
	}
	entries, err := n.fsys.entries(ctx, dirKey)
	if err != nil {
		return nil, syscall.EIO
	}
	ds := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		ds = append(ds, fuse.DirEntry{
			Name: e.Name,
			Mode: uint32(e.Mode) & unix.S_IFMT,
		})
	}
	return fs.NewListDirStream(ds), 0
}

func (n *amberNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fillAttr(&out.Attr, n.ent)
	return 0
}

func (n *amberNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(n.ent.LinkTarget), 0
}

func (n *amberNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// The mount is read-only and the materialized memfd is shared across every
	// inode with identical content, so a writable open (which passthrough would
	// honor) must be refused — otherwise a write would corrupt every file that
	// shares the backing.
	if flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) {
		return nil, 0, syscall.EROFS
	}
	ck, err := n.contentKey()
	if err != nil {
		return nil, 0, syscall.EIO
	}
	if n.fsys.maxFileSize > 0 && int64(ck.Length()) > n.fsys.maxFileSize {
		return nil, 0, syscall.EFBIG
	}
	b, err := n.fsys.cache.acquire(ctx, ck)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &amberFile{fsys: n.fsys, b: b}, 0, 0
}

// amberFile is the handle for one open regular file. Its descriptor is the
// shared memfd; PassthroughFd hands it to the kernel for zero-hop reads, and
// Read is the fallback the bridge uses when passthrough is unavailable.
type amberFile struct {
	fsys *amberFS
	b    *backing
}

var (
	_ fs.FilePassthroughFder = (*amberFile)(nil)
	_ fs.FileReader          = (*amberFile)(nil)
	_ fs.FileReleaser        = (*amberFile)(nil)
)

func (h *amberFile) PassthroughFd() (int, bool) {
	return int(h.b.f.Fd()), true
}

func (h *amberFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.fsys.fallbackReads.Add(1)
	return fuse.ReadResultFd(h.b.f.Fd(), off, len(dest)), 0
}

func (h *amberFile) Release(ctx context.Context) syscall.Errno {
	h.fsys.cache.release(h.b)
	return 0
}

// fillAttr fills a fuse.Attr from a directory entry's stored metadata.
func fillAttr(a *fuse.Attr, e client.Entry) {
	a.Mode = uint32(e.Mode)
	a.Owner.Uid = uint32(e.UID)
	a.Owner.Gid = uint32(e.GID)
	if e.MtimeNs > 0 {
		a.Mtime = uint64(e.MtimeNs / int64(time.Second))
		a.Mtimensec = uint32(e.MtimeNs % int64(time.Second))
		a.Atime, a.Atimensec = a.Mtime, a.Mtimensec
		a.Ctime, a.Ctimensec = a.Mtime, a.Mtimensec
	}
	switch uint32(e.Mode) & unix.S_IFMT {
	case unix.S_IFREG, unix.S_IFLNK:
		a.Size = e.Size
		a.Nlink = 1
	case unix.S_IFDIR:
		a.Nlink = 2
	default:
		a.Nlink = 1
	}
	if len(e.Rdev) == 2 {
		a.Rdev = uint32(unix.Mkdev(uint32(e.Rdev[0]), uint32(e.Rdev[1])))
	}
}

// hasCapSysAdmin reports whether the process holds CAP_SYS_ADMIN in its
// effective set, which the kernel requires to register FUSE passthrough backing
// descriptors. Used only to print an accurate startup hint.
func hasCapSysAdmin() bool {
	hdr := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var data [2]unix.CapUserData
	if err := unix.Capget(&hdr, &data[0]); err != nil {
		return false
	}
	return data[0].Effective&(1<<uint(unix.CAP_SYS_ADMIN)) != 0
}

func parseContentKey(s string) (key.Key, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid content key %q: %w", s, err)
	}
	return key.Parse(raw)
}

// backing is one materialized file: an anonymous memfd holding the whole
// reconstructed content, shared by content key across every inode that opens
// identical bytes. refs counts the open handles using it.
type backing struct {
	f    *os.File
	k    key.Key
	size int64
	refs int
	el   *list.Element // position in the idle LRU while refs == 0, else nil
}

// backingCache reconstructs file content into memfds on demand, dedupes by
// content key, and keeps recently-closed files materialized up to maxIdle bytes
// so re-opens (and identical content at other paths) avoid refetching.
type backingCache struct {
	cl      *client.Client
	maxIdle int64

	mu       sync.Mutex
	byKey    map[key.Key]*backing
	idle     *list.List // *backing with refs == 0, front = most recently released
	idleSize int64
}

func newBackingCache(cl *client.Client, maxIdle int64) *backingCache {
	return &backingCache{
		cl:      cl,
		maxIdle: maxIdle,
		byKey:   map[key.Key]*backing{},
		idle:    list.New(),
	}
}

func (c *backingCache) acquire(ctx context.Context, ck key.Key) (*backing, error) {
	c.mu.Lock()
	if b := c.byKey[ck]; b != nil {
		c.reuseLocked(b)
		c.mu.Unlock()
		return b, nil
	}
	c.mu.Unlock()

	// Reconstruct outside the lock so opens of different files don't serialize.
	body, err := c.cl.File(ctx, ck)
	if err != nil {
		return nil, err
	}
	f, err := newMemfd(ck.String(), int64(ck.Length()), body)
	body.Close()
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Another open of the same content may have raced us; if so, drop ours.
	if b := c.byKey[ck]; b != nil {
		f.Close()
		c.reuseLocked(b)
		return b, nil
	}
	b := &backing{f: f, k: ck, size: int64(ck.Length()), refs: 1}
	c.byKey[ck] = b
	return b, nil
}

// reuseLocked takes a reference to an existing backing, pulling it out of the
// idle LRU if it was there. Caller holds c.mu.
func (c *backingCache) reuseLocked(b *backing) {
	if b.refs == 0 && b.el != nil {
		c.idle.Remove(b.el)
		b.el = nil
		c.idleSize -= b.size
	}
	b.refs++
}

func (c *backingCache) release(b *backing) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b.refs--
	if b.refs > 0 {
		return
	}
	if c.maxIdle <= 0 || b.size > c.maxIdle {
		delete(c.byKey, b.k)
		b.f.Close()
		return
	}
	b.el = c.idle.PushFront(b)
	c.idleSize += b.size
	c.evictLocked()
}

func (c *backingCache) evictLocked() {
	for c.idleSize > c.maxIdle {
		el := c.idle.Back()
		if el == nil {
			return
		}
		b := el.Value.(*backing)
		c.idle.Remove(el)
		b.el = nil
		c.idleSize -= b.size
		delete(c.byKey, b.k)
		b.f.Close()
	}
}

func (c *backingCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, b := range c.byKey {
		b.f.Close()
		delete(c.byKey, k)
	}
	c.idle.Init()
	c.idleSize = 0
}
