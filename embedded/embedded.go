// Package embedded lets a single process own an amber store directly — the
// packstore, refstore and remotes registry the daemon would otherwise manage —
// and sync with remote servers through the same signed protocol. It exists for
// long-running components (the JOBS engine and runners) that embed amber-store
// as a library instead of running a daemon sidecar; the on-disk layout matches
// the daemon's (<dir>/packstore, <dir>/refs, <dir>/remotes, <dir>/identity),
// so an embedded store remains inspectable with the CLI when the process is
// stopped. Stores are single-owner: never share dir between live processes.
package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/draganm/amber-store/gc"
	"github.com/draganm/amber-store/identity"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/remoteclient"
	"github.com/draganm/amber-store/remotes"
	"github.com/draganm/amber-store/remotesync"
	"golang.org/x/crypto/ssh"
)

// Config configures an embedded store.
type Config struct {
	// Signer is the remote transport identity; nil uses the store's own
	// auto-generated identity.
	Signer ssh.Signer
	// Grant, when non-nil, is consulted per remote request and attached as a
	// delegated capability grant — the runner-side credential. Swap in
	// refreshed grants by returning new bytes.
	Grant func() []byte
	// Sync selects fsync durability for object and ref writes.
	Sync bool
	// NoGC skips opening the collector; GC is nil and reference writes skip
	// the closure bookkeeping, exactly like a --gc=false daemon.
	NoGC bool
	// GC configures the collector (gc.Options zero value: 1 h grace, 0.5
	// garbage line, no background cycles). NoSync follows Sync.
	GC gc.Options
}

// Store is a process-owned amber store plus its remote-sync surface. The
// exported stores are for direct local use (reads, ingestion, FUSE);
// the methods cover the remote protocol.
type Store struct {
	Objects  *packstore.Store
	Refs     *refstore.Store
	Remotes  *remotes.Registry
	Identity ssh.Signer // the store's own identity
	// GC is the garbage collector over Objects and Refs (<dir>/closures);
	// nil with Config.NoGC. Run/Status/Why/Lease are used directly. Write
	// local references through PutRef/DeleteRef, not Refs.Put/Delete: a
	// record written behind the collector's back is invisible to its union
	// until the next Open, and a cycle in this process could reap objects
	// the bypassing reference just named.
	GC *gc.Collector

	signer ssh.Signer
	grant  func() []byte

	mu      sync.Mutex
	clients map[string]cachedClient
}

// cachedClient is a memoized remoteclient.Client keyed by the resolved
// remote's canonical name, plus the remote config it was built from — so a
// registry change (URL or pinned key) invalidates the cache entry.
type cachedClient struct {
	rem remotes.Remote
	rc  *remoteclient.Client
}

// Open opens (creating as needed) the embedded store at dir.
func Open(dir string, cfg Config) (*Store, error) {
	id, err := identity.LoadOrCreate(dir)
	if err != nil {
		return nil, err
	}
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(cfg.Sync))
	if err != nil {
		return nil, err
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), cfg.Sync)
	if err != nil {
		objects.Close()
		return nil, err
	}
	reg, err := remotes.Open(filepath.Join(dir, "remotes"))
	if err != nil {
		refs.Close()
		objects.Close()
		return nil, err
	}
	var coll *gc.Collector
	if !cfg.NoGC {
		opts := cfg.GC
		opts.NoSync = !cfg.Sync
		coll, err = gc.Open(filepath.Join(dir, "closures"), objects, refs, opts)
		if err != nil {
			refs.Close()
			objects.Close()
			return nil, err
		}
	}
	signer := cfg.Signer
	if signer == nil {
		signer = id
	}
	return &Store{
		Objects: objects, Refs: refs, Remotes: reg, Identity: id, GC: coll,
		signer: signer, grant: cfg.Grant,
		clients: map[string]cachedClient{},
	}, nil
}

// Close releases the underlying stores; the collector goes first, as it
// sits on top of them.
func (s *Store) Close() error {
	var gcErr error
	if s.GC != nil {
		gcErr = s.GC.Close()
	}
	return errors.Join(gcErr, s.Refs.Close(), s.Objects.Close())
}

// PutRef writes the reference record raw (canonical encoding; signatures
// are preserved verbatim) under its name, through the collector when it
// runs: the referenced content must be complete, and a missing object fails
// the write with an error naming it — re-store the objects and retry (the
// optimistic reference PUT).
func (s *Store) PutRef(raw []byte) error {
	rec, err := reference.Decode(raw)
	if err != nil {
		return err
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return err
	}
	if s.GC == nil {
		return s.Refs.Put(rec.Name, raw)
	}
	return s.GC.PutRef(rec.Name, root, raw)
}

// DeleteRef removes the local reference name, releasing its root when the
// collector runs. A missing name returns refstore.ErrNotFound.
func (s *Store) DeleteRef(name string) error {
	if s.GC == nil {
		return s.Refs.Delete(name)
	}
	return s.GC.DeleteRef(name)
}

// RemoteClient builds a signed client for the registered remote name (empty
// selects the sole remote), carrying the configured grant if any. Clients are
// cached one per remote (keyed by its canonical registry name) and rebuilt
// only when the registry entry (URL or pinned server key) changes underneath
// it — chatty callers don't churn a fresh http.Transport/connection pool on
// every call. The grant provider, if any, is still consulted per request by
// the cached client itself, so refreshing a grant never requires a rebuild.
func (s *Store) RemoteClient(name string) (*remoteclient.Client, error) {
	canonical, rem, err := s.Remotes.Resolve(name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cc, ok := s.clients[canonical]; ok && cc.rem.URL == rem.URL && bytes.Equal(cc.rem.ServerKey, rem.ServerKey) {
		return cc.rc, nil
	}
	var opts []remoteclient.Option
	if s.grant != nil {
		opts = append(opts, remoteclient.WithGrant(s.grant))
	}
	rc, err := remoteclient.New(rem.URL, s.signer, rem.ServerKey, opts...)
	if err != nil {
		return nil, err
	}
	s.clients[canonical] = cachedClient{rem: rem, rc: rc}
	return rc, nil
}

// Push uploads everything reachable from the local signed ref refName, then
// the ref itself — daemon push parity. The local record must be signed; the
// server re-verifies it and the no-dangling rule.
func (s *Store) Push(ctx context.Context, remote, refName string, opts remotesync.Opts) (remotesync.PushStats, error) {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return remotesync.PushStats{}, err
	}
	raw, err := s.Refs.Get(refName)
	if err != nil {
		return remotesync.PushStats{}, fmt.Errorf("reading local reference %q: %w", refName, err)
	}
	rec, err := reference.DecodeVerified(raw)
	if err != nil {
		return remotesync.PushStats{}, fmt.Errorf("local reference %q: %w", refName, err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return remotesync.PushStats{}, fmt.Errorf("local reference %q: %w", refName, err)
	}
	stats, err := remotesync.Push(ctx, s.Objects, rc, refName, root, opts)
	if err != nil {
		return stats, err
	}
	if err := rc.PutRef(ctx, refName, raw); err != nil {
		return stats, err
	}
	return stats, nil
}

// PushTree uploads the object closure under root without touching any ref —
// the runner-side half of a push whose ref another party publishes.
func (s *Store) PushTree(ctx context.Context, remote string, root key.Key, opts remotesync.Opts) (remotesync.PushStats, error) {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return remotesync.PushStats{}, err
	}
	return remotesync.Push(ctx, s.Objects, rc, "", root, opts)
}

// Pull resolves refName on the remote, verifies the record, completes the
// local store under its key, and writes the record locally — daemon pull
// parity. It returns the resolved root.
func (s *Store) Pull(ctx context.Context, remote, refName string, opts remotesync.Opts) (key.Key, remotesync.PullStats, error) {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return key.Key{}, remotesync.PullStats{}, err
	}
	raw, err := rc.GetRef(ctx, refName)
	if err != nil {
		return key.Key{}, remotesync.PullStats{}, err
	}
	rec, err := reference.DecodeVerified(raw)
	if err != nil {
		return key.Key{}, remotesync.PullStats{}, fmt.Errorf("remote reference %q: %w", refName, err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, remotesync.PullStats{}, fmt.Errorf("remote reference %q: %w", refName, err)
	}
	opts, release := s.leased(root, opts)
	defer release()
	stats, err := remotesync.Pull(ctx, s.Objects, rc, root, opts)
	if err != nil {
		return key.Key{}, stats, err
	}
	if err := s.PutRef(raw); err != nil {
		return key.Key{}, stats, fmt.Errorf("writing local reference %q: %w", refName, err)
	}
	return root, stats, nil
}

// leased covers a pull's root with an upload lease against the reaping
// horizon until release, refreshing it on progress so a long transfer
// never idles past the grace window. Without a collector it is a no-op.
func (s *Store) leased(root key.Key, opts remotesync.Opts) (remotesync.Opts, func()) {
	if s.GC == nil {
		return opts, func() {}
	}
	l := s.GC.Lease(root)
	progress := opts.Progress
	opts.Progress = func(done, total int) {
		l.Refresh()
		if progress != nil {
			progress(done, total)
		}
	}
	return opts, l.Release
}

// RemoteWipe factory-resets the named remote: every object and reference on
// the server is destroyed (its allowlist and identity survive). The store's
// transport key must carry the wipe capability on the server's allowlist.
func (s *Store) RemoteWipe(ctx context.Context, remote string) error {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return err
	}
	return rc.Wipe(ctx)
}

// PullTree completes the local store under a root the caller already knows,
// without touching refs. The lease covers the transfer; a reference naming
// root should follow within the collector's grace window, or its PUT may
// have to re-send objects (the optimistic PUT).
func (s *Store) PullTree(ctx context.Context, remote string, root key.Key, opts remotesync.Opts) (remotesync.PullStats, error) {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return remotesync.PullStats{}, err
	}
	opts, release := s.leased(root, opts)
	defer release()
	return remotesync.Pull(ctx, s.Objects, rc, root, opts)
}

// PublishRef uploads a pre-signed reference record to the remote without
// requiring any of its objects locally — the engine-side publish for content
// some other party pushed. The server's completeness gate still guarantees
// objects-before-ref.
func (s *Store) PublishRef(ctx context.Context, remote string, record []byte) error {
	rec, err := reference.DecodeVerified(record)
	if err != nil {
		return err
	}
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return err
	}
	return rc.PutRef(ctx, rec.Name, record)
}

// ListRemoteRefs lists every reference on the remote.
func (s *Store) ListRemoteRefs(ctx context.Context, remote string) ([]remoteclient.RefInfo, error) {
	rc, err := s.RemoteClient(remote)
	if err != nil {
		return nil, err
	}
	return rc.ListRefs(ctx)
}
