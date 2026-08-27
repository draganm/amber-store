package nixcache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/narexport"
	"github.com/draganm/amber-store/nixcache"
	"github.com/klauspost/compress/zstd"
)

type recStore struct {
	mu   sync.Mutex
	data map[key.Key][]byte
	recs map[key.Key][]byte
}

func newRecStore() *recStore {
	return &recStore{data: map[key.Key][]byte{}, recs: map[key.Key][]byte{}}
}

func (m *recStore) emit(o fstree.Object) error {
	rec, err := amberpack.EncodeRecord(o.Key, o.Bytes)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[o.Key] = o.Bytes
	m.recs[o.Key] = rec
	return nil
}

func (m *recStore) Get(k key.Key) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[k]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

func (m *recStore) ViewRecordSpans(keys []key.Key, _ int, fn func([]byte) error) error {
	for _, k := range keys {
		if err := m.ViewRecord(k, fn); err != nil {
			return err
		}
	}
	return nil
}

func (m *recStore) ViewRecord(k key.Key, fn func([]byte) error) error {
	m.mu.Lock()
	rec, ok := m.recs[k]
	m.mu.Unlock()
	if !ok {
		return errors.New("not found")
	}
	return fn(rec)
}

// buildTree stores a one-file directory tree and returns its root key.
func buildTree(t *testing.T, st *recStore, content []byte) key.Key {
	t.Helper()
	ib := fstree.NewFileIndexBuilder(chunkers.NewItemChunker(0))
	obj, err := fstree.EncodeBlob(content)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.emit(obj); err != nil {
		t.Fatal(err)
	}
	if err := ib.AddChild(st.emit, obj.Key, nil); err != nil {
		t.Fatal(err)
	}
	ck, err := ib.Finish(st.emit)
	if err != nil {
		t.Fatal(err)
	}
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(0))
	err = db.AddEntry(st.emit, fstree.Entry{Name: []byte("f"), Mode: 0o100444, ContentKey: ck[:]})
	if err != nil {
		t.Fatal(err)
	}
	root, err := db.Finish(st.emit)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type fixture struct {
	st   *recStore
	srv  *nixcache.Server
	info nixcache.PathInfo
}

func newFixture(t *testing.T) *fixture {
	st := newRecStore()
	root := buildTree(t, st, []byte("hello nix"))
	pi := info(1)
	pi.RootKey = root
	idx, err := nixcache.Merge(key.Key{}, []nixcache.PathInfo{pi}, nil, st.Get, st.emit)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{
		st:   st,
		info: pi,
		srv:  &nixcache.Server{Store: st, Index: func() key.Key { return idx }},
	}
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func TestCacheInfo(t *testing.T) {
	f := newFixture(t)
	rr := get(t, f.srv, "/nix-cache-info")
	want := "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 10\n"
	if rr.Code != 200 || rr.Body.String() != want {
		t.Fatalf("%d %q", rr.Code, rr.Body.String())
	}
}

func TestNarinfoHit(t *testing.T) {
	f := newFixture(t)
	rr := get(t, f.srv, "/"+nixcache.HashPart(f.info.StorePath)+".narinfo")
	if rr.Code != 200 {
		t.Fatalf("status %d", rr.Code)
	}
	n, err := nixcache.ParseNarinfo(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if n.StorePath != f.info.StorePath || n.Compression != "zstd" {
		t.Fatalf("%+v", n)
	}
	if k, err := nixcache.NarURLKey(n.URL); err != nil || k != f.info.RootKey {
		t.Fatalf("URL %q: %v", n.URL, err)
	}
}

func TestNarinfoMissUncatalogued(t *testing.T) {
	f := newFixture(t)
	var fetched atomic.Int32
	f.srv.Catalog = func(string) bool { return false }
	f.srv.Fetch = func(context.Context, string) (nixcache.PathInfo, error) {
		fetched.Add(1)
		return nixcache.PathInfo{}, nil
	}
	if rr := get(t, f.srv, "/"+hashPart(99)+".narinfo"); rr.Code != 404 {
		t.Fatalf("status %d", rr.Code)
	}
	if fetched.Load() != 0 {
		t.Fatal("uncatalogued miss reached Fetch")
	}
}

func TestNarinfoMissCatalogued(t *testing.T) {
	f := newFixture(t)
	other := info(2)
	other.RootKey = f.info.RootKey
	var fetched atomic.Int32
	f.srv.Catalog = func(hp string) bool { return hp == nixcache.HashPart(other.StorePath) }
	f.srv.Fetch = func(_ context.Context, hp string) (nixcache.PathInfo, error) {
		fetched.Add(1)
		return other, nil
	}
	rr := get(t, f.srv, "/"+nixcache.HashPart(other.StorePath)+".narinfo")
	if rr.Code != 200 {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	if fetched.Load() != 1 {
		t.Fatalf("fetch count %d", fetched.Load())
	}
}

func TestNarinfoFetchError(t *testing.T) {
	f := newFixture(t)
	f.srv.Catalog = func(string) bool { return true }
	f.srv.Fetch = func(context.Context, string) (nixcache.PathInfo, error) {
		return nixcache.PathInfo{}, errors.New("origin down")
	}
	if rr := get(t, f.srv, "/"+hashPart(99)+".narinfo"); rr.Code != 502 {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestNarServing(t *testing.T) {
	f := newFixture(t)
	var want bytes.Buffer
	if err := narexport.Export(&want, f.info.RootKey, f.st.Get); err != nil {
		t.Fatal(err)
	}
	rr := get(t, f.srv, fmt.Sprintf("/nar/%x.nar.zst", f.info.RootKey[:]))
	if rr.Code != 200 {
		t.Fatalf("status %d", rr.Code)
	}
	dec, err := zstd.NewReader(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	got, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatal("served NAR differs from narexport")
	}
}

func TestNarMissing(t *testing.T) {
	f := newFixture(t)
	absent, err := key.New(key.DirLeaf, 42, []byte("absent"))
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("/nar/%x.nar.zst", absent[:])

	if rr := get(t, f.srv, url); rr.Code != 404 {
		t.Fatalf("no Ensure: status %d", rr.Code)
	}
	f.srv.Ensure = func(context.Context, key.Key) error { return errors.New("no peers") }
	if rr := get(t, f.srv, url); rr.Code != 502 {
		t.Fatalf("failing Ensure: status %d", rr.Code)
	}
}

func TestNarEnsureRepopulates(t *testing.T) {
	f := newFixture(t)
	side := newRecStore()
	root := buildTree(t, side, []byte("evicted tree"))
	f.srv.Ensure = func(_ context.Context, k key.Key) error {
		if k != root {
			return errors.New("wrong key")
		}
		side.mu.Lock()
		defer side.mu.Unlock()
		f.st.mu.Lock()
		defer f.st.mu.Unlock()
		for k, v := range side.data {
			f.st.data[k] = v
			f.st.recs[k] = side.recs[k]
		}
		return nil
	}
	rr := get(t, f.srv, fmt.Sprintf("/nar/%x.nar.zst", root[:]))
	if rr.Code != 200 {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestBadRequests(t *testing.T) {
	f := newFixture(t)
	for path, want := range map[string]int{
		"/":                     404,
		"/nope.narinfo":         404,
		"/nar/zz.nar.zst":       404,
		"/nar/" + hashPart(1):   404,
		"/../etc/passwd":        404,
		"/" + hashPart(1)[:31]:  404,
		"/" + hashPart(1) + "x": 404,
	} {
		if rr := get(t, f.srv, path); rr.Code != want {
			t.Fatalf("%s: status %d, want %d", path, rr.Code, want)
		}
	}
	rr := httptest.NewRecorder()
	f.srv.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/nix-cache-info", nil))
	if rr.Code != 405 {
		t.Fatalf("POST: status %d", rr.Code)
	}
}

func TestFetchSingleflight(t *testing.T) {
	f := newFixture(t)
	other := info(3)
	other.RootKey = f.info.RootKey
	hp := nixcache.HashPart(other.StorePath)
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	f.srv.Catalog = func(string) bool { return true }
	f.srv.Fetch = func(context.Context, string) (nixcache.PathInfo, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return other, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rr := get(t, f.srv, "/"+hp+".narinfo"); rr.Code != 200 {
				t.Errorf("status %d", rr.Code)
			}
		}()
	}
	<-started
	time.Sleep(100 * time.Millisecond) // let the rest join the flight
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fetch called %d times", calls.Load())
	}
}

// A coalesced fetch must outlive the request that happened to start it.
func TestFetchSurvivesLeaderCancel(t *testing.T) {
	f := newFixture(t)
	gate := make(chan struct{})
	f.srv.Catalog = func(string) bool { return true }
	f.srv.Fetch = func(ctx context.Context, hp string) (nixcache.PathInfo, error) {
		<-gate
		return f.info, ctx.Err()
	}
	path := "/" + hashPart(2) + ".narinfo"

	ctx1, cancel1 := context.WithCancel(t.Context())
	rr1 := httptest.NewRecorder()
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		f.srv.ServeHTTP(rr1, httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx1))
	}()
	time.Sleep(20 * time.Millisecond)
	rr2 := httptest.NewRecorder()
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		f.srv.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, path, nil))
	}()
	time.Sleep(20 * time.Millisecond)
	cancel1()
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		close(gate)
		t.Fatal("cancelled request stayed blocked on the shared fetch")
	}
	close(gate)
	<-done2
	if rr2.Code != 200 {
		t.Fatalf("second client got %d after the first disconnected: %s", rr2.Code, rr2.Body)
	}
}

// A tree whose root survived GC but whose content did not must go
// through Ensure instead of committing a 200 and aborting mid-stream.
func TestNarEnsureRepairsPartialTree(t *testing.T) {
	f := newFixture(t)
	root := f.info.RootKey
	side := newRecStore()
	f.st.mu.Lock()
	for k := range f.st.data {
		if k != root {
			side.data[k], side.recs[k] = f.st.data[k], f.st.recs[k]
			delete(f.st.data, k)
			delete(f.st.recs, k)
		}
	}
	f.st.mu.Unlock()
	f.srv.Ensure = func(context.Context, key.Key) error {
		f.st.mu.Lock()
		defer f.st.mu.Unlock()
		maps.Copy(f.st.data, side.data)
		maps.Copy(f.st.recs, side.recs)
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nar route aborted the stream instead of ensuring: %v", r)
		}
	}()
	if rr := get(t, f.srv, fmt.Sprintf("/nar/%x.nar.zst", root[:])); rr.Code != 200 {
		t.Fatalf("status %d", rr.Code)
	}
}
