# Admin Ref Browser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only ref browser in the admin SPA: list refs, browse directory trees (paginated for 100k+ entries), view/download files, download directories as `.tar`/`.tar.gz` — every endpoint behind the existing admin session auth.

**Architecture:** Two new fstree primitives (`LookupEntry`, `ListEntries`) exploit the prolly tree's `SepName` separators for O(log n) lookup and cursor pagination. The `admin` package gains four read-only GET endpoints (`refs`, `tree`, `raw`, `archive`) wired to the object/ref stores via small interfaces. The Solid.js SPA gains hash routing and two new pages.

**Tech Stack:** Go 1.26 (`net/http`, `archive/tar`, `compress/gzip`), existing packages `fstree`/`key`/`reference`/`refstore`/`tarexport`, Solid.js + Vite SPA embedded via `go:generate`/`go:embed`.

**Spec:** `docs/superpowers/specs/2026-06-11-admin-ref-browser-design.md`

**Conventions:** All commands run from the repo root. Module path is `github.com/draganm/amber-store`. Tests follow the existing styles in `fstree/collect_test.go` and `admin/admin_test.go`.

---

### Task 1: `fstree.LookupEntry` — O(log n) directory entry lookup

`DirNode` pairs store each child subtree's greatest entry name (`SepName`), so a lookup can binary-search down the tree instead of decoding whole directories.

**Files:**
- Create: `fstree/lookup.go`
- Create: `fstree/lookup_test.go`

- [ ] **Step 1: Write the failing test**

Create `fstree/lookup_test.go`:

```go
package fstree_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// memStore is an in-memory object store for builder-emitted objects.
type memStore map[key.Key][]byte

func (m memStore) get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("object %s not in store", k)
	}
	return b, nil
}

func (m memStore) emit(o fstree.Object) error {
	m[o.Key] = o.Bytes
	return nil
}

// bigDir builds a directory of n regular-file entries named e00000..e<n-1>
// into store, chunked into a multi-level prolly tree, and returns its root.
func bigDir(t *testing.T, store memStore, n int) key.Key {
	t.Helper()
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(3))
	for i := range n {
		e := fstree.Entry{
			Name:       fmt.Appendf(nil, "e%05d", i),
			Mode:       0o100644,
			ContentKey: blob.Key[:],
		}
		if err := db.AddEntry(store.emit, e); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(store.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirNode {
		t.Fatalf("fixture root = %v, want a DirNode (raise n?)", root.Type())
	}
	return root
}

func TestLookupEntry_BigDir(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)

	for _, name := range []string{"e00000", "e00001", "e00499", "e00998", "e00999"} {
		ent, err := fstree.LookupEntry(root, []byte(name), store.get)
		if err != nil {
			t.Fatalf("LookupEntry(%s): %v", name, err)
		}
		if string(ent.Name) != name {
			t.Fatalf("LookupEntry(%s).Name = %q", name, ent.Name)
		}
	}
	for _, name := range []string{"", "a", "e004995", "e00999x", "zzz"} {
		if _, err := fstree.LookupEntry(root, []byte(name), store.get); !errors.Is(err, fstree.ErrNotFound) {
			t.Fatalf("LookupEntry(%q) err = %v, want ErrNotFound", name, err)
		}
	}
}

func TestLookupEntry_TouchesFewObjects(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	reads := 0
	counting := func(k key.Key) ([]byte, error) {
		reads++
		return store.get(k)
	}
	if _, err := fstree.LookupEntry(root, []byte("e00500"), counting); err != nil {
		t.Fatal(err)
	}
	// tree depth for 1000 entries at average run 8 is ~4; anything near
	// O(n) (125+ leaves) means the descent is broken.
	if reads > 8 {
		t.Fatalf("lookup read %d objects, want a logarithmic descent", reads)
	}
}

func TestLookupEntry_SingleLeaf(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, ContentKey: blob.Key[:]},
		{Name: []byte("m"), Mode: 0o040755, ContentKey: leafSelfKey()},
		{Name: []byte("z"), Mode: 0o120777, LinkTarget: []byte("a")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(leaf); err != nil {
		t.Fatal(err)
	}

	ent, err := fstree.LookupEntry(leaf.Key, []byte("m"), store.get)
	if err != nil {
		t.Fatalf("LookupEntry(m): %v", err)
	}
	if string(ent.Name) != "m" {
		t.Fatalf("LookupEntry(m).Name = %q", ent.Name)
	}
	if _, err := fstree.LookupEntry(leaf.Key, []byte("b"), store.get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("LookupEntry(b) err = %v, want ErrNotFound", err)
	}
}

// leafSelfKey returns some canonical key bytes for fixture entries whose
// content is never fetched.
func leafSelfKey() []byte {
	o, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		panic(err)
	}
	return o.Key[:]
}

func TestLookupEntry_RejectsNonDir(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	if _, err := fstree.LookupEntry(blob.Key, []byte("a"), store.get); err == nil {
		t.Fatal("expected error for a non-directory key")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/ -run TestLookupEntry -v`
Expected: FAIL to compile with `undefined: fstree.LookupEntry`

- [ ] **Step 3: Write the implementation**

Create `fstree/lookup.go`:

```go
package fstree

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/draganm/amber-store/key"
)

// LookupEntry returns the entry called name in the directory object dir. It
// descends DirNode levels by binary search over each pair's SepName (the
// greatest entry name in that child's subtree), then scans the one DirLeaf
// that could hold the name — O(log n) objects for an n-entry directory. A
// missing name wraps ErrNotFound; get fetches the bytes stored under a key.
func LookupEntry(dir key.Key, name []byte, get func(key.Key) ([]byte, error)) (Entry, error) {
	k := dir
	for {
		data, err := get(k)
		if err != nil {
			return Entry{}, fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		switch k.Type() {
		case key.DirLeaf:
			entries, err := DecodeDirLeaf(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
			}
			i := sort.Search(len(entries), func(i int) bool {
				return bytes.Compare(entries[i].Name, name) >= 0
			})
			if i < len(entries) && bytes.Equal(entries[i].Name, name) {
				return entries[i], nil
			}
			return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
		case key.DirNode:
			pairs, err := DecodeDirNode(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
			}
			// The first pair whose SepName >= name roots the only subtree
			// that can contain name.
			i := sort.Search(len(pairs), func(i int) bool {
				return bytes.Compare(pairs[i].SepName, name) >= 0
			})
			if i == len(pairs) {
				return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
			}
			ck, err := key.Parse(pairs[i].ChildKey)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			k = ck
		default:
			return Entry{}, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fstree/ -run TestLookupEntry -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add fstree/lookup.go fstree/lookup_test.go
git commit -m "feat(fstree): O(log n) directory entry lookup"
```

---

### Task 2: `fstree.ListEntries` — cursor-paginated directory listing

**Files:**
- Create: `fstree/list.go`
- Create: `fstree/list_test.go`

- [ ] **Step 1: Write the failing test**

Create `fstree/list_test.go` (reuses `memStore` and `bigDir` from `fstree/lookup_test.go` — same package):

```go
package fstree_test

import (
	"bytes"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

func TestListEntries_PagesMatchCollect(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	want, err := fstree.CollectEntries(root, store.get)
	if err != nil {
		t.Fatal(err)
	}

	for _, limit := range []int{1, 7, 100, 999, 1000, 5000} {
		var got []fstree.Entry
		var after []byte
		for {
			page, more, err := fstree.ListEntries(root, after, limit, store.get)
			if err != nil {
				t.Fatalf("limit %d: %v", limit, err)
			}
			if len(page) > limit {
				t.Fatalf("limit %d: page has %d entries", limit, len(page))
			}
			got = append(got, page...)
			if !more {
				break
			}
			if len(page) == 0 {
				t.Fatalf("limit %d: more=true with an empty page", limit)
			}
			after = page[len(page)-1].Name
		}
		if len(got) != len(want) {
			t.Fatalf("limit %d: got %d entries, want %d", limit, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i].Name, want[i].Name) {
				t.Fatalf("limit %d: entry %d = %q, want %q", limit, i, got[i].Name, want[i].Name)
			}
		}
	}
}

func TestListEntries_MoreFlag(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)

	// limit == remaining entries: full page, nothing more.
	page, more, err := fstree.ListEntries(root, nil, 1000, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1000 || more {
		t.Fatalf("limit=1000: len=%d more=%v, want 1000 false", len(page), more)
	}

	// limit one short: more must be true.
	page, more, err = fstree.ListEntries(root, nil, 999, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 999 || !more {
		t.Fatalf("limit=999: len=%d more=%v, want 999 true", len(page), more)
	}

	// cursor in the middle of a leaf run starts strictly after it.
	page, _, err = fstree.ListEntries(root, []byte("e00007"), 3, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || string(page[0].Name) != "e00008" {
		t.Fatalf("after e00007: first = %q (len %d), want e00008", page[0].Name, len(page))
	}

	// cursor past the last entry: empty page, no more.
	page, more, err = fstree.ListEntries(root, []byte("e00999"), 10, store.get)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 || more {
		t.Fatalf("after last: len=%d more=%v, want 0 false", len(page), more)
	}
}

func TestListEntries_TouchesFewObjects(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	reads := 0
	counting := func(k key.Key) ([]byte, error) {
		reads++
		return store.get(k)
	}
	if _, _, err := fstree.ListEntries(root, []byte("e00500"), 10, counting); err != nil {
		t.Fatal(err)
	}
	// A 10-entry page from the middle needs the root path plus a few
	// leaves; reading dozens of objects means subtree skipping is broken.
	if reads > 12 {
		t.Fatalf("page read %d objects, want a bounded walk", reads)
	}
}

func TestListEntries_BadLimit(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	for _, limit := range []int{0, -5} {
		if _, _, err := fstree.ListEntries(root, nil, limit, store.get); err == nil {
			t.Fatalf("limit %d: expected an error", limit)
		}
	}
}

func TestListEntries_RejectsNonDir(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fstree.ListEntries(blob.Key, nil, 10, store.get); err == nil {
		t.Fatal("expected error for a non-directory key")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/ -run TestListEntries -v`
Expected: FAIL to compile with `undefined: fstree.ListEntries`

- [ ] **Step 3: Write the implementation**

Create `fstree/list.go`:

```go
package fstree

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/draganm/amber-store/key"
)

// ListEntries returns up to limit entries of the directory object dir whose
// names sort strictly after `after` (nil or empty lists from the start), in
// name order, and whether more such entries follow. It descends only the
// subtrees that can hold qualifying names — a DirNode pair is skipped when
// its SepName (the subtree's greatest name) is not after `after` — touching
// O(log n + limit) objects. limit must be positive.
func ListEntries(dir key.Key, after []byte, limit int, get func(key.Key) ([]byte, error)) ([]Entry, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("fstree: ListEntries limit must be positive, got %d", limit)
	}
	var out []Entry
	more, err := listInto(&out, dir, after, limit, get)
	if err != nil {
		return nil, false, err
	}
	return out, more, nil
}

// listInto appends qualifying entries of the subtree at k to out until out
// holds limit entries; it reports true the moment a qualifying entry beyond
// the limit exists.
func listInto(out *[]Entry, k key.Key, after []byte, limit int, get func(key.Key) ([]byte, error)) (bool, error) {
	data, err := get(k)
	if err != nil {
		return false, fmt.Errorf("fstree: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := DecodeDirLeaf(data)
		if err != nil {
			return false, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
		i := sort.Search(len(entries), func(i int) bool {
			return bytes.Compare(entries[i].Name, after) > 0
		})
		for ; i < len(entries); i++ {
			if len(*out) == limit {
				return true, nil
			}
			*out = append(*out, entries[i])
		}
		return false, nil
	case key.DirNode:
		pairs, err := DecodeDirNode(data)
		if err != nil {
			return false, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
		i := sort.Search(len(pairs), func(i int) bool {
			return bytes.Compare(pairs[i].SepName, after) > 0
		})
		for ; i < len(pairs); i++ {
			ck, err := key.Parse(pairs[i].ChildKey)
			if err != nil {
				return false, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			more, err := listInto(out, ck, after, limit, get)
			if err != nil {
				return false, err
			}
			if more {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fstree/ -run TestListEntries -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add fstree/list.go fstree/list_test.go
git commit -m "feat(fstree): paginated directory listing"
```

---

### Task 3: `ResolvePath` via `LookupEntry`; new `ResolveEntry`

`ResolvePath` currently decodes entire directories per component. Reimplement it on `LookupEntry` (same behavior), and add `ResolveEntry`, which resolves a path to its final entry of *any* kind.

**Files:**
- Modify: `fstree/collect.go:23-56` (replace `ResolvePath`, add `ResolveEntry`)
- Modify: `fstree/collect_test.go` (add `ResolveEntry` tests)

- [ ] **Step 1: Write the failing test**

Append to `fstree/collect_test.go`:

```go
func TestResolveEntry(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	inner := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	mid := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("inner"), Mode: 0o040755, ContentKey: inner.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, LinkTarget: []byte("inner")},
	})
	root := mustDirLeaf(t, []fstree.Entry{
		{Name: []byte("file"), Mode: 0o100644, ContentKey: blob.Key[:]},
		{Name: []byte("mid"), Mode: 0o040755, ContentKey: mid.Key[:]},
	})
	get := mapGetter(blob, inner, mid, root)

	// The empty path (and "." chains) is the root itself: no entry.
	for _, p := range []string{"", ".", "./"} {
		ent, err := fstree.ResolveEntry(root.Key, p, get)
		if err != nil || ent != nil {
			t.Fatalf("ResolveEntry(%q) = %v, %v, want nil, nil", p, ent, err)
		}
	}

	// A file at the end is returned with its metadata.
	ent, err := fstree.ResolveEntry(root.Key, "mid/inner/file", get)
	if err != nil {
		t.Fatalf("ResolveEntry(mid/inner/file): %v", err)
	}
	if string(ent.Name) != "file" || ent.Mode != 0o100644 {
		t.Fatalf("ResolveEntry(mid/inner/file) = %+v", ent)
	}

	// A directory at the end is returned as its entry.
	ent, err = fstree.ResolveEntry(root.Key, "mid/inner", get)
	if err != nil || string(ent.Name) != "inner" {
		t.Fatalf("ResolveEntry(mid/inner) = %+v, %v", ent, err)
	}

	// A symlink at the end is returned, not followed.
	ent, err = fstree.ResolveEntry(root.Key, "mid/link", get)
	if err != nil || string(ent.LinkTarget) != "inner" {
		t.Fatalf("ResolveEntry(mid/link) = %+v, %v", ent, err)
	}

	// Missing final component.
	if _, err := fstree.ResolveEntry(root.Key, "mid/nope", get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("missing component: err = %v, want ErrNotFound", err)
	}

	// Descending through a non-directory.
	if _, err := fstree.ResolveEntry(root.Key, "file/x", get); !errors.Is(err, fstree.ErrNotDir) {
		t.Fatalf("through a file: err = %v, want ErrNotDir", err)
	}

	// ".." is rejected.
	if _, err := fstree.ResolveEntry(root.Key, "mid/..", get); err == nil {
		t.Fatal("expected error for \"..\" component")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fstree/ -run TestResolveEntry -v`
Expected: FAIL to compile with `undefined: fstree.ResolveEntry`

- [ ] **Step 3: Write the implementation**

In `fstree/collect.go`, replace the whole `ResolvePath` function with:

```go
// ResolvePath descends from the directory object root along the slash-separated
// path and returns the key of the directory it names. Empty components and "."
// are ignored, so "", ".", and paths with leading/trailing slashes are
// accepted; ".." is rejected (a CAS tree has no parent links). A missing
// component wraps ErrNotFound, a non-directory component wraps ErrNotDir.
func ResolvePath(root key.Key, path string, get func(key.Key) ([]byte, error)) (key.Key, error) {
	k := root
	for comp := range strings.SplitSeq(path, "/") {
		if comp == "" || comp == "." {
			continue
		}
		if comp == ".." {
			return key.Key{}, fmt.Errorf("fstree: %q: \"..\" is not supported", path)
		}
		found, err := LookupEntry(k, []byte(comp), get)
		if err != nil {
			return key.Key{}, err
		}
		if found.Mode&unix.S_IFMT != unix.S_IFDIR {
			return key.Key{}, fmt.Errorf("fstree: %q: %w", comp, ErrNotDir)
		}
		ck, err := key.Parse(found.ContentKey)
		if err != nil {
			return key.Key{}, fmt.Errorf("fstree: %q: content key: %w", comp, err)
		}
		k = ck
	}
	return k, nil
}

// ResolveEntry descends from the directory object root along the
// slash-separated path and returns the entry the final component names — of
// any kind (file, directory, symlink, device, …), carrying its metadata. The
// empty path (or chains of "" and ".") returns nil: the root directory is not
// an entry and has no metadata of its own. Intermediate components must name
// directories (ErrNotDir otherwise); a missing component wraps ErrNotFound;
// ".." is rejected.
func ResolveEntry(root key.Key, path string, get func(key.Key) ([]byte, error)) (*Entry, error) {
	dir := root
	var cur *Entry // entry of dir; nil while dir is the root
	for comp := range strings.SplitSeq(path, "/") {
		if comp == "" || comp == "." {
			continue
		}
		if comp == ".." {
			return nil, fmt.Errorf("fstree: %q: \"..\" is not supported", path)
		}
		if cur != nil {
			if cur.Mode&unix.S_IFMT != unix.S_IFDIR {
				return nil, fmt.Errorf("fstree: %q: %w", cur.Name, ErrNotDir)
			}
			ck, err := key.Parse(cur.ContentKey)
			if err != nil {
				return nil, fmt.Errorf("fstree: %q: content key: %w", cur.Name, err)
			}
			dir = ck
		}
		ent, err := LookupEntry(dir, []byte(comp), get)
		if err != nil {
			return nil, err
		}
		cur = &ent
	}
	return cur, nil
}
```

- [ ] **Step 4: Run the package tests (old ResolvePath behavior must hold)**

Run: `go test ./fstree/ -v`
Expected: PASS — including the pre-existing `TestResolvePath`

- [ ] **Step 5: Commit**

```bash
git add fstree/collect.go fstree/collect_test.go
git commit -m "refactor(fstree): resolve paths via LookupEntry; add ResolveEntry"
```

---

### Task 4: Wire object and reference stores into the admin handler

**Files:**
- Create: `admin/browse.go` (interfaces only for now)
- Modify: `admin/admin.go` (package comment, `Config`, `handler`, `New` validation)
- Modify: `admin/admin_test.go` (`testServer` passes fakes; new `TestNewRequiresStores`)
- Create: `admin/browse_test.go` (fakes + fixtures used by Tasks 5–9)
- Modify: `cmd/amber-store/serve.go:186-192` (pass stores)

- [ ] **Step 1: Write the failing test**

Append to `admin/admin_test.go`:

```go
func TestNewRequiresStores(t *testing.T) {
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = keys.Close() })
	if _, err := admin.New(admin.Config{Password: password, Keys: keys, UI: testUI}); err == nil {
		t.Fatal("want error when Objects/Refs are missing")
	}
}
```

Create `admin/browse_test.go` with the fakes and fixtures (the seeded tree is the standard fixture for Tasks 5–9):

```go
package admin_test

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"

	"github.com/draganm/amber-store/admin"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/internal/allowstore"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// memObjects is an in-memory admin.ObjectGetter.
type memObjects map[key.Key][]byte

func (m memObjects) Get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("object %s: %w", k, diskstore.ErrNotFound)
	}
	return b, nil
}

func (m memObjects) put(o fstree.Object) { m[o.Key] = o.Bytes }

// memRefs is an in-memory admin.RefStore.
type memRefs map[string][]byte

func (m memRefs) Get(name string) ([]byte, error) {
	b, ok := m[name]
	if !ok {
		return nil, refstore.ErrNotFound
	}
	return b, nil
}

func (m memRefs) All() ([]refstore.Record, error) {
	recs := make([]refstore.Record, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		recs = append(recs, refstore.Record{Name: name, Data: m[name]})
	}
	return recs, nil
}

func mustObj(t *testing.T, o fstree.Object, err error) fstree.Object {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// mustRef encodes a reference record pointing name at k.
func mustRef(t *testing.T, name string, k key.Key, user string) []byte {
	t.Helper()
	b, err := reference.Reference{Name: name, Key: k[:], User: user, CreatedAt: 12345}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// seedTree stores this fixture tree in objs and returns its root directory
// key and the key of hello.txt's content:
//
//	hello.txt        "hello, amber"
//	sub/link         -> ../hello.txt
//	sub/nested.txt   "nested"
func seedTree(t *testing.T, objs memObjects) (root, hello key.Key) {
	t.Helper()
	helloBlob := mustObj(t, fstree.EncodeBlob([]byte("hello, amber")))
	objs.put(helloBlob)
	nested := mustObj(t, fstree.EncodeBlob([]byte("nested")))
	objs.put(nested)
	subLeaf := mustObj(t, fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("link"), Mode: 0o120777, Mtime: 3, LinkTarget: []byte("../hello.txt")},
		{Name: []byte("nested.txt"), Mode: 0o100644, Mtime: 2, ContentKey: nested.Key[:]},
	}))
	objs.put(subLeaf)
	rootLeaf := mustObj(t, fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("hello.txt"), Mode: 0o100644, Mtime: 1, ContentKey: helloBlob.Key[:]},
		{Name: []byte("sub"), Mode: 0o040755, Mtime: 4, ContentKey: subLeaf.Key[:]},
	}))
	objs.put(rootLeaf)
	return rootLeaf.Key, helloBlob.Key
}

// browseServer starts an admin server whose browse endpoints see objs and
// refs, returning it with a logged-in session cookie.
func browseServer(t *testing.T, objs memObjects, refs memRefs) (*httptest.Server, *http.Cookie) {
	t.Helper()
	keys, err := allowstore.Open(filepath.Join(t.TempDir(), "allowed-keys"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = keys.Close() })
	h, err := admin.New(admin.Config{
		Password: password, Keys: keys, Objects: objs, Refs: refs, UI: testUI,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, login(t, srv)
}
```

In `admin/admin_test.go`, update `testServer` so the existing tests satisfy the new required fields — the `admin.New` call becomes:

```go
	h, err := admin.New(admin.Config{
		Password: password, Keys: keys,
		Objects: memObjects{}, Refs: memRefs{},
		UI: testUI,
	})
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./admin/ -run 'TestNewRequiresStores' -v`
Expected: FAIL to compile — `unknown field Objects in struct literal`, `undefined: admin.ObjectGetter` etc.

- [ ] **Step 3: Write the implementation**

Create `admin/browse.go`:

```go
package admin

import (
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/refstore"
)

// ObjectGetter is the read-only object-store view the ref browser needs;
// *diskstore.Store implements it.
type ObjectGetter interface {
	Get(k key.Key) ([]byte, error)
}

// RefStore is the read-only reference view the ref browser needs;
// *refstore.Store implements it.
type RefStore interface {
	Get(name string) ([]byte, error)
	All() ([]refstore.Record, error)
}
```

In `admin/admin.go`:

1. Extend the package comment's first sentence so it stays truthful:

```go
// Package admin serves the server's admin SPA and its JSON API: a
// password-protected browser UI (under /admin/) where an operator
// inspects, adds and removes the SSH keys allowed to talk to the
// server, and browses the server's references and their file trees.
// The password comes from the environment; sessions live in
// memory, so a restart logs every admin out.
```

2. Add the fields to `Config` (after `Keys`):

```go
	Objects   ObjectGetter      // required; read-only object access for the ref browser
	Refs      RefStore          // required; read-only reference access for the ref browser
```

3. Add the fields to `handler` (after `keys`):

```go
	objects   ObjectGetter
	refs      RefStore
```

4. In `New`, after the password check, add:

```go
	if cfg.Objects == nil || cfg.Refs == nil {
		return nil, errors.New("admin UI requires the object and reference stores")
	}
```

and set `objects: cfg.Objects, refs: cfg.Refs,` in the `handler` literal.

In `cmd/amber-store/serve.go`, the `admin.New` call gains the two stores:

```go
		adminHandler, err := admin.New(admin.Config{
			Password: cfg.adminPassword,
			Keys:     keys,
			Objects:  store,
			Refs:     refs,
			UI:       ui,
			Log:      logger,
			Secure:   cfg.tlsCert != "",
		})
```

- [ ] **Step 4: Run the tests**

Run: `go test ./admin/ ./cmd/amber-store/ -count=1`
Expected: PASS (all existing admin and serve tests, plus `TestNewRequiresStores`)

- [ ] **Step 5: Commit**

```bash
git add admin/browse.go admin/admin.go admin/admin_test.go admin/browse_test.go cmd/amber-store/serve.go
git commit -m "feat(admin): wire object and reference stores into the admin handler"
```

---

### Task 5: `GET /admin/api/refs`

**Files:**
- Modify: `admin/browse.go` (handler + JSON shape)
- Modify: `admin/admin.go` (route registration)
- Modify: `admin/browse_test.go`

- [ ] **Step 1: Write the failing test**

Append to `admin/browse_test.go` (add `"encoding/json"` to its imports):

```go
type refInfoJSON struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Kind      string `json:"kind"`
}

func TestListRefs(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
		"zzz-bad":      []byte{0x42}, // undecodable record
	}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", srv.URL+"/admin/api/refs", cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refs = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Refs []refInfoJSON `json:"refs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Refs) != 3 {
		t.Fatalf("got %d refs, want 3: %+v", len(out.Refs), out.Refs)
	}
	if r := out.Refs[0]; r.Name != "backup/daily" || r.Kind != "dir" ||
		r.User != "alice@example.com" || r.Key == "" ||
		r.CreatedAt != "1970-01-01T00:00:00.000012345Z" {
		t.Fatalf("refs[0] = %+v", r)
	}
	if r := out.Refs[1]; r.Name != "motd" || r.Kind != "file" || r.User != "bob" {
		t.Fatalf("refs[1] = %+v", r)
	}
	if r := out.Refs[2]; r.Name != "zzz-bad" || r.Kind != "invalid" || r.Key != "" {
		t.Fatalf("refs[2] = %+v", r)
	}
}

func TestListRefsEmpty(t *testing.T) {
	srv, cookie := browseServer(t, memObjects{}, memRefs{})
	resp := do(t, "GET", srv.URL+"/admin/api/refs", cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refs = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Refs []refInfoJSON `json:"refs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Refs == nil || len(out.Refs) != 0 {
		t.Fatalf("refs = %#v, want an empty (non-null) array", out.Refs)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./admin/ -run TestListRefs -v`
Expected: FAIL — `refs = 404, want 200` (route does not exist yet)

- [ ] **Step 3: Write the implementation**

Append to `admin/browse.go` (extend its imports with `"encoding/json"`, `"net/http"`, `"time"`, and `"github.com/draganm/amber-store/reference"`):

```go
// refKind classifies a parsed store key for the refs listing.
func refKind(k key.Key) string {
	switch k.Type() {
	case key.DirLeaf, key.DirNode:
		return "dir"
	case key.Blob, key.FileNode:
		return "file"
	default:
		return "invalid"
	}
}

type refInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
	User      string `json:"user,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	Kind      string `json:"kind"`
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// listRefs serves every reference with its target kind. Records that fail
// to decode are listed as kind "invalid" rather than hidden — an operator
// tool must show what exists.
func (h *handler) listRefs(w http.ResponseWriter, r *http.Request) {
	recs, err := h.refs.All()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]refInfo, 0, len(recs))
	for _, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			out = append(out, refInfo{Name: rec.Name, Kind: "invalid"})
			continue
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			out = append(out, refInfo{Name: rec.Name, Kind: "invalid"})
			continue
		}
		out = append(out, refInfo{
			Name:      ref.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Kind:      refKind(k),
		})
	}
	writeJSON(w, map[string]any{"refs": out})
}
```

In `admin/admin.go` `New`, register the route next to the keys routes:

```go
	mux.HandleFunc("GET /admin/api/refs", h.authed(h.listRefs))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./admin/ -run TestListRefs -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add admin/browse.go admin/admin.go admin/browse_test.go
git commit -m "feat(admin): refs listing endpoint"
```

---

### Task 6: `GET /admin/api/tree` — listing, pagination, file stat

**Files:**
- Modify: `admin/browse.go` (target resolution shared by Tasks 6–8, tree handler)
- Modify: `admin/admin.go` (route registration)
- Modify: `admin/browse_test.go`

- [ ] **Step 1: Write the failing test**

Append to `admin/browse_test.go` (add `"net/url"` and `"github.com/draganm/amber-store/chunkers"` to its imports):

```go
type treeEntryJSON struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Size        uint64 `json:"size"`
	Mode        uint64 `json:"mode"`
	Mtime       int64  `json:"mtime"`
	Target      string `json:"target"`
	NameInvalid bool   `json:"raw_name_invalid"`
}

type treeJSON struct {
	Kind    string          `json:"kind"`
	Entries []treeEntryJSON `json:"entries"`
	More    bool            `json:"more"`
	Stat    *treeEntryJSON  `json:"stat"`
}

func treeURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/tree?" + q.Encode()
}

func getTree(t *testing.T, srv *httptest.Server, cookie *http.Cookie, params map[string]string, wantStatus int) treeJSON {
	t.Helper()
	resp := do(t, "GET", treeURL(srv, params), cookie, "")
	if resp.StatusCode != wantStatus {
		t.Fatalf("tree %v = %d, want %d", params, resp.StatusCode, wantStatus)
	}
	var out treeJSON
	if wantStatus == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestTreeListing(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	// Root listing: full metadata, name order.
	got := getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": ""}, http.StatusOK)
	if got.Kind != "dir" || got.More || len(got.Entries) != 2 {
		t.Fatalf("root tree = %+v", got)
	}
	if e := got.Entries[0]; e.Name != "hello.txt" || e.Kind != "file" ||
		e.Size != 12 || e.Mode != 0o100644 || e.Mtime != 1 {
		t.Fatalf("entries[0] = %+v", e)
	}
	if e := got.Entries[1]; e.Name != "sub" || e.Kind != "dir" || e.Mode != 0o040755 {
		t.Fatalf("entries[1] = %+v", e)
	}

	// Subdirectory: symlink carries its target.
	got = getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "sub"}, http.StatusOK)
	if len(got.Entries) != 2 || got.Entries[0].Kind != "symlink" || got.Entries[0].Target != "../hello.txt" {
		t.Fatalf("sub tree = %+v", got)
	}

	// A file path returns its stat.
	got = getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "sub/nested.txt"}, http.StatusOK)
	if got.Kind != "file" || got.Stat == nil || got.Stat.Size != 6 || got.Stat.Name != "nested.txt" {
		t.Fatalf("file tree = %+v", got)
	}

	// A ref that points straight at a file: stat from the key alone.
	got = getTree(t, srv, cookie, map[string]string{"ref": "motd", "path": ""}, http.StatusOK)
	if got.Kind != "file" || got.Stat == nil || got.Stat.Size != 12 {
		t.Fatalf("file-ref tree = %+v", got)
	}

	// Errors.
	getTree(t, srv, cookie, map[string]string{"ref": "nope", "path": ""}, http.StatusNotFound)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "ghost"}, http.StatusNotFound)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "path": "hello.txt/x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "motd", "path": "x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"path": "x"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "limit": "0"}, http.StatusBadRequest)
	getTree(t, srv, cookie, map[string]string{"ref": "backup/daily", "limit": "junk"}, http.StatusBadRequest)
}

// bigDirRef seeds a 1200-entry directory (multi-level prolly tree) under a
// "big" ref and returns the server.
func bigDirRef(t *testing.T) (*httptest.Server, *http.Cookie) {
	t.Helper()
	objs := memObjects{}
	blob := mustObj(t, fstree.EncodeBlob([]byte("x")))
	objs.put(blob)
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(3))
	emit := func(o fstree.Object) error { objs.put(o); return nil }
	for i := range 1200 {
		e := fstree.Entry{Name: fmt.Appendf(nil, "e%05d", i), Mode: 0o100644, ContentKey: blob.Key[:]}
		if err := db.AddEntry(emit, e); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(emit)
	if err != nil {
		t.Fatal(err)
	}
	refs := memRefs{"big": mustRef(t, "big", root, "alice")}
	return browseServer(t, objs, refs)
}

func TestTreePagination(t *testing.T) {
	srv, cookie := bigDirRef(t)

	// Page through with limit=500 (the default): 500 + 500 + 200.
	var names []string
	after := ""
	for page := 0; ; page++ {
		params := map[string]string{"ref": "big", "path": "", "after": after}
		got := getTree(t, srv, cookie, params, http.StatusOK)
		for _, e := range got.Entries {
			names = append(names, e.Name)
		}
		if !got.More {
			break
		}
		if page > 3 {
			t.Fatal("more=true never cleared")
		}
		after = got.Entries[len(got.Entries)-1].Name
	}
	if len(names) != 1200 || names[0] != "e00000" || names[1199] != "e01199" {
		t.Fatalf("paged %d names, first %q last %q", len(names), names[0], names[len(names)-1])
	}

	// Explicit small limit.
	got := getTree(t, srv, cookie, map[string]string{"ref": "big", "after": "e00009", "limit": "5"}, http.StatusOK)
	if len(got.Entries) != 5 || got.Entries[0].Name != "e00010" || !got.More {
		t.Fatalf("limited page = %+v", got)
	}

	// Limits above the cap are clamped to 1000, not rejected.
	got = getTree(t, srv, cookie, map[string]string{"ref": "big", "limit": "5000"}, http.StatusOK)
	if len(got.Entries) != 1000 || !got.More {
		t.Fatalf("clamped page: len=%d more=%v, want 1000 true", len(got.Entries), got.More)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./admin/ -run 'TestTree' -v`
Expected: FAIL — `tree ... = 404, want 200` (route does not exist yet)

- [ ] **Step 3: Write the implementation**

Append to `admin/browse.go` (extend its imports — the full import block of the file is now):

```go
import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
	"golang.org/x/sys/unix"
)
```

```go
const (
	defaultTreeLimit = 500
	maxTreeLimit     = 1000
)

// browseTarget is a resolved ref+path: the addressed object key (zero when
// the entry carries no content key, e.g. a symlink) and, for a non-empty
// path, the directory entry holding its metadata (nil at a ref root).
type browseTarget struct {
	key   key.Key
	entry *fstree.Entry
}

// hasComponents reports whether path names anything beyond the root.
func hasComponents(path string) bool {
	for comp := range strings.SplitSeq(path, "/") {
		if comp != "" && comp != "." {
			return true
		}
	}
	return false
}

// resolveTarget resolves the request's ref and path query parameters to a
// browse target. On failure it returns the HTTP status to respond with:
// 404 for unknown refs, unknown paths and objects missing from the store,
// 400 for bad parameters and descending through non-directories.
func (h *handler) resolveTarget(r *http.Request) (browseTarget, int, error) {
	name := r.URL.Query().Get("ref")
	if name == "" {
		return browseTarget{}, http.StatusBadRequest, errors.New("missing ref parameter")
	}
	rec, err := h.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		return browseTarget{}, http.StatusNotFound, fmt.Errorf("reference %q not found", name)
	}
	if err != nil {
		return browseTarget{}, http.StatusInternalServerError, err
	}
	ref, err := reference.Decode(rec)
	if err != nil {
		return browseTarget{}, http.StatusInternalServerError, fmt.Errorf("stored reference is malformed: %w", err)
	}
	root, err := key.Parse(ref.Key)
	if err != nil {
		return browseTarget{}, http.StatusInternalServerError, fmt.Errorf("stored reference key: %w", err)
	}
	path := r.URL.Query().Get("path")
	if root.Type() != key.DirLeaf && root.Type() != key.DirNode {
		// The ref points straight at a file (or something odd); only the
		// empty path can address it.
		if hasComponents(path) {
			return browseTarget{}, http.StatusBadRequest, fmt.Errorf("reference %q does not point at a directory", name)
		}
		return browseTarget{key: root}, 0, nil
	}
	ent, err := fstree.ResolveEntry(root, path, h.objects.Get)
	switch {
	case errors.Is(err, fstree.ErrNotDir):
		return browseTarget{}, http.StatusBadRequest, err
	case errors.Is(err, fstree.ErrNotFound), errors.Is(err, diskstore.ErrNotFound):
		return browseTarget{}, http.StatusNotFound, err
	case err != nil:
		return browseTarget{}, http.StatusInternalServerError, err
	}
	if ent == nil {
		return browseTarget{key: root}, 0, nil
	}
	t := browseTarget{entry: ent}
	if len(ent.ContentKey) > 0 {
		k, err := key.Parse(ent.ContentKey)
		if err != nil {
			return browseTarget{}, http.StatusInternalServerError, fmt.Errorf("entry content key: %w", err)
		}
		t.key = k
	}
	return t, 0, nil
}

// entryKind names an entry's file type for the JSON API.
func entryKind(mode uint64) string {
	switch mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return "dir"
	case unix.S_IFREG:
		return "file"
	case unix.S_IFLNK:
		return "symlink"
	case unix.S_IFIFO:
		return "fifo"
	case unix.S_IFCHR:
		return "char"
	case unix.S_IFBLK:
		return "block"
	case unix.S_IFSOCK:
		return "socket"
	default:
		return "unknown"
	}
}

type treeEntry struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Size        uint64 `json:"size,omitempty"`
	Mode        uint64 `json:"mode"`
	Mtime       int64  `json:"mtime"`
	Target      string `json:"target,omitempty"`
	NameInvalid bool   `json:"raw_name_invalid,omitempty"`
}

// entryJSON converts a directory entry for the JSON API. Names are raw
// bytes; JSON cannot carry invalid UTF-8, so such names are replaced
// lossily and flagged so the UI can mark them unnavigable.
func entryJSON(e fstree.Entry) treeEntry {
	name := string(e.Name)
	invalid := !utf8.ValidString(name)
	if invalid {
		name = strings.ToValidUTF8(name, "�")
	}
	out := treeEntry{
		Name:        name,
		Kind:        entryKind(e.Mode),
		Mode:        e.Mode,
		Mtime:       e.Mtime,
		Target:      strings.ToValidUTF8(string(e.LinkTarget), "�"),
		NameInvalid: invalid,
	}
	if len(e.ContentKey) == key.Size {
		if ck, err := key.Parse(e.ContentKey); err == nil {
			out.Size = ck.Length()
		}
	}
	return out
}

// tree serves a directory listing page or, for a non-directory target, its
// stat.
func (h *handler) tree(w http.ResponseWriter, r *http.Request) {
	t, status, err := h.resolveTarget(r)
	if err != nil {
		jsonError(w, status, err.Error())
		return
	}
	switch {
	case t.entry == nil && (t.key.Type() == key.DirLeaf || t.key.Type() == key.DirNode):
		h.treeDir(w, r, t.key)
	case t.entry == nil:
		// A ref pointing straight at a file: no entry metadata exists.
		writeJSON(w, map[string]any{
			"kind": "file",
			"stat": treeEntry{Kind: "file", Size: t.key.Length()},
		})
	case t.entry.Mode&unix.S_IFMT == unix.S_IFDIR:
		h.treeDir(w, r, t.key)
	default:
		st := entryJSON(*t.entry)
		writeJSON(w, map[string]any{"kind": st.Kind, "stat": st})
	}
}

// treeDir serves one page of dir's entries.
func (h *handler) treeDir(w http.ResponseWriter, r *http.Request, dir key.Key) {
	limit := defaultTreeLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			jsonError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = min(n, maxTreeLimit)
	}
	after := []byte(r.URL.Query().Get("after"))
	entries, more, err := fstree.ListEntries(dir, after, limit, h.objects.Get)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, diskstore.ErrNotFound) {
			status = http.StatusNotFound
		}
		jsonError(w, status, err.Error())
		return
	}
	out := make([]treeEntry, len(entries))
	for i, e := range entries {
		out[i] = entryJSON(e)
	}
	writeJSON(w, map[string]any{"kind": "dir", "entries": out, "more": more})
}
```

Note on the file-stat for a file path: the test expects `Stat.Name == "nested.txt"` — `entryJSON(*t.entry)` provides it. The file-ref case has no entry, so only `size` is set.

In `admin/admin.go` `New`, register the route:

```go
	mux.HandleFunc("GET /admin/api/tree", h.authed(h.tree))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./admin/ -run 'TestTree' -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add admin/browse.go admin/admin.go admin/browse_test.go
git commit -m "feat(admin): tree listing endpoint with pagination"
```

---

### Task 7: `GET /admin/api/raw` — view/download a file

**Files:**
- Modify: `admin/browse.go`
- Modify: `admin/admin.go` (route registration)
- Modify: `admin/browse_test.go`

- [ ] **Step 1: Write the failing test**

Append to `admin/browse_test.go` (add `"io"` and `"strings"` to its imports):

```go
func rawURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/raw?" + q.Encode()
}

func TestRawFile(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice@example.com"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	// View: inline disposition, typed by extension, sandboxed.
	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": "hello.txt"}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello, amber" {
		t.Fatalf("raw body = %q", body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain*", ct)
	}
	if resp.Header.Get("Content-Security-Policy") != "sandbox" {
		t.Fatalf("CSP = %q, want sandbox", resp.Header.Get("Content-Security-Policy"))
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "12" {
		t.Fatalf("Content-Length = %q, want 12", cl)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "inline") || !strings.Contains(cd, "hello.txt") {
		t.Fatalf("Content-Disposition = %q, want inline with filename", cd)
	}

	// Download: attachment disposition.
	resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": "hello.txt", "dl": "1"}), cookie, "")
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") {
		t.Fatalf("dl Content-Disposition = %q, want attachment", cd)
	}

	// A ref pointing straight at a file streams with the ref basename and a
	// sniffed type (no extension).
	resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "motd", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("file-ref raw = %d, want 200", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if string(body) != "hello, amber" {
		t.Fatalf("file-ref raw body = %q", body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("sniffed Content-Type = %q, want text/plain*", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "motd") {
		t.Fatalf("file-ref Content-Disposition = %q, want the ref basename", cd)
	}

	// Non-files are 400.
	for _, path := range []string{"sub", "sub/link"} {
		resp = do(t, "GET", rawURL(srv, map[string]string{"ref": "backup/daily", "path": path}), cookie, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("raw %s = %d, want 400", path, resp.StatusCode)
		}
	}
}

func TestRawChunkedFile(t *testing.T) {
	objs := memObjects{}
	first := mustObj(t, fstree.EncodeBlob([]byte("first-")))
	objs.put(first)
	second := mustObj(t, fstree.EncodeBlob([]byte("second")))
	objs.put(second)
	node := mustObj(t, fstree.EncodeFileNode([]key.Key{first.Key, second.Key}))
	objs.put(node)
	refs := memRefs{"chunked": mustRef(t, "chunked", node.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "chunked", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "first-second" {
		t.Fatalf("chunked body = %q, want first-second", body)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "12" {
		t.Fatalf("Content-Length = %q, want 12", cl)
	}
}

func TestRawMissingObject(t *testing.T) {
	objs := memObjects{}
	// A ref whose target object was never stored.
	ghost := mustObj(t, fstree.EncodeBlob([]byte("never stored")))
	refs := memRefs{"ghost": mustRef(t, "ghost", ghost.Key, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", rawURL(srv, map[string]string{"ref": "ghost", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("raw missing object = %d, want 404", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./admin/ -run TestRaw -v`
Expected: FAIL — `raw = 404, want 200` (route does not exist yet)

- [ ] **Step 3: Write the implementation**

Append to `admin/browse.go` (extend its imports with `"io"`, `"mime"`, and `"path"`):

```go
// fileBaseName returns the last slash-separated segment of s, lossily
// UTF-8-cleaned for use in a Content-Disposition filename.
func fileBaseName(s string) string {
	base := s[strings.LastIndexByte(s, '/')+1:]
	base = strings.ToValidUTF8(base, "_")
	if base == "" {
		return "file"
	}
	return base
}

// firstBytes returns up to n leading bytes of the file content at k,
// reading only the leftmost path of its FileNode tree.
func (h *handler) firstBytes(k key.Key, n int) ([]byte, error) {
	for {
		data, err := h.objects.Get(k)
		if err != nil {
			return nil, err
		}
		switch k.Type() {
		case key.Blob:
			if len(data) > n {
				data = data[:n]
			}
			return data, nil
		case key.FileNode:
			children, err := fstree.DecodeFileNode(data)
			if err != nil {
				return nil, err
			}
			if len(children) == 0 {
				return nil, nil
			}
			k = children[0]
		default:
			return nil, fmt.Errorf("%s is not a file-content object (type %v)", k, k.Type())
		}
	}
}

// writeFileContent streams the file content at k, descending FileNode
// levels and concatenating Blob leaves in order.
func (h *handler) writeFileContent(w io.Writer, k key.Key) error {
	data, err := h.objects.Get(k)
	if err != nil {
		return err
	}
	switch k.Type() {
	case key.Blob:
		_, err := w.Write(data)
		return err
	case key.FileNode:
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			return err
		}
		for _, ck := range children {
			if err := h.writeFileContent(w, ck); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s is not a file-content object (type %v)", k, k.Type())
	}
}

// raw streams a regular file's bytes: inline for viewing (the default) or
// as an attachment (?dl=1). Stored content is untrusted, so every response
// is sandboxed — a stored HTML file must not script against the admin
// session.
func (h *handler) raw(w http.ResponseWriter, r *http.Request) {
	t, status, err := h.resolveTarget(r)
	if err != nil {
		jsonError(w, status, err.Error())
		return
	}
	var filename string
	switch {
	case t.entry == nil:
		if t.key.Type() != key.Blob && t.key.Type() != key.FileNode {
			jsonError(w, http.StatusBadRequest, "not a file")
			return
		}
		filename = fileBaseName(r.URL.Query().Get("ref"))
	case t.entry.Mode&unix.S_IFMT == unix.S_IFREG:
		filename = fileBaseName(string(t.entry.Name))
	default:
		jsonError(w, http.StatusBadRequest, "not a regular file")
		return
	}

	ctype := mime.TypeByExtension(path.Ext(filename))
	if ctype == "" {
		head, err := h.firstBytes(t.key, 512)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, diskstore.ErrNotFound) {
				status = http.StatusNotFound
			}
			jsonError(w, status, err.Error())
			return
		}
		ctype = http.DetectContentType(head)
	}
	disposition := "inline"
	if r.URL.Query().Get("dl") != "" {
		disposition = "attachment"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Length", strconv.FormatUint(t.key.Length(), 10))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox")
	w.Header().Set("Content-Disposition",
		mime.FormatMediaType(disposition, map[string]string{"filename": filename}))
	if err := h.writeFileContent(w, t.key); err != nil {
		// Headers are sent; the only honest move is to abort the
		// connection so the client sees a truncated transfer, not a
		// silently short "success".
		h.log.Error("aborting raw file stream", "error", err)
		panic(http.ErrAbortHandler)
	}
}
```

Wait — `TestRawMissingObject` expects 404 before any body is written. The `firstBytes` call (sniffing for the extensionless "ghost" ref) hits the missing object and returns 404 — that's the pre-header path, covered above. For typed files whose first read fails, the `Content-Length` mismatch path aborts mid-stream, which the spec accepts. No code change needed; this note is for the implementer.

In `admin/admin.go` `New`, register the route:

```go
	mux.HandleFunc("GET /admin/api/raw", h.authed(h.raw))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./admin/ -run TestRaw -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add admin/browse.go admin/admin.go admin/browse_test.go
git commit -m "feat(admin): raw file endpoint"
```

---

### Task 8: `GET /admin/api/archive` — `.tar` / `.tar.gz` download

**Files:**
- Modify: `admin/browse.go`
- Modify: `admin/admin.go` (route registration)
- Modify: `admin/browse_test.go`

- [ ] **Step 1: Write the failing test**

Append to `admin/browse_test.go` (add `"archive/tar"` and `"compress/gzip"` to its imports):

```go
func archiveURL(srv *httptest.Server, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	return srv.URL + "/admin/api/archive?" + q.Encode()
}

// tarNames reads all member names (and the content of regular files) from a
// tar stream.
func tarNames(t *testing.T, rd io.Reader) map[string]string {
	t.Helper()
	out := map[string]string{}
	tr := tar.NewReader(rd)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content := ""
		if hdr.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			content = string(b)
		}
		out[hdr.Name] = content
	}
	return out
}

func wantSeedTreeMembers(t *testing.T, got map[string]string) {
	t.Helper()
	want := map[string]string{
		"hello.txt":      "hello, amber",
		"sub/":           "",
		"sub/link":       "",
		"sub/nested.txt": "nested",
	}
	if len(got) != len(want) {
		t.Fatalf("archive members = %v, want %v", got, want)
	}
	for name, content := range want {
		if c, ok := got[name]; !ok || c != content {
			t.Fatalf("member %q = %q,%v want %q", name, c, ok, content)
		}
	}
}

func TestArchiveTar(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	// Default format is tar; the filename derives from the ref basename.
	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "path": ""}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("archive = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-tar" {
		t.Fatalf("Content-Type = %q, want application/x-tar", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") || !strings.Contains(cd, "daily.tar") {
		t.Fatalf("Content-Disposition = %q, want attachment daily.tar", cd)
	}
	wantSeedTreeMembers(t, tarNames(t, resp.Body))

	// A subdirectory archive contains only that subtree.
	resp = do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "path": "sub", "format": "tar"}), cookie, "")
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "sub.tar") {
		t.Fatalf("Content-Disposition = %q, want sub.tar", cd)
	}
	got := tarNames(t, resp.Body)
	if len(got) != 2 || got["nested.txt"] != "nested" {
		t.Fatalf("sub archive members = %v", got)
	}
}

func TestArchiveTgz(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, cookie := browseServer(t, objs, refs)

	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "backup/daily", "format": "tgz"}), cookie, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("archive = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("Content-Type = %q, want application/gzip", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "daily.tar.gz") {
		t.Fatalf("Content-Disposition = %q, want daily.tar.gz", cd)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	wantSeedTreeMembers(t, tarNames(t, gz))
}

func TestArchiveErrors(t *testing.T) {
	objs := memObjects{}
	root, hello := seedTree(t, objs)
	refs := memRefs{
		"backup/daily": mustRef(t, "backup/daily", root, "alice"),
		"motd":         mustRef(t, "motd", hello, "bob"),
	}
	srv, cookie := browseServer(t, objs, refs)

	for name, params := range map[string]map[string]string{
		"file path":  {"ref": "backup/daily", "path": "hello.txt"},
		"file ref":   {"ref": "motd"},
		"bad format": {"ref": "backup/daily", "format": "zip"},
	} {
		resp := do(t, "GET", archiveURL(srv, params), cookie, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", name, resp.StatusCode)
		}
	}
	resp := do(t, "GET", archiveURL(srv, map[string]string{"ref": "nope"}), cookie, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown ref = %d, want 404", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./admin/ -run TestArchive -v`
Expected: FAIL — `archive = 404, want 200` (route does not exist yet)

- [ ] **Step 3: Write the implementation**

Append to `admin/browse.go` (extend its imports with `"compress/gzip"` and `"github.com/draganm/amber-store/tarexport"`):

```go
// archive streams the directory at ref+path as a tar (format=tar, the
// default) or gzipped tar (format=tgz) attachment.
func (h *handler) archive(w http.ResponseWriter, r *http.Request) {
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "tar"
	}
	if format != "tar" && format != "tgz" {
		jsonError(w, http.StatusBadRequest, "format must be tar or tgz")
		return
	}
	t, status, err := h.resolveTarget(r)
	if err != nil {
		jsonError(w, status, err.Error())
		return
	}
	if t.entry != nil && t.entry.Mode&unix.S_IFMT != unix.S_IFDIR {
		jsonError(w, http.StatusBadRequest, "not a directory")
		return
	}
	if t.key.Type() != key.DirLeaf && t.key.Type() != key.DirNode {
		jsonError(w, http.StatusBadRequest, "not a directory")
		return
	}

	// The filename derives from what was archived: the path's basename,
	// or the ref's for the root.
	base := r.URL.Query().Get("ref")
	if p := strings.Trim(r.URL.Query().Get("path"), "/"); p != "" {
		base = p
	}
	base = fileBaseName(base)

	abort := func(err error) {
		h.log.Error("aborting archive stream", "error", err)
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch format {
	case "tar":
		w.Header().Set("Content-Type", "application/x-tar")
		w.Header().Set("Content-Disposition",
			mime.FormatMediaType("attachment", map[string]string{"filename": base + ".tar"}))
		if err := tarexport.Write(w, t.key, h.objects.Get); err != nil {
			abort(err)
		}
	case "tgz":
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition",
			mime.FormatMediaType("attachment", map[string]string{"filename": base + ".tar.gz"}))
		gz := gzip.NewWriter(w)
		if err := tarexport.Write(gz, t.key, h.objects.Get); err != nil {
			abort(err)
		}
		if err := gz.Close(); err != nil {
			abort(err)
		}
	}
}
```

In `admin/admin.go` `New`, register the route:

```go
	mux.HandleFunc("GET /admin/api/archive", h.authed(h.archive))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./admin/ -run TestArchive -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add admin/browse.go admin/admin.go admin/browse_test.go
git commit -m "feat(admin): archive download endpoint"
```

---

### Task 9: Every browse endpoint requires a session; serve wiring smoke test

**Files:**
- Modify: `admin/browse_test.go`
- Modify: `cmd/amber-store/serve_admin_test.go:66-136` (extend `TestServeAdminUI`)

- [ ] **Step 1: Write the failing-or-passing auth table test**

Append to `admin/browse_test.go`:

```go
// TestBrowseRequiresSession is the spec's authentication gate: no live
// session cookie, no content — on every browse endpoint.
func TestBrowseRequiresSession(t *testing.T) {
	objs := memObjects{}
	root, _ := seedTree(t, objs)
	refs := memRefs{"backup/daily": mustRef(t, "backup/daily", root, "alice")}
	srv, _ := browseServer(t, objs, refs)

	for _, path := range []string{
		"/admin/api/refs",
		"/admin/api/tree?ref=backup/daily",
		"/admin/api/raw?ref=backup/daily&path=hello.txt",
		"/admin/api/archive?ref=backup/daily",
	} {
		resp := do(t, "GET", srv.URL+path, nil, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s without session = %d, want 401", path, resp.StatusCode)
		}
	}
}
```

Run: `go test ./admin/ -run TestBrowseRequiresSession -v`
Expected: PASS — every route was registered through `h.authed`. If any check fails, the route registration in `admin/admin.go` is wrong: fix it to go through `h.authed` before continuing.

- [ ] **Step 2: Extend the serve e2e test**

In `cmd/amber-store/serve_admin_test.go`, append to the end of `TestServeAdminUI` (after the keys listing check):

```go
	req, err = http.NewRequest("GET", base+"/admin/api/refs", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(session)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != `{"refs":[]}` {
		t.Fatalf("refs = %d %q, want an empty refs listing", resp.StatusCode, body)
	}
```

- [ ] **Step 3: Run the tests**

Run: `go test ./admin/ ./cmd/amber-store/ -count=1`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add admin/browse_test.go cmd/amber-store/serve_admin_test.go
git commit -m "test: browse endpoints require a session; serve wiring smoke test"
```

---

### Task 10: UI — API helpers, hash router, header nav, KeysPage extraction

No JS test infra exists (the API tests cover the contract; `vite build` catches compile errors) — so UI tasks verify with a build instead of unit tests.

**Files:**
- Modify: `cmd/amber-store/ui/src/api.js`
- Create: `cmd/amber-store/ui/src/routes.js`
- Create: `cmd/amber-store/ui/src/KeysPage.jsx`
- Modify: `cmd/amber-store/ui/src/Console.jsx`
- Modify: `cmd/amber-store/ui/src/app.css`

- [ ] **Step 1: Add the API helpers**

Append to `cmd/amber-store/ui/src/api.js`:

```js
export const listRefs = () => call('GET', '/admin/api/refs');
export const listTree = (refName, path, after, limit) => {
  const q = new URLSearchParams({ ref: refName, path: path || '' });
  if (after) q.set('after', after);
  if (limit) q.set('limit', String(limit));
  return call('GET', `/admin/api/tree?${q}`);
};
// rawURL/archiveURL feed plain <a href> links; the session cookie
// authenticates same-site navigations.
export const rawURL = (refName, path, dl) => {
  const q = new URLSearchParams({ ref: refName, path: path || '' });
  if (dl) q.set('dl', '1');
  return `/admin/api/raw?${q}`;
};
export const archiveURL = (refName, path, format) =>
  `/admin/api/archive?${new URLSearchParams({ ref: refName, path: path || '', format })}`;
```

- [ ] **Step 2: Add the route helper**

Create `cmd/amber-store/ui/src/routes.js`:

```js
// Hash-route helpers. Every segment is URL-encoded, so a ref name that
// contains '/' travels as a single segment.

export function browseHref(refName, path) {
  let h = `#/refs/${encodeURIComponent(refName)}`;
  if (path) h += '/' + path.split('/').map(encodeURIComponent).join('/');
  return h;
}

// parseRoute reads location.hash: #/keys | #/refs | #/refs/<ref>[/<path>…]
export function parseRoute() {
  const h = window.location.hash.replace(/^#\/?/, '');
  if (h === 'refs') return { page: 'refs' };
  if (h.startsWith('refs/')) {
    const segs = h.slice('refs/'.length).split('/').map(decodeURIComponent);
    return { page: 'browser', refName: segs[0], path: segs.slice(1).join('/') };
  }
  return { page: 'keys' };
}
```

- [ ] **Step 3: Extract KeysPage**

Create `cmd/amber-store/ui/src/KeysPage.jsx` — this is the existing `Console.jsx` minus the header (the `<main>` and `<footer>` blocks and all their state move here verbatim):

```jsx
import { For, Show, createResource, createSignal } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import KeyRow from './KeyRow';

// The allowed-keys console: key list, add panel, count footer.
export default function KeysPage(props) {
  const [keys, { refetch }] = createResource(async () => {
    try {
      const data = await api.listKeys();
      return data.keys ?? [];
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      throw err;
    }
  });

  const [line, setLine] = createSignal('');
  const [admin, setAdmin] = createSignal(false);
  const [addError, setAddError] = createSignal('');
  const [busy, setBusy] = createSignal(false);

  const add = async (e) => {
    e.preventDefault();
    if (busy()) return;
    setBusy(true);
    setAddError('');
    try {
      await api.addKey(line().trim(), admin());
      setLine('');
      setAdmin(false);
      refetch();
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      setAddError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (fingerprint) => {
    try {
      await api.removeKey(fingerprint);
      refetch();
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      else refetch();
    }
  };

  return (
    <>
      <main class="console container">
        <div class="eyebrow">Allowed keys</div>
        <h1 class="console__title">Keys that may talk to this server.</h1>
        <p class="console__sub">
          Changes apply immediately and are written to the server's
          allowed-keys file. Keys marked <code>admin</code> may delete
          references and bypass reference ownership.
        </p>

        <Show
          when={!keys.loading || keys()}
          fallback={<div class="empty">Loading keys…</div>}
        >
          <Show when={!keys.error} fallback={<div class="empty">{String(keys.error?.message || keys.error)}</div>}>
            <div class="keys">
              <For
                each={keys()}
                fallback={
                  <div class="empty">
                    No keys yet. Nothing can talk to this server — add the
                    first key below.
                  </div>
                }
              >
                {(key) => <KeyRow key={key} onRemove={remove} />}
              </For>
            </div>
          </Show>
        </Show>

        <form class="add-panel" onSubmit={add}>
          <h2 class="add-panel__title">Add a key</h2>
          <label class="field-label" for="key-line">
            authorized_keys line
          </label>
          <textarea
            id="key-line"
            class="input input--mono"
            classList={{ 'input--error': !!addError() }}
            placeholder="ssh-ed25519 AAAAC3… backup-host"
            value={line()}
            onInput={(e) => setLine(e.currentTarget.value)}
            rows="3"
          />
          <Show
            when={addError()}
            fallback={
              <div class="help">
                Paste the public key as one line — type, base64 key, and an
                optional comment to name it.
              </div>
            }
          >
            <div class="help help--error">{addError()}</div>
          </Show>
          <div class="add-panel__row">
            <label class="checkbox">
              <input
                type="checkbox"
                checked={admin()}
                onChange={(e) => setAdmin(e.currentTarget.checked)}
              />
              Admin key
              <span class="hint">may delete references</span>
            </label>
            <button
              type="submit"
              class="btn btn--primary"
              disabled={busy() || !line().trim()}
            >
              Add key →
            </button>
          </div>
        </form>
      </main>

      <footer class="console-footer">
        <div class="container console-footer__row">
          <span>AMBER STORE · ADMIN CONSOLE</span>
          <span>{(keys() ?? []).length} KEYS</span>
        </div>
      </footer>
    </>
  );
}
```

- [ ] **Step 4: Rewrite Console as the routed shell**

Replace the entire content of `cmd/amber-store/ui/src/Console.jsx` with:

```jsx
import { Show, createSignal, onCleanup } from 'solid-js';
import * as api from './api';
import logoBlack from './assets/Logo_Black.png';
import { parseRoute } from './routes';
import KeysPage from './KeysPage';
import RefsPage from './RefsPage';
import BrowserPage from './BrowserPage';

// Signed-in shell: sticky header with the Keys/Refs nav, hash-routed
// pages underneath. Routes: #/keys, #/refs, #/refs/<ref>[/<path>…].
export default function Console(props) {
  const [route, setRoute] = createSignal(parseRoute());
  const onHash = () => setRoute(parseRoute());
  window.addEventListener('hashchange', onHash);
  onCleanup(() => window.removeEventListener('hashchange', onHash));

  const signOut = async () => {
    try {
      await api.logout();
    } finally {
      props.onSignOut();
    }
  };

  return (
    <>
      <header class="site-header">
        <div class="container site-header__row">
          <div class="site-header__brand">
            <img src={logoBlack} alt="Fables for Robots" />
            <span class="site-header__app">AMBER STORE · ADMIN</span>
          </div>
          <nav class="site-nav">
            <a
              href="#/keys"
              classList={{ 'site-nav__link': true, 'site-nav__link--active': route().page === 'keys' }}
            >
              Keys
            </a>
            <a
              href="#/refs"
              classList={{ 'site-nav__link': true, 'site-nav__link--active': route().page !== 'keys' }}
            >
              Refs
            </a>
          </nav>
          <button class="btn btn--ghost" onClick={signOut}>
            Sign out
          </button>
        </div>
      </header>

      <Show when={route().page === 'keys'}>
        <KeysPage onSignOut={props.onSignOut} />
      </Show>
      <Show when={route().page === 'refs'}>
        <RefsPage onSignOut={props.onSignOut} />
      </Show>
      <Show when={route().page === 'browser'}>
        <BrowserPage
          refName={route().refName}
          path={route().path}
          onSignOut={props.onSignOut}
        />
      </Show>
    </>
  );
}
```

Create placeholder pages so the build passes until Tasks 11–12 fill them in — `cmd/amber-store/ui/src/RefsPage.jsx`:

```jsx
export default function RefsPage() {
  return <main class="console container"><div class="empty">Loading…</div></main>;
}
```

and `cmd/amber-store/ui/src/BrowserPage.jsx`:

```jsx
export default function BrowserPage() {
  return <main class="console container"><div class="empty">Loading…</div></main>;
}
```

- [ ] **Step 5: Add the nav styles**

Append to `cmd/amber-store/ui/src/app.css`:

```css
/* ---- header nav (keys / refs) ---- */
.site-nav {
  display: flex;
  gap: 4px;
  margin-left: auto;
  margin-right: 16px;
}
.site-nav__link {
  padding: 8px 14px;
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  text-decoration: none;
  color: #111;
  border: 1.5px solid transparent;
}
.site-nav__link--active {
  border-color: #111;
}
.site-nav__link:hover {
  border-color: #2db6be;
}
```

- [ ] **Step 6: Verify the SPA builds**

Run: `npm --prefix cmd/amber-store/ui ci && npm --prefix cmd/amber-store/ui run build`
Expected: vite build succeeds (output under `cmd/amber-store/ui/dist/`); do NOT commit `dist` yet — Task 13 regenerates it once.

- [ ] **Step 7: Commit**

```bash
git add cmd/amber-store/ui/src/api.js cmd/amber-store/ui/src/routes.js cmd/amber-store/ui/src/KeysPage.jsx cmd/amber-store/ui/src/Console.jsx cmd/amber-store/ui/src/RefsPage.jsx cmd/amber-store/ui/src/BrowserPage.jsx cmd/amber-store/ui/src/app.css
git commit -m "feat(ui): header nav and hash routing for the admin SPA"
```

---

### Task 11: UI — RefsPage

**Files:**
- Modify: `cmd/amber-store/ui/src/RefsPage.jsx` (replace the placeholder)
- Modify: `cmd/amber-store/ui/src/app.css`

- [ ] **Step 1: Implement the page**

Replace the entire content of `cmd/amber-store/ui/src/RefsPage.jsx` with:

```jsx
import { For, Show, createResource, createSignal } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import { browseHref } from './routes';

const fmtDate = (iso) => (iso ? new Date(iso).toLocaleString() : '');

// The refs listing: every reference on the server, filterable by name.
// Directory refs link into the browser; file refs view/download directly.
export default function RefsPage(props) {
  const [refs] = createResource(async () => {
    try {
      const data = await api.listRefs();
      return data.refs ?? [];
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      throw err;
    }
  });
  const [filter, setFilter] = createSignal('');
  const shown = () => (refs() ?? []).filter((r) => r.name.includes(filter()));

  return (
    <main class="console container">
      <div class="eyebrow">References</div>
      <h1 class="console__title">Everything this server points at.</h1>
      <p class="console__sub">
        Click a reference to browse its tree, view files, or download
        archives.
      </p>

      <input
        class="input refs-filter"
        placeholder="Filter by name…"
        value={filter()}
        onInput={(e) => setFilter(e.currentTarget.value)}
      />

      <Show
        when={!refs.loading || refs()}
        fallback={<div class="empty">Loading refs…</div>}
      >
        <Show when={!refs.error} fallback={<div class="empty">{String(refs.error?.message || refs.error)}</div>}>
          <div class="refs">
            <For
              each={shown()}
              fallback={<div class="empty">No references.</div>}
            >
              {(ref) => (
                <div class="ref-row">
                  <div class="ref-row__main">
                    <Show
                      when={ref.kind === 'dir'}
                      fallback={<span class="ref-row__name">{ref.name}</span>}
                    >
                      <a class="ref-row__name" href={browseHref(ref.name, '')}>
                        {ref.name}
                      </a>
                    </Show>
                    <span class="badge">{ref.kind}</span>
                    <div class="ref-row__meta">
                      {[ref.user, fmtDate(ref.created_at)].filter(Boolean).join(' · ')}
                    </div>
                  </div>
                  <div class="ref-row__actions">
                    <Show when={ref.kind === 'file'}>
                      <a class="btn btn--ghost" href={api.rawURL(ref.name, '')} target="_blank" rel="noopener">
                        View
                      </a>
                      <a class="btn btn--ghost" href={api.rawURL(ref.name, '', true)}>
                        Download
                      </a>
                    </Show>
                    <Show when={ref.kind === 'dir'}>
                      <a class="btn btn--ghost" href={api.archiveURL(ref.name, '', 'tar')}>
                        .tar
                      </a>
                      <a class="btn btn--ghost" href={api.archiveURL(ref.name, '', 'tgz')}>
                        .tar.gz
                      </a>
                    </Show>
                  </div>
                </div>
              )}
            </For>
          </div>
        </Show>
      </Show>
    </main>
  );
}
```

- [ ] **Step 2: Add the styles**

Append to `cmd/amber-store/ui/src/app.css`:

```css
/* ---- refs list ---- */
.refs-filter {
  margin-top: 18px;
  max-width: 420px;
}
.refs {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 18px;
}
.ref-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px;
  border: 1.5px solid #111;
  background: #fff;
}
.ref-row__main {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
}
.ref-row__name {
  font-weight: 600;
  color: #111;
  text-decoration: none;
  overflow-wrap: anywhere;
}
a.ref-row__name:hover {
  color: #2db6be;
}
.ref-row__meta {
  color: #666;
  font-size: 13px;
  white-space: nowrap;
}
.ref-row__actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
}
```

- [ ] **Step 3: Verify the SPA builds**

Run: `npm --prefix cmd/amber-store/ui run build`
Expected: vite build succeeds

- [ ] **Step 4: Commit**

```bash
git add cmd/amber-store/ui/src/RefsPage.jsx cmd/amber-store/ui/src/app.css
git commit -m "feat(ui): refs page"
```

---

### Task 12: UI — BrowserPage

**Files:**
- Modify: `cmd/amber-store/ui/src/BrowserPage.jsx` (replace the placeholder)
- Modify: `cmd/amber-store/ui/src/app.css`

- [ ] **Step 1: Implement the page**

Replace the entire content of `cmd/amber-store/ui/src/BrowserPage.jsx` with:

```jsx
import { For, Show, createEffect, createSignal, on } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import { browseHref } from './routes';

function fmtSize(n) {
  if (n === undefined || n === null) return '—';
  let v = n;
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return i === 0 ? `${v} B` : `${v.toFixed(1)} ${units[i]}`;
}
const fmtMode = (m) => (m & 0o7777).toString(8).padStart(4, '0');
const fmtTime = (ns) => (ns ? new Date(ns / 1e6).toLocaleString() : '—');

// One ref's tree: breadcrumbs, ls -l style listing with cursor
// pagination, per-file view/download, per-directory tar downloads.
export default function BrowserPage(props) {
  const [entries, setEntries] = createSignal([]);
  const [stat, setStat] = createSignal(null); // non-dir target: {kind, stat}
  const [more, setMore] = createSignal(false);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal('');

  const load = async (after) => {
    setLoading(true);
    setError('');
    try {
      const res = await api.listTree(props.refName, props.path, after);
      if (res.kind === 'dir') {
        setEntries(after ? entries().concat(res.entries ?? []) : res.entries ?? []);
        setMore(res.more);
        setStat(null);
      } else {
        setStat(res);
        setEntries([]);
        setMore(false);
      }
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  createEffect(on(
    () => [props.refName, props.path],
    () => {
      setEntries([]);
      setStat(null);
      load();
    },
  ));

  const crumbs = () => {
    const segs = props.path ? props.path.split('/') : [];
    const out = [{ label: props.refName, href: browseHref(props.refName, '') }];
    segs.forEach((seg, i) => out.push({
      label: seg,
      href: browseHref(props.refName, segs.slice(0, i + 1).join('/')),
    }));
    return out;
  };

  const childPath = (name) => (props.path ? `${props.path}/${name}` : name);

  return (
    <main class="console container">
      <div class="eyebrow">Reference browser</div>
      <nav class="crumbs">
        <For each={crumbs()}>
          {(c, i) => (
            <>
              <Show when={i() > 0}>
                <span class="crumbs__sep">/</span>
              </Show>
              <a href={c.href}>{c.label}</a>
            </>
          )}
        </For>
      </nav>

      <Show when={!stat() && !error()}>
        <div class="browser-actions">
          <a class="btn btn--ghost" href={api.archiveURL(props.refName, props.path, 'tar')}>
            Download .tar
          </a>
          <a class="btn btn--ghost" href={api.archiveURL(props.refName, props.path, 'tgz')}>
            Download .tar.gz
          </a>
        </div>
      </Show>

      <Show when={error()}>
        <div class="empty">{error()}</div>
      </Show>

      <Show when={stat()}>
        <div class="tree">
          <div class="tree-row">
            <span class="tree-row__name">{stat().stat?.name || props.refName}</span>
            <span class="badge">{stat().kind}</span>
            <span class="tree-row__meta">{fmtSize(stat().stat?.size)}</span>
            <span class="tree-row__meta" />
            <span class="tree-row__meta" />
            <span class="tree-row__actions">
              <Show when={stat().kind === 'file'}>
                <a class="btn btn--ghost" href={api.rawURL(props.refName, props.path)} target="_blank" rel="noopener">
                  View
                </a>
                <a class="btn btn--ghost" href={api.rawURL(props.refName, props.path, true)}>
                  Download
                </a>
              </Show>
            </span>
          </div>
        </div>
      </Show>

      <Show when={!stat() && !error()}>
        <div class="tree">
          <div class="tree-head">
            <span>name</span>
            <span>kind</span>
            <span>size</span>
            <span>modified</span>
            <span>mode</span>
            <span />
          </div>
          <For
            each={entries()}
            fallback={
              <Show when={!loading()}>
                <div class="empty">Empty directory.</div>
              </Show>
            }
          >
            {(e) => (
              <div class="tree-row">
                <Show
                  when={e.kind === 'dir' && !e.raw_name_invalid}
                  fallback={
                    <span
                      class="tree-row__name"
                      classList={{ 'tree-row__name--invalid': !!e.raw_name_invalid }}
                      title={e.raw_name_invalid ? 'name is not valid UTF-8; shown lossily' : undefined}
                    >
                      {e.name}
                      <Show when={e.kind === 'symlink'}>
                        <span class="tree-row__target"> → {e.target}</span>
                      </Show>
                    </span>
                  }
                >
                  <a class="tree-row__name" href={browseHref(props.refName, childPath(e.name))}>
                    {e.name}/
                  </a>
                </Show>
                <span class="badge">{e.kind}</span>
                <span class="tree-row__meta">{e.kind === 'file' ? fmtSize(e.size) : '—'}</span>
                <span class="tree-row__meta">{fmtTime(e.mtime)}</span>
                <span class="tree-row__meta tree-row__mode">{fmtMode(e.mode)}</span>
                <span class="tree-row__actions">
                  <Show when={e.kind === 'file' && !e.raw_name_invalid}>
                    <a class="btn btn--ghost" href={api.rawURL(props.refName, childPath(e.name))} target="_blank" rel="noopener">
                      View
                    </a>
                    <a class="btn btn--ghost" href={api.rawURL(props.refName, childPath(e.name), true)}>
                      Download
                    </a>
                  </Show>
                </span>
              </div>
            )}
          </For>
          <Show when={loading()}>
            <div class="empty">Loading…</div>
          </Show>
          <Show when={more() && !loading()}>
            <button class="btn btn--ghost load-more" onClick={() => load(entries().at(-1)?.name)}>
              Load more
            </button>
          </Show>
        </div>
      </Show>
    </main>
  );
}
```

- [ ] **Step 2: Add the styles**

Append to `cmd/amber-store/ui/src/app.css`:

```css
/* ---- tree browser ---- */
.crumbs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: baseline;
  margin: 6px 0 14px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
}
.crumbs a {
  color: #111;
  text-decoration: none;
}
.crumbs a:hover {
  color: #2db6be;
}
.crumbs__sep {
  color: #999;
}
.browser-actions {
  display: flex;
  gap: 8px;
  margin-bottom: 14px;
}
.tree {
  display: flex;
  flex-direction: column;
  border: 1.5px solid #111;
  background: #fff;
}
.tree-head,
.tree-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 90px 90px 170px 70px 170px;
  gap: 10px;
  align-items: center;
  padding: 10px 14px;
}
.tree-head {
  font-size: 11px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: #666;
  border-bottom: 1.5px solid #111;
}
.tree-row + .tree-row {
  border-top: 1px solid #e2e2e2;
}
.tree-row__name {
  font-weight: 600;
  color: #111;
  text-decoration: none;
  overflow-wrap: anywhere;
}
a.tree-row__name:hover {
  color: #2db6be;
}
.tree-row__name--invalid {
  color: #d23;
}
.tree-row__target {
  color: #666;
  font-weight: 400;
}
.tree-row__meta {
  color: #666;
  font-size: 13px;
  white-space: nowrap;
}
.tree-row__mode {
  font-family: 'JetBrains Mono', monospace;
}
.tree-row__actions {
  display: flex;
  gap: 6px;
  justify-content: flex-end;
}
.load-more {
  margin: 10px 14px;
  align-self: center;
}
```

- [ ] **Step 3: Verify the SPA builds**

Run: `npm --prefix cmd/amber-store/ui run build`
Expected: vite build succeeds

- [ ] **Step 4: Commit**

```bash
git add cmd/amber-store/ui/src/BrowserPage.jsx cmd/amber-store/ui/src/app.css
git commit -m "feat(ui): ref tree browser page"
```

---

### Task 13: Regenerate the embedded dist; full verification

**Files:**
- Modify: `cmd/amber-store/ui/dist/**` (generated)

- [ ] **Step 1: Regenerate the embedded SPA**

Run: `go generate ./cmd/amber-store`
Expected: npm ci + vite build succeed; `git status` shows changes under `cmd/amber-store/ui/dist/`

- [ ] **Step 2: Run the full test suite and format checks**

Run: `gofmt -l . && go vet ./... && go test ./... -count=1`
Expected: `gofmt -l` prints nothing; vet clean; ALL packages PASS (the serve e2e test now exercises the freshly embedded dist)

- [ ] **Step 3: Commit the dist**

```bash
git add cmd/amber-store/ui/dist
git commit -m "chore: regenerate admin UI dist with the ref browser"
```

- [ ] **Step 4: Manual smoke check (recommended)**

```bash
AMBER_ADMIN_PASSWORD=test go run ./cmd/amber-store serve --store /tmp/amber-browse-test --listen :8590 --sync=false
```

Open `http://localhost:8590/admin/`, log in with `test`, and confirm: the Keys/Refs nav renders, the Refs tab shows "No references." (an empty store has none), and signing out returns to the login hero. Stop the server and `rm -rf /tmp/amber-browse-test` afterwards. For a populated walkthrough, ingest a tree with the daemon flow and push it to this server, then click through directories, View, Download, and both archive buttons.

---

## Spec-coverage checklist (self-review)

- Authentication on every endpoint → Tasks 5–8 register via `h.authed`; Task 9 asserts 401s. ✓
- O(log n) lookup + cursor pagination (`LookupEntry`, `ListEntries`, `SepName` skipping) → Tasks 1–2. ✓
- `ResolvePath` reimplementation + `ResolveEntry` → Task 3. ✓
- `/admin/api/refs` with `kind` and `invalid` records → Task 5. ✓
- `/admin/api/tree` with full metadata, `limit` default 500 / cap 1000, `after` cursor, file stat, file-ref stat → Task 6. ✓
- `/admin/api/raw`: extension+sniff typing, Content-Length, inline vs `dl=1`, CSP sandbox + nosniff, regular-files-only, file-ref with ref-basename filename, mid-stream abort → Task 7. ✓
- `/admin/api/archive`: tar + tgz, attachment filenames from basename, directory-only, nosniff → Task 8. ✓
- Query-parameter transport (ref names may contain `/`) → all endpoints + `routes.js` encoding. ✓
- Non-UTF-8 names listed lossily with `raw_name_invalid`, unnavigable in the UI → Tasks 6, 12. ✓
- 404 unknown ref/path/missing object, 400 bad params/wrong kind → Tasks 6–8 tests. ✓
- UI: hash routing, nav tabs, refs page with filter, breadcrumb browser, Load more, View (new tab, noopener) / Download links, per-dir tar buttons → Tasks 10–12. ✓
- Committed dist via `go generate` → Task 13. ✓
- Out of scope (mutations, search, range requests, refs-list pagination) → no tasks added. ✓
