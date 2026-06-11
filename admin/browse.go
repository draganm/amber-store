package admin

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
			Name:      rec.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Kind:      refKind(k),
		})
	}
	writeJSON(w, map[string]any{"refs": out})
}

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
