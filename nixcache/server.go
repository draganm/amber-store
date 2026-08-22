package nixcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/narzstd"
)

// Store is the object access a Server needs. *packstore.Store satisfies it.
type Store interface {
	Get(key.Key) ([]byte, error)
	ViewRecord(key.Key, func([]byte) error) error
}

// Server is the loopback HTTP binary cache nix substitutes from.
type Server struct {
	Store Store
	// Index returns the current index root (zero key: empty index).
	Index func() key.Key
	// Catalog reports whether upstream may have this hashpart. Nil: never.
	Catalog func(hashpart string) bool
	// Fetch obtains a catalogued path's info from upstream and indexes it.
	// Nil: serve only what the index has.
	Fetch func(ctx context.Context, hashpart string) (PathInfo, error)
	// Ensure makes the tree at root locally complete; called for every NAR
	// request, so it must be cheap when nothing is missing. Nil: serve
	// only trees whose root is present.
	Ensure func(ctx context.Context, root key.Key) error
	// Touch reports a narinfo served from the index (a cache hit). Nil: no
	// accounting.
	Touch func(hashpart string)

	sf flights
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case r.Method != http.MethodGet && r.Method != http.MethodHead:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	case path == "nix-cache-info":
		w.Header().Set("Content-Type", "text/x-nix-cache-info")
		fmt.Fprintf(w, "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 10\n")
	case strings.HasSuffix(path, ".narinfo"):
		s.narinfo(w, r, strings.TrimSuffix(path, ".narinfo"))
	case strings.HasPrefix(path, "nar/"):
		s.nar(w, r, path)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) narinfo(w http.ResponseWriter, r *http.Request, hp string) {
	if !validHashPart(hp) {
		http.NotFound(w, r)
		return
	}
	pi, err := Lookup(s.Index(), hp, s.Store.Get)
	if err == nil && s.Touch != nil {
		s.Touch(hp)
	}
	if errors.Is(err, fstree.ErrNotFound) {
		pi, err = s.fetch(r.Context(), hp)
	}
	switch {
	case err == nil:
	case errors.Is(err, errUncatalogued):
		http.NotFound(w, r)
		return
	default:
		http.Error(w, "fetch failed", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "text/x-nix-narinfo")
	w.Write(FormatNarinfo(pi))
}

var errUncatalogued = errors.New("nixcache: path not in catalog")

func (s *Server) fetch(ctx context.Context, hp string) (PathInfo, error) {
	if s.Fetch == nil || s.Catalog == nil || !s.Catalog(hp) {
		return PathInfo{}, errUncatalogued
	}
	return inFlight(ctx, &s.sf, "narinfo:"+hp, func(ctx context.Context) (PathInfo, error) {
		return s.Fetch(ctx, hp)
	})
}

func (s *Server) nar(w http.ResponseWriter, r *http.Request, path string) {
	root, err := NarURLKey(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case s.Ensure != nil:
		// Eviction plus a partial compaction can leave the root without its
		// content, so presence of the root alone proves nothing.
		_, err := inFlight(r.Context(), &s.sf, "nar:"+root.String(), func(ctx context.Context) (struct{}, error) {
			return struct{}{}, s.Ensure(ctx, root)
		})
		if err != nil {
			http.Error(w, "tree fetch failed", http.StatusBadGateway)
			return
		}
	default:
		if _, err := s.Store.Get(root); err != nil {
			http.NotFound(w, r)
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if r.Method == http.MethodHead {
		return
	}
	bw := bufio.NewWriterSize(w, 256<<10)
	if err := narzstd.Write(bw, root, s.Store.Get, s.Store.ViewRecord); err != nil {
		// Headers are sent; closing without a final chunk aborts the client.
		panic(http.ErrAbortHandler)
	}
	bw.Flush()
}

func validHashPart(hp string) bool {
	if len(hp) != hashPartLen {
		return false
	}
	for i := 0; i < len(hp); i++ {
		if strings.IndexByte(nixBase32, hp[i]) < 0 {
			return false
		}
	}
	return true
}
