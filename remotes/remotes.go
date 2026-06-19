// Package remotes persists the daemon's registered remotes: a name → {URL,
// pinned server public key} map stored as JSON at <store-dir>/remotes. The
// pinned key is recorded once at `remote add` (after the user confirms its
// fingerprint) and enforced on every response from that remote.
package remotes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
)

// ErrExists reports an Add for a name that is already registered.
var ErrExists = errors.New("remote already exists")

// ErrNotFound reports an operation on an unregistered name.
var ErrNotFound = errors.New("remote not found")

// ErrInvalid reports a remote that fails validation (bad name, URL or key).
var ErrInvalid = errors.New("invalid remote")

// Remote is one registered remote server.
type Remote struct {
	URL       string `json:"url"`
	ServerKey []byte `json:"server_key"` // pinned public key, SSH wire format
}

// Named pairs a remote with its name, for listings.
type Named struct {
	Name string
	Remote
}

// Registry is the persistent name → Remote map. Safe for concurrent use.
type Registry struct {
	path    string
	mu      sync.Mutex
	remotes map[string]Remote
}

// Open loads the registry at path; a missing file is an empty registry.
func Open(path string) (*Registry, error) {
	r := &Registry{path: path, remotes: map[string]Remote{}}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading remotes file: %w", err)
	}
	if err := json.Unmarshal(b, &r.remotes); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return r, nil
}

// validate checks a remote name and its fields before persisting.
func validate(name string, rem Remote) error {
	if name == "" || len(name) > 64 {
		return fmt.Errorf("%w: name must be 1-64 bytes", ErrInvalid)
	}
	for _, c := range name {
		if c == '/' || c < 0x21 || c == 0x7f {
			return fmt.Errorf("%w: name %q contains an invalid character", ErrInvalid, name)
		}
	}
	u, err := url.Parse(rem.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%w: URL %q must be http(s)://host[:port][/path]", ErrInvalid, rem.URL)
	}
	if len(rem.ServerKey) == 0 {
		return fmt.Errorf("%w: no pinned server key", ErrInvalid)
	}
	return nil
}

// save writes the map atomically (tmp + rename). Caller holds mu.
func (r *Registry) save() error {
	b, err := json.MarshalIndent(r.remotes, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// Add registers a new remote; ErrExists if the name is taken.
func (r *Registry) Add(name string, rem Remote) error {
	if err := validate(name, rem); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.remotes[name]; ok {
		return fmt.Errorf("%w: %s", ErrExists, name)
	}
	r.remotes[name] = rem
	if err := r.save(); err != nil {
		delete(r.remotes, name)
		return err
	}
	return nil
}

// Remove unregisters name; ErrNotFound if absent.
func (r *Registry) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rem, ok := r.remotes[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	delete(r.remotes, name)
	if err := r.save(); err != nil {
		r.remotes[name] = rem
		return err
	}
	return nil
}

// Get returns the remote registered under name.
func (r *Registry) Get(name string) (Remote, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rem, ok := r.remotes[name]
	return rem, ok
}

// All returns every remote in name order.
func (r *Registry) All() []Named {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Named, 0, len(r.remotes))
	for n, rem := range r.remotes {
		out = append(out, Named{Name: n, Remote: rem})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Resolve returns the remote for name; an empty name selects the sole
// registered remote and errors when there are none or several.
func (r *Registry) Resolve(name string) (string, Remote, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name != "" {
		rem, ok := r.remotes[name]
		if !ok {
			return "", Remote{}, fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		return name, rem, nil
	}
	switch len(r.remotes) {
	case 0:
		return "", Remote{}, errors.New("no remotes registered — run 'amber-store remote add NAME URL' first")
	case 1:
		for n, rem := range r.remotes {
			return n, rem, nil
		}
	}
	return "", Remote{}, errors.New("several remotes registered — name one explicitly")
}
