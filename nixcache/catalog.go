package nixcache

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Catalog is the set of hashparts upstream may have. It is the filter that
// keeps the substituter non-proxying: only catalogued misses touch upstream.
type Catalog struct {
	mu  sync.RWMutex
	set []byte // sorted hashPartLen-sized entries, back to back
}

// LoadCatalog reads a catalog saved by Save. A missing file is empty.
func LoadCatalog(path string) (*Catalog, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Catalog{}, nil
	}
	if err != nil {
		return nil, err
	}
	c := &Catalog{}
	if _, err := c.AddList(bytes.NewReader(b)); err != nil {
		return nil, fmt.Errorf("nixcache: catalog %s: %w", path, err)
	}
	return c, nil
}

// AddList merges store paths (or bare hashparts), one per line, and returns
// how many entries the catalog grew by.
func (c *Catalog) AddList(r io.Reader) (int, error) {
	var incoming []byte
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		hp := line
		if len(hp) != hashPartLen {
			hp = HashPart(line)
		}
		if !validHashPart(hp) {
			return 0, fmt.Errorf("nixcache: malformed catalog line %q", line)
		}
		incoming = append(incoming, hp...)
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	merged := mergeSets(c.set, sortSet(incoming))
	grew := (len(merged) - len(c.set)) / hashPartLen
	c.set = merged
	return grew, nil
}

// Merge adds o's entries to c.
func (c *Catalog) Merge(o *Catalog) {
	o.mu.RLock()
	set := bytes.Clone(o.set)
	o.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set = mergeSets(c.set, set)
}

// Contains reports whether hp is catalogued.
func (c *Catalog) Contains(hp string) bool {
	if len(hp) != hashPartLen {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := len(c.set) / hashPartLen
	i := sort.Search(n, func(i int) bool { return c.entry(i) >= hp })
	return i < n && c.entry(i) == hp
}

// Len returns the number of catalogued hashparts.
func (c *Catalog) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.set) / hashPartLen
}

// Save writes the catalog atomically as one hashpart per line.
func (c *Catalog) Save(path string) error {
	c.mu.RLock()
	out := make([]byte, 0, len(c.set)+len(c.set)/hashPartLen)
	for i := 0; i < len(c.set); i += hashPartLen {
		out = append(out, c.set[i:i+hashPartLen]...)
		out = append(out, '\n')
	}
	c.mu.RUnlock()

	tmp, err := os.CreateTemp(filepath.Dir(path), ".catalog-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// All iterates a snapshot of the catalogued hashparts.
func (c *Catalog) All() iter.Seq[string] {
	c.mu.RLock()
	set := bytes.Clone(c.set)
	c.mu.RUnlock()
	return func(yield func(string) bool) {
		for i := 0; i+hashPartLen <= len(set); i += hashPartLen {
			if !yield(string(set[i : i+hashPartLen])) {
				return
			}
		}
	}
}

func (c *Catalog) entry(i int) string {
	return string(c.set[i*hashPartLen : (i+1)*hashPartLen])
}

// sortSet sorts concatenated fixed-size entries and drops duplicates.
func sortSet(set []byte) []byte {
	hps := make([]string, 0, len(set)/hashPartLen)
	for i := 0; i < len(set); i += hashPartLen {
		hps = append(hps, string(set[i:i+hashPartLen]))
	}
	sort.Strings(hps)
	out := make([]byte, 0, len(set))
	for i, hp := range hps {
		if i == 0 || hp != hps[i-1] {
			out = append(out, hp...)
		}
	}
	return out
}

// mergeSets merges two sorted deduplicated sets.
func mergeSets(a, b []byte) []byte {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]byte, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		switch bytes.Compare(a[:hashPartLen], b[:hashPartLen]) {
		case -1:
			out, a = append(out, a[:hashPartLen]...), a[hashPartLen:]
		case 1:
			out, b = append(out, b[:hashPartLen]...), b[hashPartLen:]
		default:
			out, a, b = append(out, a[:hashPartLen]...), a[hashPartLen:], b[hashPartLen:]
		}
	}
	out = append(out, a...)
	return append(out, b...)
}
