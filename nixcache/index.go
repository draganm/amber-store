// Package nixcache implements the cache index: a prolly-tree map from store
// path hashpart to PathInfo, stored as symlink-style directory entries whose
// LinkTarget holds the canonical-CBOR record. No new CAS types.
package nixcache

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/draganm/amber-store/chunkers"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/fxamacker/cbor/v2"
)

const (
	storeDir     = "/nix/store/"
	hashPartLen  = 32
	nixBase32    = "0123456789abcdfghijklmnpqrsvwxyz"
	linkMode     = 0o120777
	listPageSize = 1024
)

// PathInfo is one indexed store path.
type PathInfo struct {
	StorePath           string
	RootKey             key.Key
	NarHash             [32]byte
	NarSize             uint64
	References          []string
	Deriver             string
	Sigs                []string
	IngestedAt          int64
	AgedAt              int64 // when the path left the catalog; 0: still listed
	UpstreamCompression string
}

// indexChunker sets index tree fanout: 2^7 entries per leaf on average.
func indexChunker() chunkers.ItemChunker { return chunkers.NewItemChunker(7) }

type record struct {
	StorePath   string   `cbor:"0,keyasint"`
	RootKey     []byte   `cbor:"1,keyasint"`
	NarHash     []byte   `cbor:"2,keyasint"`
	NarSize     uint64   `cbor:"3,keyasint"`
	References  []string `cbor:"4,keyasint,omitempty"`
	Deriver     string   `cbor:"5,keyasint,omitempty"`
	Sigs        []string `cbor:"6,keyasint,omitempty"`
	IngestedAt  int64    `cbor:"7,keyasint"`
	Compression string   `cbor:"8,keyasint,omitempty"`
	AgedAt      int64    `cbor:"9,keyasint,omitempty"`
}

var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.Time = cbor.TimeUnix
	m, err := opts.EncMode()
	if err != nil {
		panic(err)
	}
	encMode = m
}

// HashPart returns the 32-char nix-base32 hash part of a full store path, or
// "" if the path is not well-formed.
func HashPart(storePath string) string {
	rest, ok := strings.CutPrefix(storePath, storeDir)
	if !ok || len(rest) < hashPartLen+2 || rest[hashPartLen] != '-' {
		return ""
	}
	if hp := rest[:hashPartLen]; validHashPart(hp) {
		return hp
	}
	return ""
}

func (p *PathInfo) validate() error {
	if HashPart(p.StorePath) == "" {
		return fmt.Errorf("nixcache: malformed store path %q", p.StorePath)
	}
	if p.RootKey == (key.Key{}) {
		return errors.New("nixcache: zero root key")
	}
	if err := p.RootKey.Validate(); err != nil {
		return fmt.Errorf("nixcache: root key: %w", err)
	}
	return nil
}

// EncodeRecord serializes p as canonical CBOR.
func EncodeRecord(p PathInfo) ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	refs := slices.Sorted(slices.Values(p.References))
	return encMode.Marshal(record{
		StorePath:   p.StorePath,
		RootKey:     p.RootKey[:],
		NarHash:     p.NarHash[:],
		NarSize:     p.NarSize,
		References:  refs,
		Deriver:     p.Deriver,
		Sigs:        p.Sigs,
		IngestedAt:  p.IngestedAt,
		AgedAt:      p.AgedAt,
		Compression: p.UpstreamCompression,
	})
}

// DecodeRecord parses an index record.
func DecodeRecord(b []byte) (PathInfo, error) {
	var r record
	if err := cbor.Unmarshal(b, &r); err != nil {
		return PathInfo{}, fmt.Errorf("nixcache: decoding record: %w", err)
	}
	rk, err := key.Parse(r.RootKey)
	if err != nil {
		return PathInfo{}, fmt.Errorf("nixcache: record root key: %w", err)
	}
	if len(r.NarHash) != 32 {
		return PathInfo{}, fmt.Errorf("nixcache: narHash length %d", len(r.NarHash))
	}
	p := PathInfo{
		StorePath:           r.StorePath,
		RootKey:             rk,
		NarSize:             r.NarSize,
		References:          r.References,
		Deriver:             r.Deriver,
		Sigs:                r.Sigs,
		IngestedAt:          r.IngestedAt,
		AgedAt:              r.AgedAt,
		UpstreamCompression: r.Compression,
	}
	copy(p.NarHash[:], r.NarHash)
	if err := p.validate(); err != nil {
		return PathInfo{}, err
	}
	return p, nil
}

// Lookup returns the PathInfo indexed under hashpart. A missing entry (or
// the zero key's empty index) wraps fstree.ErrNotFound.
func Lookup(root key.Key, hashpart string, get func(key.Key) ([]byte, error)) (PathInfo, error) {
	if root == (key.Key{}) {
		return PathInfo{}, fmt.Errorf("nixcache: empty index: %w", fstree.ErrNotFound)
	}
	e, err := fstree.LookupEntry(root, []byte(hashpart), get)
	if err != nil {
		return PathInfo{}, err
	}
	return DecodeRecord(e.LinkTarget)
}

// Merge builds a new index from prev (zero key: empty) with upserts
// applied and deletes removed. Untouched leaves are reused by key, so a
// small change costs O(changed leaves).
func Merge(prev key.Key, upserts []PathInfo, deletes []string, get func(key.Key) ([]byte, error), emit fstree.Emit) (key.Key, error) {
	m, err := newMerger(upserts, deletes, emit)
	if err != nil {
		return key.Key{}, err
	}
	for leaf, err := range indexLeaves(prev, get) {
		if err != nil {
			return key.Key{}, err
		}
		if err := m.mergeLeaf(leaf, get); err != nil {
			return key.Key{}, err
		}
	}
	return m.finish()
}

type merger struct {
	db      *fstree.DirBuilder
	emit    fstree.Emit
	ups     []fstree.Entry
	del     map[string]bool
	changes [][]byte // pending changed names, sorted
}

func newMerger(upserts []PathInfo, deletes []string, emit fstree.Emit) (*merger, error) {
	ups, err := upsertEntries(upserts)
	if err != nil {
		return nil, err
	}
	del := make(map[string]bool, len(deletes))
	changes := make([][]byte, 0, len(ups)+len(deletes))
	for _, e := range ups {
		changes = append(changes, e.Name)
	}
	for _, h := range deletes {
		del[h] = true
		changes = append(changes, []byte(h))
	}
	slices.SortFunc(changes, bytes.Compare)
	db := fstree.NewDirBuilder(indexChunker())
	return &merger{db: db, emit: emit, ups: ups, del: del, changes: changes}, nil
}

// mergeLeaf reuses leaf verbatim when no pending change falls in its
// range and the builder sits on a boundary. The final leaf never closed
// on one, so it is always rebuilt.
func (m *merger) mergeLeaf(leaf leafRef, get func(key.Key) ([]byte, error)) error {
	touched := len(m.changes) > 0 && bytes.Compare(m.changes[0], leaf.sep) <= 0
	if !leaf.last && !touched && m.db.Aligned() {
		return m.db.AddSealedLeaf(m.emit, leaf.key, leaf.sep)
	}
	data, err := get(leaf.key)
	if err != nil {
		return err
	}
	entries, err := fstree.DecodeDirLeaf(data)
	if err != nil {
		return err
	}
	for _, old := range entries {
		if err := m.mergeOld(old); err != nil {
			return err
		}
	}
	if !leaf.last {
		for len(m.changes) > 0 && bytes.Compare(m.changes[0], leaf.sep) <= 0 {
			m.changes = m.changes[1:]
		}
	}
	return nil
}

func (m *merger) mergeOld(old fstree.Entry) error {
	for len(m.ups) > 0 && bytes.Compare(m.ups[0].Name, old.Name) < 0 {
		if err := m.add(m.ups[0]); err != nil {
			return err
		}
		m.ups = m.ups[1:]
	}
	if len(m.ups) > 0 && bytes.Equal(m.ups[0].Name, old.Name) {
		old = m.ups[0] // upsert replaces existing record
		m.ups = m.ups[1:]
	}
	return m.add(old)
}

func (m *merger) add(e fstree.Entry) error {
	if m.del[string(e.Name)] {
		return nil
	}
	return m.db.AddEntry(m.emit, e)
}

func (m *merger) finish() (key.Key, error) {
	for _, e := range m.ups {
		if err := m.add(e); err != nil {
			return key.Key{}, err
		}
	}
	return m.db.Finish(m.emit)
}

type leafRef struct {
	key  key.Key
	sep  []byte // greatest entry name, nil for the final leaf
	last bool
}

// indexLeaves yields prev's DirLeaf chunks in order, undecoded.
func indexLeaves(prev key.Key, get func(key.Key) ([]byte, error)) iter.Seq2[leafRef, error] {
	return func(yield func(leafRef, error) bool) {
		if prev == (key.Key{}) {
			return
		}
		walkLeaves(prev, nil, true, get, yield)
	}
}

func walkLeaves(k key.Key, sep []byte, last bool, get func(key.Key) ([]byte, error), yield func(leafRef, error) bool) bool {
	if k.Type() == key.DirLeaf {
		if last {
			sep = nil
		}
		return yield(leafRef{key: k, sep: sep, last: last}, nil)
	}
	data, err := get(k)
	if err != nil {
		return yield(leafRef{}, err)
	}
	pairs, err := fstree.DecodeDirNode(data)
	if err != nil {
		return yield(leafRef{}, err)
	}
	for i, p := range pairs {
		ck, err := key.Parse(p.ChildKey)
		if err != nil {
			return yield(leafRef{}, err)
		}
		if !walkLeaves(ck, p.SepName, last && i == len(pairs)-1, get, yield) {
			return false
		}
	}
	return true
}

// upsertEntries encodes upserts as index entries, sorted by hashpart.
func upsertEntries(upserts []PathInfo) ([]fstree.Entry, error) {
	ups := make([]fstree.Entry, 0, len(upserts))
	for _, p := range upserts {
		rec, err := EncodeRecord(p)
		if err != nil {
			return nil, err
		}
		ups = append(ups, fstree.Entry{
			Name:       []byte(HashPart(p.StorePath)),
			Mode:       linkMode,
			LinkTarget: rec,
		})
	}
	slices.SortFunc(ups, func(a, b fstree.Entry) int { return bytes.Compare(a.Name, b.Name) })
	for i := 1; i < len(ups); i++ {
		if bytes.Equal(ups[i].Name, ups[i-1].Name) {
			return nil, fmt.Errorf("nixcache: duplicate upsert %s", ups[i].Name)
		}
	}
	return ups, nil
}

// indexEntries yields the entries of the index at root in name order. The
// zero key yields nothing.
func indexEntries(root key.Key, get func(key.Key) ([]byte, error)) iter.Seq2[fstree.Entry, error] {
	return func(yield func(fstree.Entry, error) bool) {
		if root == (key.Key{}) {
			return
		}
		var after []byte
		for {
			page, more, err := fstree.ListEntries(root, after, listPageSize, get)
			if err != nil {
				yield(fstree.Entry{}, err)
				return
			}
			for _, e := range page {
				if !yield(e, nil) {
					return
				}
				after = e.Name
			}
			if !more {
				return
			}
		}
	}
}
