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

// Merge builds a new index from prev (zero key: empty) with upserts applied
// and hashparts in deletes removed, returning the new root. The result is
// identical to building the final path set in one batch.
func Merge(prev key.Key, upserts []PathInfo, deletes []string, get func(key.Key) ([]byte, error), emit fstree.Emit) (key.Key, error) {
	ups, err := upsertEntries(upserts)
	if err != nil {
		return key.Key{}, err
	}
	del := make(map[string]bool, len(deletes))
	for _, h := range deletes {
		del[h] = true
	}

	db := fstree.NewDirBuilder(indexChunker())
	add := func(e fstree.Entry) error {
		if del[string(e.Name)] {
			return nil
		}
		return db.AddEntry(emit, e)
	}

	for old, err := range indexEntries(prev, get) {
		if err != nil {
			return key.Key{}, err
		}
		for len(ups) > 0 && bytes.Compare(ups[0].Name, old.Name) < 0 {
			if err := add(ups[0]); err != nil {
				return key.Key{}, err
			}
			ups = ups[1:]
		}
		if len(ups) > 0 && bytes.Equal(ups[0].Name, old.Name) {
			old = ups[0] // upsert replaces existing record
			ups = ups[1:]
		}
		if err := add(old); err != nil {
			return key.Key{}, err
		}
	}
	for _, e := range ups {
		if err := add(e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
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
