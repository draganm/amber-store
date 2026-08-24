// closureDir is <store>/closures: one <root-hex>.tails per root named by a
// reference; tmp/ stages writes and is swept at open.

package gc

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/draganm/amber-store/key"
)

const tailsSuffix = ".tails"

type closureDir struct {
	path string
	f    *os.File // for directory fsyncs
	sync bool
}

func openClosureDir(path string, sync bool) (*closureDir, error) {
	tmp := filepath.Join(path, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	// Sweep staging: anything here is a write that never committed.
	ents, err := os.ReadDir(tmp)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if err := os.RemoveAll(filepath.Join(tmp, e.Name())); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &closureDir{path: path, f: f, sync: sync}, nil
}

func (d *closureDir) file(root key.Key) string {
	return filepath.Join(d.path, root.String()+tailsSuffix)
}

// write stages the closure in tmp/, fsyncs, renames it into place and
// fsyncs the directory — the seal pattern.
func (d *closureDir) write(root key.Key, tails []uint64) (err error) {
	f, err := os.CreateTemp(filepath.Join(d.path, "tmp"), root.String()+".*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() {
		if err != nil {
			os.Remove(name)
		}
	}()
	if _, err = f.Write(encodeClosure(root, tails)); err != nil {
		f.Close()
		return err
	}
	if d.sync {
		if err = f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, d.file(root)); err != nil {
		return err
	}
	if d.sync {
		err = d.f.Sync()
	}
	return err
}

// read returns root's tails; ok is false when the file is missing or fails
// validation — an invalid closure counts as absent.
func (d *closureDir) read(root key.Key) ([]uint64, bool) {
	b, err := os.ReadFile(d.file(root))
	if err != nil {
		return nil, false
	}
	tails, err := decodeClosure(root, b)
	if err != nil {
		return nil, false
	}
	return tails, true
}

// remove deletes root's closure file; a missing file is fine.
func (d *closureDir) remove(root key.Key) error {
	if err := os.Remove(d.file(root)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if d.sync {
		return d.f.Sync()
	}
	return nil
}

// list returns every root with a closure file, valid or not. Foreign files
// are ignored, as the packstore ignores non-segment files.
func (d *closureDir) list() ([]key.Key, error) {
	ents, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var roots []key.Key
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), tailsSuffix)
		if !ok || e.IsDir() {
			continue
		}
		raw, err := hex.DecodeString(name)
		if err != nil || len(raw) != key.Size {
			continue
		}
		k, err := key.Parse(raw)
		if err != nil {
			continue
		}
		roots = append(roots, k)
	}
	return roots, nil
}

// wipe removes every closure file.
func (d *closureDir) wipe() error {
	roots, err := d.list()
	if err != nil {
		return err
	}
	for _, root := range roots {
		if err := d.remove(root); err != nil {
			return err
		}
	}
	return nil
}

func (d *closureDir) close() error {
	return d.f.Close()
}
