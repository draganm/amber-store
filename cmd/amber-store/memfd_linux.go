//go:build linux

package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// newMemfd creates an anonymous, RAM-backed file (memfd_create(2)), fills it
// with exactly size bytes read from r, and returns it positioned at offset 0.
// The returned *os.File owns the descriptor; the caller must Close it. Keeping
// the *os.File (rather than a bare fd) alive prevents the runtime finalizer from
// closing the descriptor while the kernel still uses it as a passthrough backing
// file.
func newMemfd(name string, size int64, r io.Reader) (*os.File, error) {
	fd, err := unix.MemfdCreate("amber:"+name, unix.MFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	f := os.NewFile(uintptr(fd), "amber:"+name)
	if size > 0 {
		// Fail fast (e.g. on ENOMEM) before streaming the whole body in.
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("sizing memfd to %d bytes: %w", size, err)
		}
	}
	n, err := io.Copy(f, r)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("filling memfd: %w", err)
	}
	if n != size {
		f.Close()
		return nil, fmt.Errorf("short content: got %d bytes, want %d", n, size)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("rewinding memfd: %w", err)
	}
	return f, nil
}
