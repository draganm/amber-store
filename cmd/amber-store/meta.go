package main

import (
	"io/fs"
	"syscall"

	"golang.org/x/sys/unix"
)

// meta is the POSIX metadata pulled from an lstat result.
type meta struct {
	Mode  uint64 // raw st_mode (type + perms)
	UID   uint64
	GID   uint64
	Mtime int64 // ns since the Unix epoch
}

// entryMeta extracts the raw POSIX metadata from an lstat FileInfo.
func entryMeta(info fs.FileInfo) meta {
	sys := info.Sys().(*syscall.Stat_t)
	return meta{
		Mode:  uint64(sys.Mode),
		UID:   uint64(sys.Uid),
		GID:   uint64(sys.Gid),
		Mtime: info.ModTime().UnixNano(),
	}
}

// deviceNumbers returns the major/minor device numbers for a device-node
// lstat FileInfo.
func deviceNumbers(info fs.FileInfo) (uint64, uint64) {
	sys := info.Sys().(*syscall.Stat_t)
	rdev := uint64(sys.Rdev)
	return uint64(unix.Major(rdev)), uint64(unix.Minor(rdev))
}
