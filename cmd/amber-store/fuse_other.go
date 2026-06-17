//go:build !linux

package main

import "github.com/urfave/cli/v2"

// fuseCommand returns nil on non-Linux platforms. The FUSE passthrough mount
// relies on Linux-only facilities (memfd_create and FUSE_PASSTHROUGH), so the
// command is not registered at all elsewhere — newApp skips nil commands.
func fuseCommand() *cli.Command { return nil }
