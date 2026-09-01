//go:build linux

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves real blocks for the output file.
//
// Truncate alone does not do this: it sets the file's length and leaves it
// sparse, so the filesystem has to find and allocate blocks during the
// download, on the write path, scattered across the whole file. Reserving the
// space up front moves that work to a single call before any bytes arrive.
func preallocate(file *os.File, size int64) error {
	return unix.Fallocate(int(file.Fd()), 0, 0, size)
}
