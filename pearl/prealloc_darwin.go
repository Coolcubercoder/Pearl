//go:build darwin

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
	store := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	if err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, store); err == nil {
		return nil
	}
	// A single contiguous extent may not be available on a fragmented volume.
	// The same space allocated in pieces is still far better than allocating
	// it lazily on every write.
	store.Flags = unix.F_ALLOCATEALL
	return unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, store)
}
