//go:build !darwin && !linux

package main

import "os"

// preallocate has no portable equivalent here. Truncate still sets the file
// size and the download still works; blocks are just allocated lazily on the
// write path, which is the behaviour everywhere before this existed.
func preallocate(file *os.File, size int64) error { return nil }
