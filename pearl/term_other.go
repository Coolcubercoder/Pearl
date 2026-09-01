//go:build !unix

package main

import "os"

// terminalSize has no portable implementation off unix, so the display falls
// back to its default geometry.
func terminalSize(f *os.File) (width, height int, ok bool) {
	return 0, 0, false
}
