//go:build unix

package main

import (
	"os"
	"syscall"
	"unsafe"
)

type winsize struct {
	rows, cols, xpixel, ypixel uint16
}

// terminalSize reports the character dimensions of f. ok is true whenever f
// is a terminal, even if it could not report its geometry: some pseudo
// terminals (script, docker exec, CI runners) answer the ioctl with a zero
// window size, and those still want the live display, just at a default
// width. Callers must therefore treat a zero width or height as "unknown"
// rather than as "not a terminal".
func terminalSize(f *os.File) (width, height int, ok bool) {
	var ws winsize
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		f.Fd(),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(&ws)),
	)
	if errno != 0 {
		return 0, 0, false
	}
	return int(ws.cols), int(ws.rows), true
}
