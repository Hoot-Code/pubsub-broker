//go:build !linux

package storage

import (
	"net"
	"os"
)

// sendfileSegment transfers up to length bytes from f starting at offset to
// dst using a 512 KiB userspace copy buffer.  On non-Linux platforms there is
// no sendfile(2) syscall; this fallback ensures the build succeeds everywhere.
//
// Returns the number of bytes sent and any error encountered.
func sendfileSegment(dst net.Conn, f *os.File, offset int64, length int64) (int64, error) {
	return sendfileFallback(dst, f, offset, length)
}
