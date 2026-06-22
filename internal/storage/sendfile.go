//go:build linux

package storage

import (
	"net"
	"os"
	"syscall"
)

// sendfileSegment transfers up to length bytes from f starting at offset
// directly to dst using the Linux sendfile(2) syscall, avoiding any copy
// through userspace buffers.
//
// Returns the number of bytes sent and any error encountered.
func sendfileSegment(dst net.Conn, f *os.File, offset int64, length int64) (int64, error) {
	tc, ok := dst.(*net.TCPConn)
	if !ok {
		return sendfileFallback(dst, f, offset, length)
	}

	rc, err := tc.SyscallConn()
	if err != nil {
		return sendfileFallback(dst, f, offset, length)
	}

	// Obtain the source fd via SyscallConn to avoid the blocking-mode side
	// effect caused by calling (*os.File).Fd() directly.
	var srcFd uintptr
	srcConn, err := f.SyscallConn()
	if err != nil {
		return sendfileFallback(dst, f, offset, length)
	}
	if ctrlErr := srcConn.Control(func(fd uintptr) { srcFd = fd }); ctrlErr != nil {
		return sendfileFallback(dst, f, offset, length)
	}

	var sent int64
	var sendErr error
	ctrlErr := rc.Control(func(dstFd uintptr) {
		remaining := length
		off := offset
		for remaining > 0 {
			n, e := syscall.Sendfile(int(dstFd), int(srcFd), &off, int(remaining))
			if n > 0 {
				sent += int64(n)
				remaining -= int64(n)
			}
			// Retry on EINTR instead of treating it as a fatal error.
			if e == syscall.EINTR {
				continue
			}
			if e != nil {
				sendErr = e
				return
			}
			if n == 0 {
				// EOF on source file.
				return
			}
		}
	})
	if ctrlErr != nil {
		return sent, ctrlErr
	}
	return sent, sendErr
}
