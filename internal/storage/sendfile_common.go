package storage

import (
	"io"
	"net"
	"os"
)

// sendfileFallback is the shared read+write path used when the zero-copy
// syscall is unavailable or not applicable (e.g. non-TCP connections on Linux,
// or all connections on non-Linux platforms).
// It reads from f at the given byte offset and writes to dst in 512 KiB chunks.
func sendfileFallback(dst net.Conn, f *os.File, offset int64, length int64) (int64, error) {
	const bufSize = 512 * 1024
	buf := make([]byte, bufSize)

	var total int64
	remaining := length

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}

	for remaining > 0 {
		toRead := int64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}
		nr, err := f.Read(buf[:toRead])
		if nr > 0 {
			nw, werr := dst.Write(buf[:nr])
			total += int64(nw)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
		remaining -= int64(nr)
	}
	return total, nil
}
