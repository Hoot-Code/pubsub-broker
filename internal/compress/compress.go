// Package compress provides payload compression using stdlib codec only.
// Supported codecs: raw DEFLATE (compress/flate) and zlib (compress/zlib).
package compress

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"fmt"
	"io"
)

// Codec identifies the compression algorithm applied to a payload.
type Codec uint8

const (
	// CodecNone means no compression; payload is stored verbatim.
	CodecNone Codec = 0
	// CodecFlate uses raw DEFLATE (compress/flate) at BestSpeed level.
	CodecFlate Codec = 1
	// CodecZlib uses zlib-wrapped DEFLATE (compress/zlib) at BestSpeed level.
	CodecZlib Codec = 2
)

// Compress compresses src using the specified codec and returns the result.
// CodecNone returns src unchanged (no allocation). Both CodecFlate and
// CodecZlib are safe for concurrent use — each call allocates its own buffers.
func Compress(codec Codec, src []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return src, nil
	case CodecFlate:
		return compressFlate(src)
	case CodecZlib:
		return compressZlib(src)
	default:
		return nil, fmt.Errorf("compress: unknown codec %d", codec)
	}
}

// Decompress decompresses src using the specified codec and returns the result.
// CodecNone returns src unchanged (no allocation). Both CodecFlate and
// CodecZlib are safe for concurrent use — each call allocates its own buffers.
func Decompress(codec Codec, src []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return src, nil
	case CodecFlate:
		return decompressFlate(src)
	case CodecZlib:
		return decompressZlib(src)
	default:
		return nil, fmt.Errorf("compress: unknown codec %d", codec)
	}
}

func compressFlate(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(src))
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("compress: flate writer: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("compress: flate write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compress: flate close: %w", err)
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func decompressFlate(src []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("compress: flate decompress: %w", err)
	}
	return out, nil
}

func compressZlib(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(src))
	w, err := zlib.NewWriterLevel(&buf, zlib.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("compress: zlib writer: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("compress: zlib write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compress: zlib close: %w", err)
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func decompressZlib(src []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("compress: zlib reader: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("compress: zlib decompress: %w", err)
	}
	return out, nil
}
