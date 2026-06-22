package compress_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/compress"
)

// TestRoundTrip verifies that Compress→Decompress round-trips for every codec
// at payloads of size 0, 1, 100, 1024, and 64 KiB, and that the decompressed
// output exactly matches the input.
func TestRoundTrip(t *testing.T) {
	sizes := []int{0, 1, 100, 1024, 64 * 1024}
	codecs := []compress.Codec{compress.CodecFlate, compress.CodecZlib}

	for _, sz := range sizes {
		payload := make([]byte, sz)
		if sz > 0 {
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}
		}
		for _, c := range codecs {
			c := c
			sz := sz
			t.Run(codecName(c)+"_"+sizeLabel(sz), func(t *testing.T) {
				t.Parallel()
				compressed, err := compress.Compress(c, payload)
				if err != nil {
					t.Fatalf("Compress(%d bytes): %v", sz, err)
				}
				got, err := compress.Decompress(c, compressed)
				if err != nil {
					t.Fatalf("Decompress(%d bytes): %v", sz, err)
				}
				if !bytes.Equal(got, payload) {
					t.Errorf("round-trip mismatch: want %d bytes, got %d bytes", len(payload), len(got))
				}
			})
		}
	}
}

// TestCodecNoneIsNoop verifies CodecNone returns the original slice pointer
// without copying or allocating, and that decompression is also a no-op.
func TestCodecNoneIsNoop(t *testing.T) {
	src := []byte("hello codec none")
	compressed, err := compress.Compress(compress.CodecNone, src)
	if err != nil {
		t.Fatalf("Compress(CodecNone): %v", err)
	}
	if &compressed[0] != &src[0] {
		t.Error("Compress(CodecNone) should return the original slice unchanged")
	}
	decompressed, err := compress.Decompress(compress.CodecNone, src)
	if err != nil {
		t.Fatalf("Decompress(CodecNone): %v", err)
	}
	if &decompressed[0] != &src[0] {
		t.Error("Decompress(CodecNone) should return the original slice unchanged")
	}
}

// TestMultipleRoundTrips ensures 10 payloads per codec all round-trip correctly.
func TestMultipleRoundTrips(t *testing.T) {
	sizes := []int{0, 1, 100, 1024, 64 * 1024, 0, 1, 100, 1024, 64 * 1024}
	codecs := []compress.Codec{compress.CodecFlate, compress.CodecZlib}
	for _, c := range codecs {
		for i, sz := range sizes {
			payload := make([]byte, sz)
			if sz > 0 {
				rand.Read(payload) //nolint:errcheck
			}
			compressed, err := compress.Compress(c, payload)
			if err != nil {
				t.Errorf("codec %d trip %d Compress: %v", c, i, err)
				continue
			}
			got, err := compress.Decompress(c, compressed)
			if err != nil {
				t.Errorf("codec %d trip %d Decompress: %v", c, i, err)
				continue
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("codec %d trip %d mismatch: want len=%d got len=%d", c, i, len(payload), len(got))
			}
		}
	}
}

// BenchmarkFlate measures Compress+Decompress throughput for CodecFlate on a 4 KiB payload.
func BenchmarkFlate(b *testing.B) {
	payload := make([]byte, 4096)
	rand.Read(payload) //nolint:errcheck
	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		c, err := compress.Compress(compress.CodecFlate, payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := compress.Decompress(compress.CodecFlate, c); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZlib measures Compress+Decompress throughput for CodecZlib on a 4 KiB payload.
func BenchmarkZlib(b *testing.B) {
	payload := make([]byte, 4096)
	rand.Read(payload) //nolint:errcheck
	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		c, err := compress.Compress(compress.CodecZlib, payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := compress.Decompress(compress.CodecZlib, c); err != nil {
			b.Fatal(err)
		}
	}
}

func codecName(c compress.Codec) string {
	switch c {
	case compress.CodecFlate:
		return "flate"
	case compress.CodecZlib:
		return "zlib"
	default:
		return "unknown"
	}
}

func sizeLabel(n int) string {
	switch n {
	case 0:
		return "0B"
	case 1:
		return "1B"
	case 100:
		return "100B"
	case 1024:
		return "1KiB"
	case 64 * 1024:
		return "64KiB"
	default:
		return "?B"
	}
}
