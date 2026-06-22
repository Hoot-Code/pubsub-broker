package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/compress"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Record on-disk format constants ─────────────────────────────────────────
//
// Version 1: [ offset(8) | ts(8) | keyLen(2) | payloadLen(4) | crc(4) | key | payload ]
// Version 2: same but codec(1) appears after crc, before key.

const (
	recV1HeaderSize = 8 + 8 + 2 + 4 + 4     // 26 bytes
	recV2HeaderSize = 8 + 8 + 2 + 4 + 4 + 1 // 27 bytes (adds codec byte)
	// RecordVersion is the current record version written by new segments.
	RecordVersion = uint8(2)
)

// ─── Segment file header ─────────────────────────────────────────────────────

// segMagic is the 4-byte magic written at the start of version-2 segment files.
var segMagic = [4]byte{0x50, 0x53, 0x47, 0x32} // "PSG2"

const segFileHeaderSize = int64(5) // magic(4) + version(1)

// encodeRecord serialises a message into a log record byte slice.
// For version 2 the codec byte is included after the CRC field.
func encodeRecord(offset int64, msg *types.Message, version uint8) ([]byte, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	key := []byte(msg.Key)

	var rec []byte
	if version >= RecordVersion {
		hdr := make([]byte, recV2HeaderSize)
		binary.LittleEndian.PutUint64(hdr[0:8], uint64(offset))
		binary.LittleEndian.PutUint64(hdr[8:16], uint64(time.Now().UnixNano()))
		binary.LittleEndian.PutUint16(hdr[16:18], uint16(len(key)))
		binary.LittleEndian.PutUint32(hdr[18:22], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[22:26], crc32.ChecksumIEEE(payload))
		hdr[26] = msg.Codec
		rec = make([]byte, recV2HeaderSize+len(key)+len(payload))
		copy(rec, hdr)
		copy(rec[recV2HeaderSize:], key)
		copy(rec[recV2HeaderSize+len(key):], payload)
	} else {
		hdr := make([]byte, recV1HeaderSize)
		binary.LittleEndian.PutUint64(hdr[0:8], uint64(offset))
		binary.LittleEndian.PutUint64(hdr[8:16], uint64(time.Now().UnixNano()))
		binary.LittleEndian.PutUint16(hdr[16:18], uint16(len(key)))
		binary.LittleEndian.PutUint32(hdr[18:22], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[22:26], crc32.ChecksumIEEE(payload))
		rec = make([]byte, recV1HeaderSize+len(key)+len(payload))
		copy(rec, hdr)
		copy(rec[recV1HeaderSize:], key)
		copy(rec[recV1HeaderSize+len(key):], payload)
	}
	return rec, nil
}

// decodeRecord reads one record from r according to the segment version.
// For version 2 it reads the codec byte and decompresses the payload if needed.
func decodeRecord(r io.Reader, version uint8) (*types.Message, int64, error) {
	var codec uint8
	var keyLen, payloadLen int
	var checksum uint32
	var offset int64

	if version >= RecordVersion {
		hdr := make([]byte, recV2HeaderSize)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil, 0, err
		}
		offset = int64(binary.LittleEndian.Uint64(hdr[0:8]))
		keyLen = int(binary.LittleEndian.Uint16(hdr[16:18]))
		payloadLen = int(binary.LittleEndian.Uint32(hdr[18:22]))
		checksum = binary.LittleEndian.Uint32(hdr[22:26])
		codec = hdr[26]
	} else {
		hdr := make([]byte, recV1HeaderSize)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil, 0, err
		}
		offset = int64(binary.LittleEndian.Uint64(hdr[0:8]))
		keyLen = int(binary.LittleEndian.Uint16(hdr[16:18]))
		payloadLen = int(binary.LittleEndian.Uint32(hdr[18:22]))
		checksum = binary.LittleEndian.Uint32(hdr[22:26])
	}

	body := make([]byte, keyLen+payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, 0, err
	}
	payload := body[keyLen:]
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, 0, fmt.Errorf("checksum mismatch at offset %d", offset)
	}

	if codec != 0 {
		var err error
		payload, err = compress.Decompress(compress.Codec(codec), payload)
		if err != nil {
			return nil, 0, fmt.Errorf("decompress at offset %d: %w", offset, err)
		}
	}

	var msg types.Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, 0, err
	}
	msg.Offset = offset
	return &msg, offset, nil
}
