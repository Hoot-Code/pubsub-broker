package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Segment is a fixed-size slice of the log for one topic-partition.
// It owns a .log file (records) and a .index file (sparse offset→position table).
type Segment struct {
	mu             sync.RWMutex
	baseOffset     int64
	nextOffset     int64
	logFile        *os.File
	indexFile      *os.File
	logSize        int64
	maxBytes       int64
	indexEvery     int64
	byteSinceIndex int64
	syncPolicy     string
	// version is 1 for legacy segments and 2 for new segments.
	version uint8
	// dataStart is the byte offset in the log file where records begin.
	// 0 for version-1 segments, segFileHeaderSize for version-2 segments.
	dataStart int64
}

// createSegment opens or creates a segment rooted at dir/baseOffset.{log,index}.
// On creation it writes the version-2 file header. On open it auto-detects the
// version from the header and recovers nextOffset by scanning the log.
func createSegment(dir string, baseOffset int64, maxBytes, indexEvery int64, syncPolicy string) (*Segment, error) {
	logPath := filepath.Join(dir, segmentFilename(baseOffset, ".log"))
	idxPath := filepath.Join(dir, segmentFilename(baseOffset, ".index"))

	lf, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("log: %w", err)
	}
	fi, _ := lf.Stat()
	logSize := fi.Size()

	idf, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("index: %w", err)
	}

	s := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		logFile:    lf,
		indexFile:  idf,
		logSize:    logSize,
		maxBytes:   maxBytes,
		indexEvery: indexEvery,
		syncPolicy: syncPolicy,
	}

	if logSize == 0 {
		hdr := [5]byte{segMagic[0], segMagic[1], segMagic[2], segMagic[3], RecordVersion}
		if _, err := lf.Write(hdr[:]); err != nil {
			_ = lf.Close()
			_ = idf.Close()
			return nil, fmt.Errorf("segment header write: %w", err)
		}
		s.version = RecordVersion
		s.logSize = segFileHeaderSize
		s.dataStart = segFileHeaderSize
	} else {
		s.version = 1
		s.dataStart = 0
		if logSize >= segFileHeaderSize {
			rf, openErr := os.Open(lf.Name())
			if openErr == nil {
				var h [5]byte
				if _, readErr := io.ReadFull(rf, h[:]); readErr == nil {
					if h[0] == segMagic[0] && h[1] == segMagic[1] &&
						h[2] == segMagic[2] && h[3] == segMagic[3] &&
						h[4] == RecordVersion {
						s.version = RecordVersion
						s.dataStart = segFileHeaderSize
					}
				}
				rf.Close()
			}
		}
		if err := s.recoverNextOffset(); err != nil {
			_ = lf.Close()
			_ = idf.Close()
			return nil, err
		}
	}
	return s, nil
}

// Write appends a message to the segment, returning its assigned offset.
//
// Ordering invariant: the data record is written to logFile and
// fsynced (unless SyncPolicy == "os") BEFORE the sparse index entry is
// written. This guarantees that a valid index entry always has a corresponding
// durable data record. If the process crashes between the data write and the
// index write, the orphaned data record is harmless — it is re-indexed on
// recovery (recoverNextOffset scans the log). The reverse (an index entry
// pointing at a non-existent record) would corrupt all subsequent reads from
// the segment, so it must never be observable on disk.
func (s *Segment) Write(msg *types.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.nextOffset
	msg.Offset = offset
	rec, err := encodeRecord(offset, msg, s.version)
	if err != nil {
		return 0, err
	}

	// recPos is the byte offset in the log file at which this record will be
	// written. It becomes the value stored in the sparse index entry below.
	recPos := s.logSize

	// 1. Write the data record to the log file first.
	n, err := s.logFile.Write(rec)
	if err != nil {
		return 0, fmt.Errorf("log write: %w", err)
	}
	s.logSize += int64(n)
	s.nextOffset++

	// 2. Fsync the data record unless the user opted into OS-managed flushing
	//    (SyncPolicy == "os"). For "always" and "interval" we fsync here so the
	//    index entry is never durable ahead of the data it references.
	if s.syncPolicy != "os" {
		if err := s.logFile.Sync(); err != nil {
			return 0, fmt.Errorf("log sync: %w", err)
		}
	}

	// 3. Now that the data record is durable, write the sparse index entry.
	//    An orphaned index entry (pointing at a non-existent record) would
	//    corrupt reads, so it must only be written after the data is safe.
	s.byteSinceIndex += int64(len(rec))
	if s.byteSinceIndex >= s.indexEvery {
		idxEntry := make([]byte, 16)
		binary.LittleEndian.PutUint64(idxEntry[0:8], uint64(offset))
		binary.LittleEndian.PutUint64(idxEntry[8:16], uint64(recPos))
		if _, err := s.indexFile.Write(idxEntry); err != nil {
			return 0, fmt.Errorf("index write: %w", err)
		}
		s.byteSinceIndex = 0
		if s.syncPolicy != "os" {
			_ = s.indexFile.Sync()
		}
	}

	return offset, nil
}

// Sync explicitly flushes the segment log to disk.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logFile.Sync()
}

// Read returns messages starting at startOffset, up to maxCount.
func (s *Segment) Read(startOffset int64, maxCount int) ([]*types.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pos, err := s.findPosition(startOffset)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(s.logFile.Name())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	var msgs []*types.Message
	for len(msgs) < maxCount {
		msg, offset, err := decodeRecord(f, s.version)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if offset < startOffset {
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// IsFull reports whether the segment has reached its size limit.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize >= s.maxBytes
}

// BaseOffset returns the first logical offset in the segment.
func (s *Segment) BaseOffset() int64 { return s.baseOffset }

// NextOffset returns the next offset that would be assigned by Write.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// Size returns the current log file size in bytes.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize
}

// Close flushes and closes the segment files.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if err := s.logFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := s.logFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// recoverNextOffset scans the segment log from dataStart to find the highest
// recorded offset, then sets nextOffset = lastOffset + 1.
func (s *Segment) recoverNextOffset() error {
	f, err := os.Open(s.logFile.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	if s.dataStart > 0 {
		if _, err := f.Seek(s.dataStart, io.SeekStart); err != nil {
			return err
		}
	}

	var lastOffset int64 = s.baseOffset - 1
	for {
		_, off, err := decodeRecord(f, s.version)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			break
		}
		lastOffset = off
	}
	s.nextOffset = lastOffset + 1
	return nil
}

// findPosition returns the file byte position to start reading to find
// startOffset. Uses the sparse index for acceleration.
func (s *Segment) findPosition(startOffset int64) (int64, error) {
	data, err := os.ReadFile(s.indexFile.Name())
	if err != nil {
		return s.dataStart, nil
	}

	bestPos := s.dataStart
	for i := 0; i+16 <= len(data); i += 16 {
		idxOff := int64(binary.LittleEndian.Uint64(data[i : i+8]))
		filePos := int64(binary.LittleEndian.Uint64(data[i+8 : i+16]))
		if idxOff <= startOffset {
			bestPos = filePos
		} else {
			break
		}
	}
	return bestPos, nil
}

// findExactPosition returns the file byte position of the first record whose
// logical offset is >= startOffset, starting the scan from the sparse-index
// approximation. Falls back to dataStart on error.
func (s *Segment) findExactPosition(startOffset, dataStart int64) int64 {
	approx, _ := s.findPosition(startOffset)

	f, err := os.Open(s.logFile.Name())
	if err != nil {
		return approx
	}
	defer f.Close()

	if _, err := f.Seek(approx, io.SeekStart); err != nil {
		return approx
	}

	var hdrSize int64
	if s.version >= RecordVersion {
		hdrSize = recV2HeaderSize
	} else {
		hdrSize = recV1HeaderSize
	}

	pos := approx
	hdr := make([]byte, hdrSize)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		recOff := int64(binary.LittleEndian.Uint64(hdr[0:8]))
		keyLen := int64(binary.LittleEndian.Uint16(hdr[16:18]))
		payloadLen := int64(binary.LittleEndian.Uint32(hdr[18:22]))
		if recOff >= startOffset {
			return pos
		}
		skip := keyLen + payloadLen
		pos += hdrSize + skip
		if _, err := f.Seek(skip, io.SeekCurrent); err != nil {
			break
		}
	}
	return pos
}

// segmentFilename returns the log or index filename for a given base offset.
// Format: %020d + ext (e.g. "00000000000000000000.log").
func segmentFilename(baseOffset int64, ext string) string {
	return fmt.Sprintf("%020d%s", baseOffset, ext)
}
