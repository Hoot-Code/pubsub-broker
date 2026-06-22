// Package wal implements a crash-safe Write-Ahead Log.
//
// On-disk format per entry:
//
//	┌────────────┬────────────┬────────────┬────────────────────┐
//	│ Offset (8B)│Length (4B) │CRC32  (4B) │ Data (Length bytes)│
//	└────────────┴────────────┴────────────┴────────────────────┘
//
// Entries are appended sequentially and synced to disk before
// returning to callers. On recovery, the WAL replays entries in order
// and discards any partial/corrupt tail entry.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	headerSize = 16 // 8 (offset) + 4 (length) + 4 (crc32)
)

// Entry represents a single WAL record.
type Entry struct {
	Offset int64
	Data   []byte
}

// WAL is a thread-safe, append-only write-ahead log.
type WAL struct {
	mu         sync.Mutex
	file       *os.File
	nextOffset int64
	path       string
}

// Open opens (or creates) the WAL at the given path and recovers any
// existing entries. The caller receives the recovered entries.
func Open(path string) (*WAL, []Entry, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &WAL{file: f, path: path}
	entries, err := w.recover()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("wal: recover: %w", err)
	}

	return w, entries, nil
}

// Append writes data to the WAL, fsyncs, and returns the assigned offset.
// Safe for concurrent use.
func (w *WAL) Append(data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset := w.nextOffset
	if err := w.writeEntry(offset, data); err != nil {
		return 0, err
	}
	w.nextOffset++
	return offset, nil
}

// NextOffset returns the offset that will be assigned to the next Append.
func (w *WAL) NextOffset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextOffset
}

// Sync flushes WAL data to disk. Append already calls sync internally;
// this is provided for explicit checkpointing.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Truncate atomically replaces all entries in the WAL with the provided
// replacement entries. The file is truncated to zero bytes and then each
// entry in newEntries is appended in order. This is used for checkpointing
// the offset WAL: a single compact snapshot replaces the full history.
func (w *WAL) Truncate(newEntries [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate file: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	w.nextOffset = 0
	for _, data := range newEntries {
		if err := w.writeEntry(w.nextOffset, data); err != nil {
			return fmt.Errorf("wal: rewrite entry %d: %w", w.nextOffset, err)
		}
		w.nextOffset++
	}
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// ─── Internal ────────────────────────────────────────────────────────────────

func (w *WAL) writeEntry(offset int64, data []byte) error {
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(offset))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(data)))
	binary.LittleEndian.PutUint32(hdr[12:16], crc32sum(data))

	// Seek to end before writing (safe with multiple writers if locked).
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}
	if _, err := w.file.Write(hdr); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("wal: write data: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// recover reads all valid entries from the WAL file, truncating any partial
// or corrupt final entry (crash during write). Returns (entries, error).
//
// Truncation always targets the byte position BEFORE the first bad entry,
// not the position after it. This ensures that a corrupt or partially-written
// record is fully removed from disk on recovery rather than left in place.
func (w *WAL) recover() ([]Entry, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []Entry
	var maxOffset int64 = -1
	hdr := make([]byte, headerSize)

	// truncateTo is updated to the file position just after each fully
	// validated entry. On any error it is left pointing to the start of the
	// bad entry, so Truncate removes it entirely.
	var truncateTo int64

	for {
		// Capture the file position BEFORE attempting to read this entry.
		// If the entry turns out to be partial or corrupt, we truncate here.
		entryStart, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		_, err = io.ReadFull(w.file, hdr)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean end-of-file or truncated header — stop here.
			// truncateTo already points to the end of the last good entry
			// (or 0 for an empty file), so no action is needed yet.
			truncateTo = entryStart
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		offset := int64(binary.LittleEndian.Uint64(hdr[0:8]))
		length := binary.LittleEndian.Uint32(hdr[8:12])
		checksum := binary.LittleEndian.Uint32(hdr[12:16])

		data := make([]byte, length)
		_, err = io.ReadFull(w.file, data)
		if err != nil {
			// Truncated data — partial write at crash.
			// Truncate before this entry's header so it is fully removed.
			truncateTo = entryStart
			break
		}

		if crc32sum(data) != checksum {
			// Corrupt entry — truncate before this entry's header.
			truncateTo = entryStart
			break
		}

		entries = append(entries, Entry{Offset: offset, Data: data})
		if offset > maxOffset {
			maxOffset = offset
		}
		// Advance the validated boundary past this complete, correct entry.
		truncateTo = entryStart + int64(headerSize) + int64(length)
	}

	// Truncate file at the last validated boundary, removing any corrupt tail.
	if err := w.file.Truncate(truncateTo); err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}
	// Fsync the truncation immediately. Without this, a second crash
	// right after recover() returns could leave the file's on-disk size at the
	// old (larger) value, causing the same corrupt tail to be replayed again
	// and triggering an endless recovery loop. If the sync fails we must not
	// proceed with a potentially un-fsynced truncation.
	if err := w.file.Sync(); err != nil {
		return nil, fmt.Errorf("truncate sync: %w", err)
	}

	if maxOffset >= 0 {
		w.nextOffset = maxOffset + 1
	}
	return entries, nil
}

func crc32sum(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}
