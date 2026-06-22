package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/compress"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// defaultCompactionInterval is used by NewKeyCompactor when interval <= 0.
const defaultCompactionInterval = 60 * time.Second

// defaultTombstoneGrace is used by CompactPartition when the configured
// grace period is <= 0.
const defaultTombstoneGrace = 24 * time.Hour

// tombstoneHeaderKey is the message header set by producers (via
// pkg/client's Producer.Tombstone) to mark a key as logically deleted.
const tombstoneHeaderKey = "_compaction"

// tombstoneHeaderValue is the header value that marks a tombstone record.
const tombstoneHeaderValue = "delete"

// PartitionSource is implemented by anything that can enumerate the
// PartitionLogs belonging to topics configured for key-based compaction
// (TopicConfig.CompactionMode == "compact"). *topic.Manager satisfies this
// interface; it is expressed here as an interface (rather than importing
// package topic directly) to avoid an import cycle, since package topic
// already imports package storage.
type PartitionSource interface {
	// CompactablePartitions returns every PartitionLog whose owning topic is
	// configured with CompactionMode == "compact".
	CompactablePartitions() []*PartitionLog
}

// KeyCompactor periodically rewrites the non-active segments of compacted
// PartitionLogs, retaining only the latest record per key (Kafka-style
// log compaction). Messages published with an empty key are never removed.
type KeyCompactor struct {
	mu           sync.RWMutex
	interval     time.Duration
	tombstoneTTL time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewKeyCompactor creates a KeyCompactor that sweeps every interval.
// interval <= 0 defaults to 60 seconds.
func NewKeyCompactor(interval time.Duration) *KeyCompactor {
	if interval <= 0 {
		interval = defaultCompactionInterval
	}
	return &KeyCompactor{
		interval:     interval,
		tombstoneTTL: defaultTombstoneGrace,
	}
}

// UpdateConfig applies new compaction timing parameters. The new interval
// takes effect on the NEXT scheduled tick (the running ticker is replaced);
// the new tombstone grace period takes effect on the next sweep. This method
// is safe for concurrent use.
func (kc *KeyCompactor) UpdateConfig(interval time.Duration, graceMs int64) {
	if interval <= 0 {
		interval = defaultCompactionInterval
	}
	if graceMs < 0 {
		graceMs = 0
	}
	kc.mu.Lock()
	kc.interval = interval
	kc.tombstoneTTL = time.Duration(graceMs) * time.Millisecond
	kc.mu.Unlock()
}

// SetTombstoneGrace overrides the default tombstone grace period (24 h).
// A zero value means tombstones are eligible for removal on the very next
// sweep (no grace period); negative values are treated as zero.
func (kc *KeyCompactor) SetTombstoneGrace(d time.Duration) {
	if d < 0 {
		d = 0
	}
	kc.mu.Lock()
	kc.tombstoneTTL = d
	kc.mu.Unlock()
}

// Start launches the compaction sweep loop in a background goroutine. It
// queries topics for compactable PartitionLogs on every tick and compacts
// each one in turn. The loop stops when ctx is cancelled.
func (kc *KeyCompactor) Start(ctx context.Context, topics PartitionSource) {
	kc.stopCh = make(chan struct{})
	kc.doneCh = make(chan struct{})
	kc.mu.RLock()
	interval := kc.interval
	kc.mu.RUnlock()
	go func() {
		defer close(kc.doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-kc.stopCh:
				return
			case <-ticker.C:
				for _, pl := range topics.CompactablePartitions() {
					_, _ = kc.CompactPartition(pl)
				}
			}
		}
	}()
}

// Stop halts the compaction loop started by Start and waits for it to exit.
// Safe to call even if Start was never called.
func (kc *KeyCompactor) Stop() {
	if kc.stopCh == nil {
		return
	}
	select {
	case <-kc.stopCh:
	default:
		close(kc.stopCh)
	}
	if kc.doneCh != nil {
		<-kc.doneCh
	}
}

// compactRecord is a single record read off disk during a compaction sweep,
// retaining its exact raw bytes so that rewriting never re-encodes (and thus
// never changes) the original timestamp, codec, or CRC.
type compactRecord struct {
	offset    int64
	key       string
	timestamp int64
	tombstone bool
	raw       []byte // full on-disk record: header + key + payload
}

// CompactPartition runs one compaction sweep over pl's non-active segments.
// It returns the number of records removed. The active (currently-written)
// segment is never inspected or modified.
func (kc *KeyCompactor) CompactPartition(pl *PartitionLog) (removed int, err error) {
	pl.mu.RLock()
	if len(pl.segments) <= 1 {
		pl.mu.RUnlock()
		return 0, nil
	}
	targets := make([]*Segment, len(pl.segments)-1)
	copy(targets, pl.segments[:len(pl.segments)-1])
	pl.mu.RUnlock()

	kc.mu.RLock()
	grace := kc.tombstoneTTL
	kc.mu.RUnlock()
	if grace < 0 {
		grace = 0
	}

	// Pass 1: scan every target segment, oldest to newest, building the
	// highest-offset-per-key map. Empty keys are excluded — they are
	// always retained and never participate in the dedup decision.
	keyLatest := make(map[string]int64)
	allRecords := make([][]compactRecord, len(targets))
	for i, seg := range targets {
		recs, scanErr := scanSegment(seg)
		if scanErr != nil {
			return removed, fmt.Errorf("storage: compactor scan segment %d: %w", seg.BaseOffset(), scanErr)
		}
		allRecords[i] = recs
		for _, r := range recs {
			if r.key == "" {
				continue
			}
			keyLatest[r.key] = r.offset
		}
	}

	now := time.Now().UnixNano()
	graceNs := grace.Nanoseconds()

	for i, seg := range targets {
		var kept [][]byte
		droppedHere := 0
		for _, r := range allRecords[i] {
			keep := true
			switch {
			case r.key == "":
				keep = true
			case r.offset != keyLatest[r.key]:
				keep = false
			case r.tombstone:
				keep = now-r.timestamp < graceNs
			default:
				keep = true
			}
			if keep {
				kept = append(kept, r.raw)
			} else {
				droppedHere++
			}
		}
		if droppedHere == 0 {
			continue
		}
		if err := rewriteSegment(seg, kept); err != nil {
			return removed, fmt.Errorf("storage: compactor rewrite segment %d: %w", seg.BaseOffset(), err)
		}
		removed += droppedHere
	}
	return removed, nil
}

// scanSegment reads every record in seg (skipping the file header) and
// returns them as compactRecords, preserving the exact raw on-disk bytes.
func scanSegment(seg *Segment) ([]compactRecord, error) {
	seg.mu.RLock()
	name := seg.logFile.Name()
	dataStart := seg.dataStart
	version := seg.version
	seg.mu.RUnlock()

	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	var recs []compactRecord
	for {
		raw, offset, ts, key, tombstone, err := readRawRecord(f, version)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		recs = append(recs, compactRecord{
			offset:    offset,
			key:       key,
			timestamp: ts,
			tombstone: tombstone,
			raw:       raw,
		})
	}
	return recs, nil
}

// readRawRecord reads one record from r according to the segment record
// version, returning the exact raw bytes (header+key+payload) alongside the
// decoded offset, timestamp, key, and whether the record carries the
// tombstone header. It mirrors decodeRecord's header parsing but additionally
// preserves the raw bytes so the record can be rewritten byte-for-byte.
func readRawRecord(r io.Reader, version uint8) (raw []byte, offset, ts int64, key string, tombstone bool, err error) {
	hdrSize := recV1HeaderSize
	if version >= RecordVersion {
		hdrSize = recV2HeaderSize
	}
	hdr := make([]byte, hdrSize)
	if _, err = io.ReadFull(r, hdr); err != nil {
		return nil, 0, 0, "", false, err
	}

	offset = int64(binary.LittleEndian.Uint64(hdr[0:8]))
	ts = int64(binary.LittleEndian.Uint64(hdr[8:16]))
	keyLen := int(binary.LittleEndian.Uint16(hdr[16:18]))
	payloadLen := int(binary.LittleEndian.Uint32(hdr[18:22]))
	checksum := binary.LittleEndian.Uint32(hdr[22:26])
	var codec uint8
	if version >= RecordVersion {
		codec = hdr[26]
	}

	body := make([]byte, keyLen+payloadLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return nil, 0, 0, "", false, err
	}
	key = string(body[:keyLen])
	payload := body[keyLen:]
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, 0, 0, "", false, fmt.Errorf("checksum mismatch at offset %d", offset)
	}

	decoded := payload
	if codec != 0 {
		decoded, err = compress.Decompress(compress.Codec(codec), payload)
		if err != nil {
			return nil, 0, 0, "", false, fmt.Errorf("decompress at offset %d: %w", offset, err)
		}
	}
	var msg types.Message
	if err = json.Unmarshal(decoded, &msg); err != nil {
		return nil, 0, 0, "", false, err
	}
	tombstone = msg.Headers != nil && msg.Headers[tombstoneHeaderKey] == tombstoneHeaderValue

	raw = make([]byte, 0, len(hdr)+len(body))
	raw = append(raw, hdr...)
	raw = append(raw, body...)
	return raw, offset, ts, key, tombstone, nil
}

// rewriteSegment replaces seg's on-disk log and index files with one
// containing only the given raw records (already filtered by the caller),
// in their original order. It writes to temp files, fsyncs them, atomically
// renames them over the originals (os.Rename), then reopens the segment's
// file handles so subsequent reads observe the new content.
//
// If the log rename succeeds but the index rename fails, the segment is
// left in a degraded but usable state: the log contains the new (compacted)
// content, but the sparse index is stale. The segment remains readable via
// full-scan fallback but the index should be considered untrusted until
// the next successful compaction or a manual reindex.
func rewriteSegment(seg *Segment, keptRaw [][]byte) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	logPath := seg.logFile.Name()
	idxPath := seg.indexFile.Name()
	tmpLogPath := logPath + ".compact.tmp"
	tmpIdxPath := idxPath + ".compact.tmp"

	tmpLog, err := os.OpenFile(tmpLogPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp log: %w", err)
	}
	tmpIdx, err := os.OpenFile(tmpIdxPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		_ = tmpLog.Close()
		_ = os.Remove(tmpLogPath)
		return fmt.Errorf("create temp index: %w", err)
	}

	var pos int64
	if seg.version >= RecordVersion {
		hdr := [5]byte{segMagic[0], segMagic[1], segMagic[2], segMagic[3], RecordVersion}
		if _, err := tmpLog.Write(hdr[:]); err != nil {
			_ = tmpLog.Close()
			_ = tmpIdx.Close()
			_ = os.Remove(tmpLogPath)
			_ = os.Remove(tmpIdxPath)
			return fmt.Errorf("write header: %w", err)
		}
		pos = segFileHeaderSize
	}

	hdrSize := recV1HeaderSize
	if seg.version >= RecordVersion {
		hdrSize = recV2HeaderSize
	}

	var byteSinceIndex int64
	for _, rec := range keptRaw {
		recOffset := int64(binary.LittleEndian.Uint64(rec[0:8]))
		recPos := pos

		if _, err := tmpLog.Write(rec); err != nil {
			_ = tmpLog.Close()
			_ = tmpIdx.Close()
			_ = os.Remove(tmpLogPath)
			_ = os.Remove(tmpIdxPath)
			return fmt.Errorf("write record: %w", err)
		}
		pos += int64(len(rec))
		byteSinceIndex += int64(len(rec))

		if byteSinceIndex >= seg.indexEvery {
			idxEntry := make([]byte, 16)
			binary.LittleEndian.PutUint64(idxEntry[0:8], uint64(recOffset))
			binary.LittleEndian.PutUint64(idxEntry[8:16], uint64(recPos))
			if _, err := tmpIdx.Write(idxEntry); err != nil {
				_ = tmpLog.Close()
				_ = tmpIdx.Close()
				_ = os.Remove(tmpLogPath)
				_ = os.Remove(tmpIdxPath)
				return fmt.Errorf("write index: %w", err)
			}
			byteSinceIndex = 0
		}
	}
	_ = hdrSize // header size already accounted for via raw record bytes

	if err := tmpLog.Sync(); err != nil {
		_ = tmpLog.Close()
		_ = tmpIdx.Close()
		_ = os.Remove(tmpLogPath)
		_ = os.Remove(tmpIdxPath)
		return fmt.Errorf("sync temp log: %w", err)
	}
	if err := tmpIdx.Sync(); err != nil {
		_ = tmpLog.Close()
		_ = tmpIdx.Close()
		_ = os.Remove(tmpLogPath)
		_ = os.Remove(tmpIdxPath)
		return fmt.Errorf("sync temp index: %w", err)
	}
	_ = tmpLog.Close()
	_ = tmpIdx.Close()

	// Close the segment's current handles before renaming over them — on
	// Linux the rename swaps the directory entry but existing open file
	// descriptors keep pointing at the old (now unlinked) inode, so the
	// segment must reopen the path afterwards to observe the new content.
	_ = seg.logFile.Close()
	_ = seg.indexFile.Close()

	if err := os.Rename(tmpLogPath, logPath); err != nil {
		reopenOriginal(seg, logPath, idxPath)
		return fmt.Errorf("rename log: %w", err)
	}
	if err := os.Rename(tmpIdxPath, idxPath); err != nil {
		// logPath now has NEW content, idxPath still OLD — log the
		// inconsistency; the segment remains readable via full-scan
		// fallback but the sparse index is stale.
		reopenOriginal(seg, logPath, idxPath)
		return fmt.Errorf("rename index: %w", err)
	}

	newLog, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopen log: %w", err)
	}
	newIdx, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		_ = newLog.Close()
		return fmt.Errorf("reopen index: %w", err)
	}

	fi, _ := newLog.Stat()
	seg.logFile = newLog
	seg.indexFile = newIdx
	seg.logSize = fi.Size()
	seg.byteSinceIndex = 0
	return nil
}

// reopenOriginal reopens whatever currently exists at logPath and idxPath
// and reassigns seg.logFile / seg.indexFile. This is a best-effort recovery
// called after a partial rename failure (e.g. log renamed but index didn't,
// or vice versa). The segment remains readable via full-scan fallback but
// the sparse index should be considered untrusted.
func reopenOriginal(seg *Segment, logPath, idxPath string) {
	newLog, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	newIdx, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		_ = newLog.Close()
		return
	}
	seg.logFile = newLog
	seg.indexFile = newIdx
	fi, _ := newLog.Stat()
	if fi != nil {
		seg.logSize = fi.Size()
	}
	seg.byteSinceIndex = 0
}
