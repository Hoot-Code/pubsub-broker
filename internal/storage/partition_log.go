package storage

import (
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ISRUpdater is implemented by objects that track in-sync replica state for a
// partition. *cluster.ISRTracker satisfies this interface.
// Using an interface here prevents an import cycle between storage and cluster.
type ISRUpdater interface {
	// Update records that nodeID has durably written up to offset.
	Update(nodeID string, offset int64)
}

// PartitionLog manages an ordered sequence of Segments for one topic-partition.
type PartitionLog struct {
	mu          sync.RWMutex
	dir         string
	segments    []*Segment
	segMaxBytes int64
	indexEvery  int64
	syncPolicy  string

	notifyAppend chan struct{}

	// isr is an optional hook called by the replicator to report replica acks.
	// Set via SetISRTracker; may be nil in single-node mode.
	isr ISRUpdater

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// OpenPartitionLog opens (or creates) a PartitionLog in dir.
// All existing segments are loaded and sorted; a new initial segment is created
// when the directory is empty.
func OpenPartitionLog(dir string, segMaxBytes, indexEvery int64, syncPolicy string) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	pl := &PartitionLog{
		dir:          dir,
		segMaxBytes:  segMaxBytes,
		indexEvery:   indexEvery,
		syncPolicy:   syncPolicy,
		notifyAppend: make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("readdir: %w", err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		seg, err := createSegment(dir, base, segMaxBytes, indexEvery, syncPolicy)
		if err != nil {
			return nil, fmt.Errorf("open segment %d: %w", base, err)
		}
		pl.segments = append(pl.segments, seg)
	}
	sort.Slice(pl.segments, func(i, j int) bool {
		return pl.segments[i].baseOffset < pl.segments[j].baseOffset
	})

	if len(pl.segments) == 0 {
		seg, err := createSegment(dir, 0, segMaxBytes, indexEvery, syncPolicy)
		if err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
		pl.segments = append(pl.segments, seg)
	}

	if syncPolicy == "interval" {
		pl.wg.Add(1)
		go pl.syncLoop()
	}
	return pl, nil
}

func (pl *PartitionLog) syncLoop() {
	defer pl.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-pl.stopCh:
			return
		case <-ticker.C:
			pl.mu.RLock()
			if len(pl.segments) > 0 {
				_ = pl.segments[len(pl.segments)-1].Sync()
			}
			pl.mu.RUnlock()
		}
	}
}

// Append writes msg to the active segment, rolling over to a new segment if needed.
// Returns the assigned offset.
func (pl *PartitionLog) Append(msg *types.Message) (int64, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	active := pl.segments[len(pl.segments)-1]
	if active.IsFull() {
		nextBase := active.NextOffset()
		newSeg, err := createSegment(pl.dir, nextBase, pl.segMaxBytes, pl.indexEvery, pl.syncPolicy)
		if err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
		pl.segments = append(pl.segments, newSeg)
		active = newSeg
	}

	offset, err := active.Write(msg)
	if err != nil {
		return 0, err
	}

	select {
	case pl.notifyAppend <- struct{}{}:
	default:
	}
	return offset, nil
}

// NotifyAppend returns a channel that receives a signal when a message is appended.
func (pl *PartitionLog) NotifyAppend() <-chan struct{} { return pl.notifyAppend }

// SetISRTracker attaches an ISRUpdater to this partition log. Passing nil
// clears the tracker. Safe for concurrent use.
func (pl *PartitionLog) SetISRTracker(t ISRUpdater) {
	pl.mu.Lock()
	pl.isr = t
	pl.mu.Unlock()
}

// ISRTracker returns the currently attached ISRUpdater, or nil if none is set.
func (pl *PartitionLog) ISRTracker() ISRUpdater {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.isr
}

// Read returns messages starting at startOffset, up to maxCount.
func (pl *PartitionLog) Read(startOffset int64, maxCount int) ([]*types.Message, error) {
	// Take a copy of the segments slice header under the read lock so
	// that a concurrent append (segment rollover in Append, or Compact) growing
	// the underlying array cannot leave this caller iterating a stale slice.
	pl.mu.RLock()
	segs := make([]*Segment, len(pl.segments))
	copy(segs, pl.segments)
	pl.mu.RUnlock()

	var msgs []*types.Message
	for _, seg := range segs {
		if seg.NextOffset() <= startOffset {
			continue
		}
		batch, err := seg.Read(startOffset, maxCount-len(msgs))
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, batch...)
		if len(msgs) >= maxCount {
			break
		}
	}
	return msgs, nil
}

// NextOffset returns the next offset that would be assigned to a new message.
func (pl *PartitionLog) NextOffset() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if len(pl.segments) == 0 {
		return 0
	}
	return pl.segments[len(pl.segments)-1].NextOffset()
}

// Close stops background goroutines and closes all segment files.
func (pl *PartitionLog) Close() error {
	select {
	case <-pl.stopCh:
	default:
		close(pl.stopCh)
	}
	pl.wg.Wait()

	pl.mu.Lock()
	defer pl.mu.Unlock()
	for _, seg := range pl.segments {
		_ = seg.Close()
	}
	return nil
}

// Compact removes segments older than maxAgeHours or when total size exceeds
// maxSizeMB. The active segment is never removed.
// Returns the number of deleted segments.
//
// Lock discipline: the write lock is held only long enough to decide
// which segments to evict and to remove them from pl.segments. The actual file
// I/O (Close + os.Remove) is performed AFTER releasing the lock so that
// concurrent reads and appends are not blocked for the (potentially
// multi-millisecond) duration of the unlink calls.
func (pl *PartitionLog) Compact(maxAgeHours int64, maxSizeMB int64) (deleted int, err error) {
	pl.mu.Lock()

	if len(pl.segments) <= 1 {
		pl.mu.Unlock()
		return 0, nil
	}

	cutoffTime := time.Now().Add(-time.Duration(maxAgeHours) * time.Hour)
	maxBytes := maxSizeMB * 1024 * 1024

	var keep []*Segment
	// toDelete holds the segments evicted from pl.segments; their files are
	// removed after the lock is released.
	var toDelete []*Segment
	for i, seg := range pl.segments {
		if i == len(pl.segments)-1 {
			keep = append(keep, seg)
			continue
		}
		fi, ferr := seg.logFile.Stat()
		if ferr != nil {
			keep = append(keep, seg)
			continue
		}
		tooOld := maxAgeHours > 0 && fi.ModTime().Before(cutoffTime)
		tooBig := maxBytes > 0 && pl.totalLogSize() > maxBytes
		if tooOld || tooBig {
			toDelete = append(toDelete, seg)
			deleted++
		} else {
			keep = append(keep, seg)
		}
	}
	pl.segments = keep
	pl.mu.Unlock()

	// Perform file I/O (Close + os.Remove) outside the lock. If a Remove fails
	// we log via the returned error but do NOT re-acquire the lock or touch
	// pl.segments — the segment is already logically removed, so a leftover
	// file is harmless (it will simply be orphaned on disk).
	for _, seg := range toDelete {
		_ = seg.logFile.Close()
		_ = seg.indexFile.Close()
		if rErr := os.Remove(seg.logFile.Name()); rErr != nil && err == nil {
			err = fmt.Errorf("remove log %s: %w", seg.logFile.Name(), rErr)
		}
		if rErr := os.Remove(seg.indexFile.Name()); rErr != nil && err == nil {
			err = fmt.Errorf("remove index %s: %w", seg.indexFile.Name(), rErr)
		}
	}
	return deleted, err
}

// totalLogSize returns the sum of all segment log file sizes.
// Caller must hold pl.mu (at least read lock).
func (pl *PartitionLog) totalLogSize() int64 {
	var total int64
	for _, s := range pl.segments {
		total += s.logSize
	}
	return total
}

// SendTo transfers raw segment bytes covering [startOffset, …) to dst using
// the OS zero-copy path (sendfile on Linux, read+write elsewhere).
// Returns the total bytes transferred and any I/O error.
func (pl *PartitionLog) SendTo(dst net.Conn, startOffset int64, maxBytes int64) (int64, error) {
	pl.mu.RLock()
	segs := make([]*Segment, len(pl.segments))
	copy(segs, pl.segments)
	pl.mu.RUnlock()

	var totalSent int64
	firstSegIdx := -1

	for i, seg := range segs {
		if seg.NextOffset() <= startOffset {
			continue
		}
		if firstSegIdx == -1 {
			firstSegIdx = i
		}
		if maxBytes > 0 && totalSent >= maxBytes {
			break
		}

		seg.mu.RLock()
		f := seg.logFile
		fileSize := seg.logSize
		dataStart := seg.dataStart
		seg.mu.RUnlock()

		var fileOffset int64
		if i == firstSegIdx {
			fileOffset = seg.findExactPosition(startOffset, dataStart)
		} else {
			fileOffset = dataStart
		}

		length := fileSize - fileOffset
		if length <= 0 {
			continue
		}
		if maxBytes > 0 && totalSent+length > maxBytes {
			length = maxBytes - totalSent
		}

		n, err := sendfileSegment(dst, f, fileOffset, length)
		totalSent += n
		if err != nil {
			return totalSent, fmt.Errorf("storage: SendTo segment: %w", err)
		}
	}
	return totalSent, nil
}

// SegmentCount returns the number of segments in this partition log.
func (pl *PartitionLog) SegmentCount() int {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return len(pl.segments)
}

// TotalLogSize returns the sum of all segment log file sizes in bytes.
func (pl *PartitionLog) TotalLogSize() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.totalLogSize()
}

// ActiveSegmentBytes returns the byte size of the active (last) segment.
func (pl *PartitionLog) ActiveSegmentBytes() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if len(pl.segments) == 0 {
		return 0
	}
	return pl.segments[len(pl.segments)-1].Size()
}

// OffsetForTimestamp returns the first logical offset whose record timestamp
// is >= ts (Unix nanoseconds). Scans segments from oldest to newest.
// Returns pl.NextOffset() when ts is in the future, 0 when ts predates all records.
func (pl *PartitionLog) OffsetForTimestamp(ts int64) (int64, error) {
	pl.mu.RLock()
	segs := make([]*Segment, len(pl.segments))
	copy(segs, pl.segments)
	pl.mu.RUnlock()

	for _, seg := range segs {
		seg.mu.RLock()
		name := seg.logFile.Name()
		dataStart := seg.dataStart
		version := seg.version
		seg.mu.RUnlock()

		f, err := os.Open(name)
		if err != nil {
			continue
		}
		if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
			_ = f.Close()
			continue
		}

		for {
			msg, offset, err := decodeRecord(f, version)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				break
			}
			if msg.Timestamp >= ts {
				_ = f.Close()
				return offset, nil
			}
		}
		_ = f.Close()
	}
	return pl.NextOffset(), nil
}
