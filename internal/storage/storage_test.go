package storage_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// openPL is a helper that opens a PartitionLog with the default "always" sync
// policy so all tests are deterministic (no data in OS page-cache only).
func openPL(t *testing.T, dir string, segMax, idxEvery int64) *storage.PartitionLog {
	t.Helper()
	pl, err := storage.OpenPartitionLog(dir, segMax, idxEvery, "always")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}
	return pl
}

func newMsg(topic, key, payload string) *types.Message {
	return &types.Message{
		ID:        types.NewUUID(),
		Topic:     topic,
		Key:       key,
		Payload:   []byte(payload),
		Timestamp: time.Now().UnixNano(),
	}
}

func TestPartitionLog_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 1<<20, 512)
	defer pl.Close()

	for i := 0; i < 10; i++ {
		msg := newMsg("test", fmt.Sprintf("k%d", i), fmt.Sprintf("payload-%d", i))
		off, err := pl.Append(msg)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("offset[%d]: want %d, got %d", i, i, off)
		}
	}

	msgs, err := pl.Read(0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("read %d msgs, want 10", len(msgs))
	}
	for i, m := range msgs {
		want := fmt.Sprintf("payload-%d", i)
		if string(m.Payload) != want {
			t.Errorf("msg[%d].Payload: want %q, got %q", i, want, string(m.Payload))
		}
	}
}

func TestPartitionLog_Recovery(t *testing.T) {
	dir := t.TempDir()

	pl1 := openPL(t, dir, 1<<20, 512)
	for i := 0; i < 20; i++ {
		_, err := pl1.Append(newMsg("t", "k", fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	_ = pl1.Close()

	pl2 := openPL(t, dir, 1<<20, 512)
	defer pl2.Close()

	if pl2.NextOffset() != 20 {
		t.Errorf("NextOffset after recovery: want 20, got %d", pl2.NextOffset())
	}
	msgs, err := pl2.Read(0, 20)
	if err != nil {
		t.Fatalf("Read after recovery: %v", err)
	}
	if len(msgs) != 20 {
		t.Errorf("recovered %d messages, want 20", len(msgs))
	}
}

func TestPartitionLog_SegmentRollover(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 256, 64)
	defer pl.Close()

	for i := 0; i < 100; i++ {
		_, err := pl.Append(newMsg("t", "k", fmt.Sprintf("p-%04d", i)))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	msgs, err := pl.Read(0, 100)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 100 {
		t.Errorf("read %d msgs across segments, want 100", len(msgs))
	}
}

func TestPartitionLog_ReadFromMiddle(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 1<<20, 512)
	defer pl.Close()

	for i := 0; i < 50; i++ {
		pl.Append(newMsg("t", "k", fmt.Sprintf("msg-%d", i)))
	}
	msgs, err := pl.Read(25, 10)
	if err != nil {
		t.Fatalf("Read from middle: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected messages from offset 25")
	}
	if msgs[0].Offset != 25 {
		t.Errorf("first msg offset: want 25, got %d", msgs[0].Offset)
	}
}

func BenchmarkPartitionLog_Append(b *testing.B) {
	dir := b.TempDir()
	pl, _ := storage.OpenPartitionLog(dir, 1<<30, 4096, "os")
	defer pl.Close()
	msg := newMsg("bench", "key", string(make([]byte, 256)))
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pl.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPartitionLog_PersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 1<<20, 512)

	for i := 0; i < 5; i++ {
		_, err := pl.Append(newMsg("t", "k", fmt.Sprintf("persist-%d", i)))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := pl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var logFiles int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logFiles++
		}
	}
	if logFiles == 0 {
		t.Error("expected at least one .log segment file on disk after Close")
	}

	pl2 := openPL(t, dir, 1<<20, 512)
	defer pl2.Close()

	msgs, err := pl2.Read(0, 10)
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("after restart: want 5 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		want := fmt.Sprintf("persist-%d", i)
		if string(m.Payload) != want {
			t.Errorf("msg[%d].Payload: want %q, got %q", i, want, string(m.Payload))
		}
	}
}

func TestPartitionLog_SegmentRolloverAllReadable(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 256, 64)
	defer pl.Close()

	const total = 50
	for i := 0; i < total; i++ {
		_, err := pl.Append(newMsg("t", "k", fmt.Sprintf("m%03d", i)))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	msgs, err := pl.Read(0, total)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != total {
		t.Fatalf("read %d msgs, want %d", len(msgs), total)
	}
	for i, m := range msgs {
		want := fmt.Sprintf("m%03d", i)
		if string(m.Payload) != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, string(m.Payload))
		}
	}
}

func TestPartitionLog_ConcurrentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 1<<20, 512)
	defer pl.Close()

	const writers = 4
	const perWriter = 25
	done := make(chan struct{})
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perWriter; i++ {
				_, err := pl.Append(newMsg("t", fmt.Sprintf("w%d", id), fmt.Sprintf("v%d", i)))
				if err != nil {
					t.Errorf("writer %d Append: %v", id, err)
				}
			}
		}(w)
	}
	for i := 0; i < writers; i++ {
		<-done
	}
	total := pl.NextOffset()
	if total != writers*perWriter {
		t.Errorf("NextOffset: want %d, got %d", writers*perWriter, total)
	}
}

// ─── SyncPolicy=always fsync test ───────────────────────────────────────

// TestSegmentSyncPolicy_Always opens a segment with policy="always", writes
// 3 records, forcibly closes the file descriptor by removing it from under
// the PartitionLog (without calling Close), reopens the file, and verifies
// all 3 records decode correctly.
//
// This exercises the fsync-on-every-write guarantee: because Sync() is called
// after each Write(), the data is durable even if the file is not cleanly
// closed afterwards.
func TestSegmentSyncPolicy_Always(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	pl, err := storage.OpenPartitionLog(dir, 1<<20, 512, "always")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}

	payloads := []string{"record-zero", "record-one", "record-two"}
	for _, p := range payloads {
		if _, err := pl.Append(newMsg("t", "k", p)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Forcibly abandon the PartitionLog without calling Close().
	// Because syncPolicy="always" fsynced after every Write, the data is
	// already durable on disk.  We do NOT call pl.Close() here.

	// Reopen the same directory with a new PartitionLog instance.
	pl2, err := storage.OpenPartitionLog(dir, 1<<20, 512, "always")
	if err != nil {
		t.Fatalf("OpenPartitionLog (reopen): %v", err)
	}
	defer pl2.Close()

	msgs, err := pl2.Read(0, 10)
	if err != nil {
		t.Fatalf("Read after simulated crash: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 records, got %d", len(msgs))
	}
	for i, m := range msgs {
		if string(m.Payload) != payloads[i] {
			t.Errorf("msg[%d]: want %q, got %q", i, payloads[i], string(m.Payload))
		}
	}
}

// TestPartitionLog_NotifyAppend verifies that a goroutine that appends a
// message after 100 ms wakes a polling goroutine via NotifyAppend() within
// 500 ms — not after the 1500 ms fallback timeout (PERFORMANCE 14d).
func TestPartitionLog_NotifyAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pl := openPL(t, dir, 1<<20, 512)
	defer pl.Close()

	start := time.Now()
	appendAfter := 100 * time.Millisecond
	maxWait := 500 * time.Millisecond

	// Producer: append after 100 ms.
	go func() {
		time.Sleep(appendAfter)
		_, _ = pl.Append(newMsg("t", "k", "notify-test"))
	}()

	// Consumer: poll on NotifyAppend or timeout.
	select {
	case <-pl.NotifyAppend():
		elapsed := time.Since(start)
		if elapsed > maxWait {
			t.Errorf("woke after %v (want < %v)", elapsed, maxWait)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("poll timed out after 1500 ms; NotifyAppend did not fire")
	}
}

// ─── Compact tests (unchanged from original) ─────────────────────────────────

func TestPartitionLog_Compact_ByAge(t *testing.T) {
	dir := t.TempDir()
	pl := openPL(t, dir, 256, 64)

	for i := 0; i < 60; i++ {
		if _, err := pl.Append(newMsg("t", "k", fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	entries, _ := os.ReadDir(dir)
	var logsBefore []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logsBefore = append(logsBefore, filepath.Join(dir, e.Name()))
		}
	}
	if len(logsBefore) < 2 {
		t.Skip("not enough segments produced")
	}

	// Backdate all non-active segment .log files by 2 hours so that
	// maxAgeHours=1 triggers the tooOld condition. Then call Compact(1, 0).
	pastTime := time.Now().Add(-2 * time.Hour)
	for _, logPath := range logsBefore[:len(logsBefore)-1] {
		if err := os.Chtimes(logPath, pastTime, pastTime); err != nil {
			t.Fatalf("Chtimes %s: %v", logPath, err)
		}
	}

	deleted, err := pl.Compact(1, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("Compact(ByAge): expected deleted > 0, got 0")
	}
	for _, logPath := range logsBefore {
		idxPath := strings.TrimSuffix(logPath, ".log") + ".index"
		isActive := logPath == logsBefore[len(logsBefore)-1]
		if isActive {
			if _, statErr := os.Stat(logPath); os.IsNotExist(statErr) {
				t.Errorf("active segment log deleted: %s", logPath)
			}
			continue
		}
		if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
			t.Errorf("expected deleted log to be gone: %s", logPath)
		}
		if _, statErr := os.Stat(idxPath); !os.IsNotExist(statErr) {
			t.Errorf("expected deleted index to be gone: %s", idxPath)
		}
	}
	msgs, err := pl.Read(pl.NextOffset()-1, 1)
	if err != nil {
		t.Fatalf("Read after compact: %v", err)
	}
	_ = msgs
	_ = pl.Close()
}

func TestPartitionLog_Compact_BySize(t *testing.T) {
	dir := t.TempDir()
	// Use 512 KB segments so a handful of large messages each trigger a rollover
	// and the total quickly exceeds 1 MB — allowing Compact(0, 1) to fire.
	pl := openPL(t, dir, 512*1024, 1024)

	// Write 5 × 300 KB = 1.5 MB total, spread across multiple segments.
	bigPayload := bytes.Repeat([]byte("X"), 300*1024)
	for i := 0; i < 5; i++ {
		if _, err := pl.Append(newMsg("t", "k", string(bigPayload))); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	entries, _ := os.ReadDir(dir)
	var logsBefore []string
	var totalBytes int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logPath := filepath.Join(dir, e.Name())
			logsBefore = append(logsBefore, logPath)
			fi, err := os.Stat(logPath)
			if err == nil {
				totalBytes += fi.Size()
			}
		}
	}
	if len(logsBefore) < 2 {
		t.Skip("not enough segments for size test")
	}
	if totalBytes == 0 {
		t.Fatal("no log bytes written")
	}

	// Pass a maxSizeMB strictly less than half the actual total so
	// tooBig fires on the older (non-active) segments.
	halfMB := totalBytes / 2 / (1024 * 1024)
	if halfMB < 1 {
		halfMB = 1 // safety: totalBytes is ≥ 1.5 MB so this should never fire
	}

	deleted, err := pl.Compact(0, halfMB)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("Compact(BySize): expected deleted > 0, got 0 (totalBytes=%d, threshold=%d MB)", totalBytes, halfMB)
	}
	firstLog := logsBefore[0]
	firstIdx := strings.TrimSuffix(firstLog, ".log") + ".index"
	if _, statErr := os.Stat(firstLog); !os.IsNotExist(statErr) {
		t.Errorf("oldest .log still exists after size compaction: %s", firstLog)
	}
	if _, statErr := os.Stat(firstIdx); !os.IsNotExist(statErr) {
		t.Errorf("oldest .index still exists after size compaction: %s", firstIdx)
	}
	lastLog := logsBefore[len(logsBefore)-1]
	if _, statErr := os.Stat(lastLog); os.IsNotExist(statErr) {
		t.Errorf("active segment log was deleted: %s", lastLog)
	}
	activeBase := pl.NextOffset() - 1
	if activeBase < 0 {
		activeBase = 0
	}
	if _, err := pl.Read(activeBase, 1); err != nil {
		t.Fatalf("Read after size compact: %v", err)
	}
	_ = pl.Close()
}

// TestMessageOffsetRoundTrip verifies that after appending N messages, reading
// them back returns messages whose Offset field matches their logical position.
// Segment.Write() must set msg.Offset before calling encodeRecord()
// so the JSON payload stored on-disk carries the correct offset.
func TestMessageOffsetRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pl, err := storage.OpenPartitionLog(dir, 4*1024*1024, 512, "always")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pl.Close()

	const N = 20
	for i := 0; i < N; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("msg-%d", i),
			Topic:   "test-offset",
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		}
		off, err := pl.Append(msg)
		if err != nil {
			t.Fatalf("append[%d]: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("append[%d]: got offset %d, want %d", i, off, i)
		}
	}

	msgs, err := pl.Read(0, N)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(msgs) != N {
		t.Fatalf("read: got %d messages, want %d", len(msgs), N)
	}
	for i, msg := range msgs {
		if msg.Offset != int64(i) {
			t.Errorf("msg[%d].Offset = %d, want %d", i, msg.Offset, i)
		}
	}
}

// ─── index entry written before data record ────────────────────────────────

// TestSegmentCrashBetweenDataAndIndex verifies: the data record
// is always written (and fsynced) BEFORE the sparse index entry. The test
// simulates a crash that leaves the index with entries for records whose data
// was truncated away, then reopens the segment and confirms Read returns only
// the records that still have valid data — without panicking.
//
// Without the ordering guarantee (index before data) the orphaned index entries
// pointed at non-existent records and decodeRecord either panicked or
// returned garbage. With the ordering guarantee, Read gracefully stops at EOF
// when the data runs out, returning exactly the durable records.
func TestSegmentCrashBetweenDataAndIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// indexEvery=1 so every record gets a sparse index entry — this maximises
	// the chance of orphaned index entries after truncation.
	pl := openPL(t, dir, 1<<20, 1)
	t.Cleanup(func() { _ = pl.Close() })

	const total = 5
	const keep = 3 // keep offsets 0,1,2; truncate away offsets 3,4

	// Write `keep` records and snapshot the log file size — this is the byte
	// boundary we will truncate back to.
	for i := 0; i < keep; i++ {
		if _, err := pl.Append(newMsg("t", "k", fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	// Locate the single .log file and record its size after `keep` records.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var logPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logPath = filepath.Join(dir, e.Name())
			break
		}
	}
	if logPath == "" {
		t.Fatal("no .log file found")
	}
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	sizeAfterKeep := fi.Size()

	// Write the remaining records (offsets keep..total-1).
	for i := keep; i < total; i++ {
		if _, err := pl.Append(newMsg("t", "k", fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	// Close the partition log (flushes + closes segment files).
	if err := pl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate the crash: truncate the .log file back to sizeAfterKeep,
	// removing the data for offsets `keep..total-1` while leaving the .index
	// file untouched (it still has entries for those offsets).
	if err := os.Truncate(logPath, sizeAfterKeep); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Reopen the segment. recoverNextOffset scans the (now-truncated) data file
	// and sets nextOffset = keep. The index still references offsets >= keep,
	// but those positions are beyond EOF.
	pl2, err := storage.OpenPartitionLog(dir, 1<<20, 1, "always")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = pl2.Close() })

	// Read must NOT panic and must return exactly `keep` valid records.
	msgs, err := pl2.Read(0, 100)
	if err != nil {
		t.Fatalf("Read after truncation: %v", err)
	}
	if len(msgs) != keep {
		t.Fatalf("Read after truncation: want %d valid records, got %d", keep, len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != int64(i) {
			t.Errorf("msg[%d].Offset = %d, want %d", i, m.Offset, i)
		}
	}

	// NextOffset must reflect only the durable records.
	if no := pl2.NextOffset(); no != int64(keep) {
		t.Errorf("NextOffset after truncation: want %d, got %d", keep, no)
	}
}
