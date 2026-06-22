package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/wal"
)

func openWAL(t *testing.T) (*wal.WAL, []wal.Entry, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, entries, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, entries, path
}

func TestWAL_AppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.wal")

	// First open — write three entries.
	w1, _, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for i, msg := range []string{"hello", "world", "broker"} {
		off, err := w1.Append([]byte(msg))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("offset[%d]: want %d, got %d", i, i, off)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Second open — verify recovery.
	_, entries, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(entries))
	}
	want := []string{"hello", "world", "broker"}
	for i, e := range entries {
		if string(e.Data) != want[i] {
			t.Errorf("entry[%d]: want %q, got %q", i, want[i], string(e.Data))
		}
	}
}

func TestWAL_CorruptionDetection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	w, _, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append([]byte("good entry")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the last byte of data.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Recovery should return zero entries (corrupt entry truncated).
	_, entries, err := wal.Open(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 valid entries after corruption, got %d", len(entries))
	}
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	w, _, _ := openWAL(t)
	const goroutines = 50
	const perGoroutine = 100
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perGoroutine; i++ {
				if _, err := w.Append([]byte("concurrent")); err != nil {
					t.Errorf("concurrent Append: %v", err)
				}
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	total := goroutines * perGoroutine
	if got := w.NextOffset(); got != int64(total) {
		t.Errorf("NextOffset: want %d, got %d", total, got)
	}
}

func TestWAL_EmptyRecovery(t *testing.T) {
	_, entries, _ := openWAL(t)
	if len(entries) != 0 {
		t.Errorf("fresh WAL should have 0 entries, got %d", len(entries))
	}
}

func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	w, _, err := wal.Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer w.Close()
	payload := make([]byte, 256)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}
