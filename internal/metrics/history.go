// Package metrics implements a Prometheus-compatible metrics registry with
// text-format exposition (version 0.0.4). Uses stdlib only.
package metrics

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Sample is a single timestamped data point for one metric.
type Sample struct {
	// Timestamp is when the sample was collected.
	Timestamp time.Time
	// Value is the metric value at that point in time.
	Value float64
}

// HistoryStore is a bounded in-memory time-series ring buffer for operational
// dashboard charts (5m/15m/1h/24h windows). With default settings:
//
//	retention=24h, sampleInterval=10s → 8640 samples per metric.
//	Approximate memory: ~15 metrics * 8640 samples * 16 bytes ≈ 2 MB.
type HistoryStore struct {
	mu             sync.RWMutex
	data           map[string][]Sample // metric name → samples
	retention      time.Duration
	sampleInterval time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// NewHistoryStore creates a HistoryStore with the given retention and sample interval.
func NewHistoryStore(retention, sampleInterval time.Duration) *HistoryStore {
	return &HistoryStore{
		data:           make(map[string][]Sample),
		retention:      retention,
		sampleInterval: sampleInterval,
		stopCh:         make(chan struct{}),
	}
}

// Start begins periodic collection by calling collect every sampleInterval.
// It stores the returned metric values and evicts samples older than retention.
// The context controls shutdown: when ctx is cancelled, collection stops.
func (h *HistoryStore) Start(ctx context.Context, collect func() map[string]float64) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.sampleInterval)
		defer ticker.Stop()

		// Run an initial collection immediately.
		h.recordSample(collect())

		for {
			select {
			case <-ctx.Done():
				return
			case <-h.stopCh:
				return
			case <-ticker.C:
				h.recordSample(collect())
			}
		}
	}()
}

// recordSample stores a sample for each metric and evicts old data.
func (h *HistoryStore) recordSample(values map[string]float64) {
	now := time.Now()
	cutoff := now.Add(-h.retention)

	h.mu.Lock()
	defer h.mu.Unlock()

	for name, val := range values {
		samples := h.data[name]
		// Evict old samples.
		for len(samples) > 0 && samples[0].Timestamp.Before(cutoff) {
			samples = samples[1:]
		}
		samples = append(samples, Sample{Timestamp: now, Value: val})
		h.data[name] = samples
	}
}

// Range returns all samples for all metrics since the given time, sorted by
// metric name then by timestamp. The map keys are metric names and the values
// are ordered slices of samples.
func (h *HistoryStore) Range(since time.Time) map[string][]Sample {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make(map[string][]Sample, len(h.data))
	for name, samples := range h.data {
		var filtered []Sample
		for _, s := range samples {
			if !s.Timestamp.Before(since) {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			out[name] = filtered
		}
	}
	return out
}

// RangeSorted returns all samples for all metrics since the given time,
// as a sorted slice of (name, samples) pairs. The metric names are sorted
// alphabetically.
func (h *HistoryStore) RangeSorted(since time.Time) []HistorySeries {
	data := h.Range(since)
	names := make([]string, 0, len(data))
	for name := range data {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]HistorySeries, len(names))
	for i, name := range names {
		out[i] = HistorySeries{Name: name, Samples: data[name]}
	}
	return out
}

// Stop halts background collection and waits for the goroutine to exit.
func (h *HistoryStore) Stop() {
	select {
	case <-h.stopCh:
	default:
		close(h.stopCh)
	}
	h.wg.Wait()
}

// HistorySeries is a named time series of samples.
type HistorySeries struct {
	// Name is the metric name.
	Name string
	// Samples is the ordered list of data points.
	Samples []Sample
}

// Snapshot returns a copy of the raw data for the given metric name.
// Returns nil if the metric has no data.
func (h *HistoryStore) Snapshot(metric string) []Sample {
	h.mu.RLock()
	defer h.mu.RUnlock()
	samples := h.data[metric]
	if samples == nil {
		return nil
	}
	out := make([]Sample, len(samples))
	copy(out, samples)
	return out
}
