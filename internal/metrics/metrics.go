// Package metrics implements a Prometheus-compatible metrics registry with
// text-format exposition (version 0.0.4). Uses stdlib only.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ─── Counter ─────────────────────────────────────────────────────────────────

// Counter is a monotonically increasing metric.
type Counter struct {
	name   string
	help   string
	labels map[string]string
	value  atomic.Uint64
}

// Inc adds delta to the counter (delta must be positive).
func (c *Counter) Inc(delta uint64) { c.value.Add(delta) }

// Value returns the current counter value.
func (c *Counter) Value() uint64 { return c.value.Load() }

// ─── Gauge ───────────────────────────────────────────────────────────────────

// Gauge is a metric that can go up and down.
type Gauge struct {
	name   string
	help   string
	labels map[string]string
	mu     sync.Mutex
	value  float64
}

// Set sets the gauge to v.
func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	g.value = v
	g.mu.Unlock()
}

// Add adds delta to the gauge.
func (g *Gauge) Add(delta float64) {
	g.mu.Lock()
	g.value += delta
	g.mu.Unlock()
}

// Value returns the current gauge value.
func (g *Gauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.value
}

// ─── Histogram ───────────────────────────────────────────────────────────────

// Histogram tracks the distribution of observed values.
type Histogram struct {
	name    string
	help    string
	labels  map[string]string
	mu      sync.Mutex
	buckets []float64 // upper bounds
	counts  []uint64  // bucket[i] = count of obs <= buckets[i]
	sum     float64
	count   uint64
}

// Observe records a single observation.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, bound := range h.buckets {
		if v <= bound {
			h.counts[i]++
		}
	}
}

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry holds all registered metrics and exposes them in Prometheus text format.
type Registry struct {
	mu         sync.RWMutex
	counters   []*Counter
	gauges     []*Gauge
	histograms []*Histogram
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry { return &Registry{} }

// NewCounter registers and returns a counter.
func (r *Registry) NewCounter(name, help string, labels map[string]string) *Counter {
	c := &Counter{name: name, help: help, labels: labels}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

// NewGauge registers and returns a gauge.
func (r *Registry) NewGauge(name, help string, labels map[string]string) *Gauge {
	g := &Gauge{name: name, help: help, labels: labels}
	r.mu.Lock()
	r.gauges = append(r.gauges, g)
	r.mu.Unlock()
	return g
}

// NewHistogram registers and returns a histogram with the given bucket bounds.
func (r *Registry) NewHistogram(name, help string, labels map[string]string, bounds []float64) *Histogram {
	sorted := make([]float64, len(bounds))
	copy(sorted, bounds)
	sort.Float64s(sorted)
	h := &Histogram{
		name:    name,
		help:    help,
		labels:  labels,
		buckets: sorted,
		counts:  make([]uint64, len(sorted)),
	}
	r.mu.Lock()
	r.histograms = append(r.histograms, h)
	r.mu.Unlock()
	return h
}

// escapeHelp escapes a Prometheus HELP string per the text format spec.
// Backslash is replaced with \\ and newline with \n (backslash first).
func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// Expose writes all metrics in Prometheus text exposition format 0.0.4 to w.
// Each counter family is emitted with a # HELP line, a # TYPE counter line,
// and a value line with the _total suffix appended when not already present.
// Label values are escaped per the Prometheus spec. HELP strings are escaped
// per the Prometheus text format spec (backslash → \\ and newline → \n).
func (r *Registry) Expose(w io.Writer) {
	r.mu.RLock()
	counters := r.counters
	gauges := r.gauges
	histograms := r.histograms
	r.mu.RUnlock()

	for _, c := range counters {
		valueName := counterName(c.name)
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			c.name, escapeHelp(c.help), c.name,
			metricLine(valueName, c.labels), c.Value())
	}
	for _, g := range gauges {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n",
			g.name, escapeHelp(g.help), g.name,
			metricLine(g.name, g.labels), g.Value())
	}
	for _, h := range histograms {
		h.mu.Lock()
		sum := h.sum
		count := h.count
		counts := make([]uint64, len(h.counts))
		copy(counts, h.counts)
		buckets := h.buckets
		h.mu.Unlock()

		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, escapeHelp(h.help), h.name)
		var cumulative uint64
		for i, bound := range buckets {
			cumulative += counts[i]
			boundStr := formatFloat(bound)
			lbls := mergeLabels(h.labels, map[string]string{"le": boundStr})
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(lbls), cumulative)
		}
		lblsInf := mergeLabels(h.labels, map[string]string{"le": "+Inf"})
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(lblsInf), count)
		fmt.Fprintf(w, "%s_sum%s %g\n", h.name, labelStr(h.labels), sum)
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelStr(h.labels), count)
	}
}

// counterName returns the metric name with _total suffix for the value line.
// If the name already ends with _total it is returned unchanged.
func counterName(name string) string {
	if strings.HasSuffix(name, "_total") {
		return name
	}
	return name + "_total"
}

// ─── Broker metrics bundle ────────────────────────────────────────────────────

// Broker holds all standard broker metrics.
type Broker struct {
	MessagesPublished *Counter
	MessagesConsumed  *Counter
	MessagesErrored   *Counter
	// MessagesAcked counts explicit ACK frames received from consumers.
	MessagesAcked *Counter
	// MessagesNacked counts explicit NACK frames received from consumers.
	MessagesNacked    *Counter
	ActiveConnections *Gauge
	ConsumerLag       *Gauge
	ReplicationLag    *Gauge
	MessageSizeBytes  *Histogram
	PublishLatencyMs  *Histogram

	// New metrics added in Part C.
	BytesPublished       *Counter
	BytesConsumed        *Counter
	ConsumerLagTotal     *Gauge
	ActiveConsumerGroups *Gauge
	TopicCount           *Gauge
	PartitionCount       *Gauge
	WALBytesTotal        *Counter
	WALEntriesTotal      *Counter

	// Cluster election metrics (Part C3).

	// ElectionCount is incremented each time this node starts an election.
	ElectionCount *Counter
	// LeaderChanges is incremented each time the cluster leader changes.
	LeaderChanges *Counter
	// CurrentTerm tracks the current election term.
	CurrentTerm *Gauge
	// IsLeader is 1.0 when this node is the cluster leader, 0.0 otherwise.
	IsLeader *Gauge

	// Cluster ISR and replication metrics (Part F1).

	// ISRSize is the current ISR size averaged across all registered partitions.
	ISRSize *Gauge
	// ClusterReplicationLag is the maximum follower lag in messages across all
	// registered partitions.
	ClusterReplicationLag *Gauge
	// UnderReplicated is the number of partitions whose ISR size is below the
	// topic's MinISR threshold.
	UnderReplicated *Gauge

	// Runtime metrics (Part C1).

	// ProcessResidentMemoryBytes reports the current resident memory usage.
	ProcessResidentMemoryBytes *Gauge
	// GoGCDurationSecondsCount counts the total number of GC cycles.
	GoGCDurationSecondsCount *Counter
	// ProcessCPUSecondsTotal reports cumulative CPU time in seconds.
	ProcessCPUSecondsTotal *Gauge

	// Live Message Explorer metrics (Phase 17).

	// ExplorerActiveSessions is the current number of active Explorer WebSocket connections.
	ExplorerActiveSessions *Gauge
	// ExplorerMessagesSentTotal counts messages successfully sent to Explorer sessions.
	ExplorerMessagesSentTotal *Counter
	// ExplorerMessagesDroppedTotal counts messages dropped due to slow Explorer consumers.
	ExplorerMessagesDroppedTotal *Counter
}

// DefaultBuckets is a standard set of histogram bucket boundaries.
var DefaultBuckets = []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000}

// NewBrokerMetrics registers and returns the standard broker metrics set.
func NewBrokerMetrics(r *Registry) *Broker {
	return &Broker{
		MessagesPublished: r.NewCounter("pubsub_messages_published_total",
			"Total messages successfully published", nil),
		MessagesConsumed: r.NewCounter("pubsub_messages_consumed_total",
			"Total messages delivered to consumers", nil),
		MessagesErrored: r.NewCounter("pubsub_messages_errored_total",
			"Total messages that failed processing", nil),
		MessagesAcked: r.NewCounter("pubsub_messages_acked_total",
			"Total ACK frames received from consumers", nil),
		MessagesNacked: r.NewCounter("pubsub_messages_nacked_total",
			"Total NACK frames received from consumers", nil),
		ActiveConnections: r.NewGauge("pubsub_active_connections",
			"Number of currently open client connections", nil),
		ConsumerLag: r.NewGauge("pubsub_consumer_lag_messages",
			"Approximate consumer lag (messages behind head)", nil),
		ReplicationLag: r.NewGauge("pubsub_replication_lag_bytes",
			"Replication lag in bytes between leader and furthest follower", nil),
		MessageSizeBytes: r.NewHistogram("pubsub_message_size_bytes",
			"Distribution of message payload sizes",
			nil, []float64{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}),
		PublishLatencyMs: r.NewHistogram("pubsub_publish_latency_milliseconds",
			"End-to-end publish latency", nil, DefaultBuckets),
		BytesPublished: r.NewCounter("pubsub_bytes_published_total",
			"Total payload bytes written by producers", nil),
		BytesConsumed: r.NewCounter("pubsub_bytes_consumed_total",
			"Total payload bytes read by consumers", nil),
		ConsumerLagTotal: r.NewGauge("pubsub_consumer_lag_total",
			"Sum of (log.NextOffset - committedOffset) across all partitions and groups", nil),
		ActiveConsumerGroups: r.NewGauge("pubsub_active_consumer_groups",
			"Number of non-empty consumer groups", nil),
		TopicCount: r.NewGauge("pubsub_topic_count",
			"Number of topics", nil),
		PartitionCount: r.NewGauge("pubsub_partition_count",
			"Total partitions across all topics", nil),
		WALBytesTotal: r.NewCounter("pubsub_wal_bytes_total",
			"Bytes appended to the message WAL", nil),
		WALEntriesTotal: r.NewCounter("pubsub_wal_entries_total",
			"Entries appended to the message WAL", nil),
		ElectionCount: r.NewCounter("pubsub_cluster_election_total",
			"Total number of elections started by this node", nil),
		LeaderChanges: r.NewCounter("pubsub_cluster_leader_changes_total",
			"Total number of leader changes observed by this node", nil),
		CurrentTerm: r.NewGauge("pubsub_cluster_current_term",
			"Current election term", nil),
		IsLeader: r.NewGauge("pubsub_cluster_is_leader",
			"1 if this node is the cluster leader, 0 otherwise", nil),
		ISRSize: r.NewGauge("pubsub_cluster_isr_size",
			"Current ISR size averaged across all registered partitions", nil),
		ClusterReplicationLag: r.NewGauge("pubsub_cluster_replication_lag_messages",
			"Maximum follower replication lag in messages across all partitions", nil),
		UnderReplicated: r.NewGauge("pubsub_cluster_under_replicated_partitions",
			"Number of partitions whose ISR size is below the topic MinISR threshold", nil),
		ProcessResidentMemoryBytes: r.NewGauge("process_resident_memory_bytes",
			"Resident memory size in bytes", nil),
		GoGCDurationSecondsCount: r.NewCounter("go_gc_duration_seconds_count",
			"Total number of GC cycles", nil),
		ProcessCPUSecondsTotal: r.NewGauge("process_cpu_seconds_total",
			"Total user and system CPU time spent in seconds", nil),
		ExplorerActiveSessions: r.NewGauge("explorer_active_sessions",
			"Number of active Explorer WebSocket connections", nil),
		ExplorerMessagesSentTotal: r.NewCounter("explorer_messages_sent_total",
			"Total messages successfully sent to Explorer sessions", nil),
		ExplorerMessagesDroppedTotal: r.NewCounter("explorer_messages_dropped_total",
			"Total messages dropped due to slow Explorer consumers", nil),
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func metricLine(name string, labels map[string]string) string {
	return name + labelStr(labels)
}

// labelStr formats a label set as {k="v",...}. Label values are escaped per
// the Prometheus text format spec: backslash, double-quote, and newline.
func labelStr(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `%s="%s"`, k, escapeLabelValue(labels[k]))
	}
	sb.WriteByte('}')
	return sb.String()
}

// escapeLabelValue escapes backslash, double-quote, and newline in label values.
func escapeLabelValue(v string) string {
	var sb strings.Builder
	sb.Grow(len(v))
	for _, r := range v {
		switch r {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func mergeLabels(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	if math.IsInf(f, -1) {
		return "-Inf"
	}
	return fmt.Sprintf("%g", f)
}
