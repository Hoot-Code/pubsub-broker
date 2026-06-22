package broker

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
)

// httpMetrics implements GET /metrics in Prometheus text format.
func (b *Broker) httpMetrics(w http.ResponseWriter, r *http.Request) {
	b.updateDynamicMetrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	b.metricsReg.Expose(w)
}

// updateDynamicMetrics recalculates gauge metrics that must be computed on demand.
func (b *Broker) updateDynamicMetrics() {
	topics := b.topics.List()
	b.metrics.TopicCount.Set(float64(len(topics)))
	var totalParts int
	for _, t := range topics {
		totalParts += t.Config.Partitions
	}
	b.metrics.PartitionCount.Set(float64(totalParts))

	groups := b.consumers.ListGroups()
	var lagTotal float64
	for _, g := range groups {
		for _, off := range g.CommittedOffsets {
			tp, err := b.topics.Get(g.Topic)
			if err != nil {
				continue
			}
			pl, err := tp.PartitionLog(off.Partition)
			if err != nil {
				continue
			}
			if next := pl.NextOffset(); next > off.Offset {
				lagTotal += float64(next - off.Offset)
			}
		}
	}
	b.metrics.ConsumerLagTotal.Set(lagTotal)
	b.metrics.ActiveConsumerGroups.Set(float64(len(groups)))

	// Runtime metrics (Part C2).
	b.metrics.ProcessResidentMemoryBytes.Set(readResidentMemory())
	b.metrics.GoGCDurationSecondsCount.Inc(readGCCountDelta())
	b.metrics.ProcessCPUSecondsTotal.Set(readCPUTimeDelta())

	// Explorer metrics (Phase 17) — gauge is updated on demand.
	b.metrics.ExplorerActiveSessions.Set(float64(b.explorerHub.ActiveSessions()))
	if cur := b.explorerHub.SentTotal(); cur > b.explorerPrevSent {
		b.metrics.ExplorerMessagesSentTotal.Inc(cur - b.explorerPrevSent)
		b.explorerPrevSent = cur
	}
	if cur := b.explorerHub.DroppedTotal(); cur > b.explorerPrevDropped {
		b.metrics.ExplorerMessagesDroppedTotal.Inc(cur - b.explorerPrevDropped)
		b.explorerPrevDropped = cur
	}

	// Cluster ISR and replication-lag metrics.
	if b.clusterNode != nil {
		nodeID := b.clusterNode.SelfID()
		var isrTotal float64
		var isrCount int
		var maxLag int64
		var underReplicated int

		b.partTrackersMu.RLock()
		for topicName, parts := range b.partTrackers {
			tp, tErr := b.topics.Get(topicName)
			if tErr != nil {
				continue
			}
			minISR := tp.Config().MinISR
			if minISR < 1 {
				minISR = 1
			}
			for partIdx, tracker := range parts {
				pl, pErr := tp.PartitionLog(partIdx)
				if pErr != nil {
					continue
				}
				leaderOffset := pl.NextOffset()
				isr := tracker.ISR(nodeID, leaderOffset)
				isrTotal += float64(len(isr))
				isrCount++
				for _, st := range tracker.Snapshot() {
					if lag := leaderOffset - st.LastOffset; lag > maxLag {
						maxLag = lag
					}
				}
				if len(isr) < minISR {
					underReplicated++
				}
			}
		}
		b.partTrackersMu.RUnlock()

		if isrCount > 0 {
			b.metrics.ISRSize.Set(isrTotal / float64(isrCount))
		} else {
			b.metrics.ISRSize.Set(0)
		}
		b.metrics.ClusterReplicationLag.Set(float64(maxLag))
		b.metrics.UnderReplicated.Set(float64(underReplicated))
	}
}

// httpMetricsHistory implements GET /metrics/history?range=5m|15m|1h|24h.
// Returns time-series data for dashboard charting.
func (b *Broker) httpMetricsHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "15m"
	}

	var since time.Duration
	switch rangeStr {
	case "5m":
		since = 5 * time.Minute
	case "15m":
		since = 15 * time.Minute
	case "1h":
		since = 1 * time.Hour
	case "24h":
		since = 24 * time.Hour
	default:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "invalid range value; must be one of: 5m, 15m, 1h, 24h",
		})
		return
	}

	if b.historyStore == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"range":            rangeStr,
			"interval_seconds": 10,
			"series":           map[string]interface{}{},
		})
		return
	}

	data := b.historyStore.Range(time.Now().Add(-since))
	type point struct {
		T string  `json:"t"`
		V float64 `json:"v"`
	}
	series := make(map[string][]point, len(data))
	for name, samples := range data {
		points := make([]point, len(samples))
		for i, s := range samples {
			points[i] = point{T: s.Timestamp.Format(time.RFC3339), V: s.Value}
		}
		series[name] = points
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"range":            rangeStr,
		"interval_seconds": 10,
		"series":           series,
	})
}

// httpTraces implements GET /traces — streams the last 1000 completed spans.
func (b *Broker) httpTraces(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	exp := tracing.NewJSONExporter(w)
	for _, sp := range b.spanStore.Snapshot() {
		_ = exp.Export(sp)
	}
}
