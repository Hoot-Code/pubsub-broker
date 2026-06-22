package broker

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/cluster"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Silence unused cluster import when cluster is disabled.
var _ cluster.Member

// httpTopics implements GET /topics.
func (b *Broker) httpTopics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type topicRow struct {
		Config           types.TopicConfig `json:"Config"`
		CreatedAt        time.Time         `json:"CreatedAt"`
		MessageCount     int64             `json:"MessageCount"`
		StorageSizeBytes int64             `json:"StorageSizeBytes"`
	}
	topics := b.topics.List()
	rows := make([]topicRow, len(topics))
	for i, t := range topics {
		var msgCount int64
		var storageBytes int64
		for p := int32(0); p < int32(t.Config.Partitions); p++ {
			if tp, err := b.topics.Get(t.Config.Name); err == nil {
				if pl, err := tp.PartitionLog(p); err == nil {
					msgCount += pl.NextOffset()
					storageBytes += pl.TotalLogSize()
				}
			}
		}
		rows[i] = topicRow{
			Config:           t.Config,
			CreatedAt:        t.CreatedAt,
			MessageCount:     msgCount,
			StorageSizeBytes: storageBytes,
		}
	}
	json.NewEncoder(w).Encode(rows)
}

// httpPartitionDetail implements GET /topics/{topic}/partitions/{partition}.
// Returns detailed information about a single partition.
func (b *Broker) httpPartitionDetail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	topic := r.PathValue("topic")
	partStr := r.PathValue("partition")
	part, err := strconv.ParseInt(partStr, 10, 32)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid partition number"})
		return
	}

	tp, err := b.topics.Get(topic)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	pl, err := tp.PartitionLog(int32(part))
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	nodeID := b.cfg.Get().Broker.NodeID
	if b.clusterNode != nil {
		nodeID = b.clusterNode.SelfID()
	}

	walStatus := "synced"
	walPending := b.pendingWALBytes.Load()
	if walPending > 0 {
		walStatus = "pending"
	}

	isr := []string{nodeID}
	replicas := []string{nodeID}
	underReplicated := false
	if b.clusterNode != nil {
		tracker := b.getISRTracker(topic, int32(part))
		if tracker != nil {
			isr = tracker.ISR(nodeID, pl.NextOffset())
			snap := tracker.Snapshot()
			replicaSet := map[string]bool{nodeID: true}
			for _, s := range snap {
				replicaSet[s.NodeID] = true
			}
			replicas = make([]string, 0, len(replicaSet))
			for id := range replicaSet {
				replicas = append(replicas, id)
			}
			tpMeta, tErr := b.topics.Get(topic)
			if tErr == nil {
				minISR := tpMeta.Config().MinISR
				if minISR < 1 {
					minISR = 1
				}
				underReplicated = len(isr) < minISR
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"topic":                topic,
		"partition":            int32(part),
		"leader_node_id":       nodeID,
		"wal_status":           walStatus,
		"wal_pending_bytes":    walPending,
		"isr":                  isr,
		"replicas":             replicas,
		"under_replicated":     underReplicated,
		"segment_count":        pl.SegmentCount(),
		"segment_total_bytes":  pl.TotalLogSize(),
		"active_segment_bytes": pl.ActiveSegmentBytes(),
	})
}

// httpPartitionList implements GET /topics/{topic}/partitions.
// Returns partition details for every partition in the topic.
func (b *Broker) httpPartitionList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	topic := r.PathValue("topic")

	tp, err := b.topics.Get(topic)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	nodeID := b.cfg.Get().Broker.NodeID
	if b.clusterNode != nil {
		nodeID = b.clusterNode.SelfID()
	}

	cfg := tp.Config()
	var partitions []map[string]interface{}
	for p := 0; p < cfg.Partitions; p++ {
		pl, pErr := tp.PartitionLog(int32(p))
		if pErr != nil {
			continue
		}

		walStatus := "synced"
		walPending := b.pendingWALBytes.Load()
		if walPending > 0 {
			walStatus = "pending"
		}

		isr := []string{nodeID}
		replicas := []string{nodeID}
		underReplicated := false
		if b.clusterNode != nil {
			tracker := b.getISRTracker(topic, int32(p))
			if tracker != nil {
				isr = tracker.ISR(nodeID, pl.NextOffset())
				snap := tracker.Snapshot()
				replicaSet := map[string]bool{nodeID: true}
				for _, s := range snap {
					replicaSet[s.NodeID] = true
				}
				replicas = make([]string, 0, len(replicaSet))
				for id := range replicaSet {
					replicas = append(replicas, id)
				}
				minISR := cfg.MinISR
				if minISR < 1 {
					minISR = 1
				}
				underReplicated = len(isr) < minISR
			}
		}

		partitions = append(partitions, map[string]interface{}{
			"topic":                topic,
			"partition":            int32(p),
			"leader_node_id":       nodeID,
			"wal_status":           walStatus,
			"wal_pending_bytes":    walPending,
			"isr":                  isr,
			"replicas":             replicas,
			"under_replicated":     underReplicated,
			"segment_count":        pl.SegmentCount(),
			"segment_total_bytes":  pl.TotalLogSize(),
			"active_segment_bytes": pl.ActiveSegmentBytes(),
		})
	}
	json.NewEncoder(w).Encode(partitions)
}
