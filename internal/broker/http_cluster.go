package broker

import (
	"encoding/json"
	"net/http"
	"time"
)

// httpClusterMembers implements GET /cluster/members.
func (b *Broker) httpClusterMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type memberRow struct {
		NodeID   string    `json:"node_id"`
		Addr     string    `json:"addr"`
		HTTPAddr string    `json:"http_addr"`
		JoinedAt time.Time `json:"joined_at"`
	}
	members := b.ClusterMembers()
	rows := make([]memberRow, len(members))
	for i, m := range members {
		rows[i] = memberRow{
			NodeID:   m.NodeID,
			Addr:     m.Addr,
			HTTPAddr: m.HTTPAddr,
			JoinedAt: m.JoinedAt,
		}
	}
	json.NewEncoder(w).Encode(rows)
}

// httpClusterPartitions implements GET /cluster/partitions.
func (b *Broker) httpClusterPartitions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if b.clusterNode == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{})
		return
	}
	pm := b.clusterNode.PartitionMap()
	data, err := pm.MarshalJSON()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

// httpClusterISR implements GET /cluster/isr — returns ISR state per partition.
func (b *Broker) httpClusterISR(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if b.clusterNode == nil {
		json.NewEncoder(w).Encode([]struct{}{})
		return
	}

	type isrEntry struct {
		Topic           string   `json:"topic"`
		Partition       int32    `json:"partition"`
		ISR             []string `json:"isr"`
		Leader          string   `json:"leader"`
		UnderReplicated bool     `json:"under_replicated"`
	}

	nodeID := b.clusterNode.SelfID()
	var leader string
	if lm, ok := b.clusterNode.Leader(); ok {
		leader = lm.NodeID
	}

	var entries []isrEntry
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
			isr := tracker.ISR(nodeID, pl.NextOffset())
			entries = append(entries, isrEntry{
				Topic:           topicName,
				Partition:       partIdx,
				ISR:             isr,
				Leader:          leader,
				UnderReplicated: len(isr) < minISR,
			})
		}
	}
	b.partTrackersMu.RUnlock()

	// Stable insertion sort by topic then partition.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0; j-- {
			a, bv := entries[j-1], entries[j]
			if a.Topic > bv.Topic || (a.Topic == bv.Topic && a.Partition > bv.Partition) {
				entries[j-1], entries[j] = entries[j], entries[j-1]
			}
		}
	}
	json.NewEncoder(w).Encode(entries)
}

// httpClusterRaft implements GET /cluster/raft.
// Returns Raft internals when Raft consensus is active; 404 otherwise.
func (b *Broker) httpClusterRaft(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if b.clusterNode == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "raft not active; consensus_algorithm is set to bully or cluster is disabled",
		})
		return
	}
	snap, ok := b.clusterNode.RaftSnapshot()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "raft not active; consensus_algorithm is set to bully or cluster is disabled",
		})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"role":         snap.Role,
		"term":         snap.Term,
		"commit_index": snap.CommitIndex,
		"last_applied": snap.LastApplied,
		"leader_id":    snap.LeaderID,
		"log_length":   snap.LogLength,
		"match_index":  snap.MatchIndex,
		"next_index":   snap.NextIndex,
	})
}
