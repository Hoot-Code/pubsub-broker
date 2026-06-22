package broker

import (
	"encoding/json"
	"net/http"
	"time"
)

// httpConsumers implements GET /consumers — returns consumer group lag.
func (b *Broker) httpConsumers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	groups := b.consumers.ListGroups()

	type groupRow struct {
		Group     string `json:"group"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Lag       int64  `json:"lag"`
	}
	var rows []groupRow
	for _, g := range groups {
		for _, off := range g.CommittedOffsets {
			var nextOff int64
			if tp, err := b.topics.Get(g.Topic); err == nil {
				if pl, err := tp.PartitionLog(off.Partition); err == nil {
					nextOff = pl.NextOffset()
				}
			}
			lag := nextOff - off.Offset
			if lag < 0 {
				lag = 0
			}
			rows = append(rows, groupRow{
				Group:     g.Group,
				Topic:     g.Topic,
				Partition: off.Partition,
				Lag:       lag,
			})
		}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_connections": b.server.ActiveConnections(),
		"groups":             rows,
	})
}

// httpConsumerGroupDetail implements GET /consumers/{group}/{topic}.
// Returns detailed consumer group information including per-partition offsets.
func (b *Broker) httpConsumerGroupDetail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	group := r.PathValue("group")
	topic := r.PathValue("topic")

	detail := b.consumers.GetGroupDetail(group, topic, func(t string, partition int32) int64 {
		if tp, err := b.topics.Get(t); err == nil {
			if pl, err := tp.PartitionLog(partition); err == nil {
				return pl.NextOffset()
			}
		}
		return 0
	})

	if detail == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "consumer group not found"})
		return
	}

	type memberJSON struct {
		ConsumerID     string    `json:"consumer_id"`
		Partitions     []int32   `json:"partitions"`
		ConnectedSince time.Time `json:"connected_since"`
		PushMode       bool      `json:"push_mode"`
	}
	type partitionJSON struct {
		Partition       int32 `json:"partition"`
		CommittedOffset int64 `json:"committed_offset"`
		CurrentOffset   int64 `json:"current_offset"`
		Lag             int64 `json:"lag"`
	}

	members := make([]memberJSON, len(detail.Members))
	for i, m := range detail.Members {
		members[i] = memberJSON{
			ConsumerID: m.ConsumerID,
			Partitions: m.Partitions,
			PushMode:   m.PushMode,
		}
	}

	parts := make([]partitionJSON, len(detail.Partitions))
	for i, p := range detail.Partitions {
		parts[i] = partitionJSON{
			Partition:       p.Partition,
			CommittedOffset: p.CommittedOffset,
			CurrentOffset:   p.CurrentOffset,
			Lag:             p.Lag,
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"group":                detail.Group,
		"topic":                detail.Topic,
		"members":              members,
		"rebalancing":          detail.Rebalancing,
		"last_rebalance_at":    detail.LastRebalanceAt,
		"max_retries":          detail.MaxRetries,
		"retry_delay_ms":       detail.RetryDelayMs,
		"failed_message_count": detail.FailedMessageCount,
		"partitions":           parts,
	})
}
