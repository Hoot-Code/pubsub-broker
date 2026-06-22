package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// httpDLQ handles GET /dlq and DELETE /dlq.
//
// GET /dlq?group=<g>&topic=<t>&limit=<n> returns DLQ entries as JSON.
// DELETE /dlq?group=<g>&topic=<t> purges matching entries.
func (b *Broker) httpDLQ(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	group := r.URL.Query().Get("group")
	topic := r.URL.Query().Get("topic")

	switch r.Method {
	case http.MethodGet:
		limit := 100
		if ls := r.URL.Query().Get("limit"); ls != "" {
			if parsed, err := strconv.Atoi(ls); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		entries := b.consumers.DLQEntries(group, topic, limit)
		if len(entries) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no DLQ entries found"})
			return
		}
		type dlqRow struct {
			ID         string `json:"id"`
			Topic      string `json:"topic"`
			Partition  int32  `json:"partition"`
			Offset     int64  `json:"offset"`
			Key        string `json:"key"`
			Attempts   int    `json:"attempts"`
			EnqueuedAt string `json:"enqueued_at"`
			PayloadB64 string `json:"payload_b64"`
		}
		out := make([]dlqRow, len(entries))
		for i, e := range entries {
			out[i] = dlqRow{
				ID:         e.Original.ID,
				Topic:      e.Original.Topic,
				Partition:  e.Original.Partition,
				Offset:     e.Original.Offset,
				Key:        e.Original.Key,
				Attempts:   e.Attempts,
				EnqueuedAt: e.ArrivedAt.Format(time.RFC3339),
				PayloadB64: base64.StdEncoding.EncodeToString(e.Original.Payload),
			}
		}
		json.NewEncoder(w).Encode(out)

	case http.MethodDelete:
		purged := b.consumers.DLQPurge(group, topic)
		json.NewEncoder(w).Encode(map[string]interface{}{"purged": purged})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

// httpDLQReplay handles POST /dlq/replay?group=<g>&topic=<t>&limit=<n>.
// Re-publishes up to limit DLQ entries back to their original topic.
func (b *Broker) httpDLQReplay(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	group := r.URL.Query().Get("group")
	topic := r.URL.Query().Get("topic")
	limit := 10
	if ls := r.URL.Query().Get("limit"); ls != "" {
		if parsed, err := strconv.Atoi(ls); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	replayed, err := b.consumers.DLQReplay(group, topic, limit, func(msg *types.Message) error {
		_, pubErr := b.producers.Publish(
			r.Context(), msg.Topic, msg.Key, msg.Payload, msg.Headers,
			types.DeliveryMode(msg.Codec), 0, 0,
		)
		return pubErr
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	remaining := b.consumers.DLQEntries(group, topic, 0)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"replayed":  replayed,
		"remaining": len(remaining),
	})
}

// httpDLQDelete implements DELETE /dlq/{id}?group=<g>&topic=<t>.
// Removes a single DLQ entry by its ID.
func (b *Broker) httpDLQDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	id := r.PathValue("id")
	group := r.URL.Query().Get("group")
	topic := r.URL.Query().Get("topic")

	if !b.consumers.DLQDelete(group, topic, id) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "DLQ entry not found"})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"deleted": true})
}

// httpDLQExport implements GET /dlq/{id}/export?group=<g>&topic=<t>.
// Returns the single DLQ entry as a downloadable JSON file.
func (b *Broker) httpDLQExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	id := r.PathValue("id")
	group := r.URL.Query().Get("group")
	topic := r.URL.Query().Get("topic")

	entry := b.consumers.DLQGet(group, topic, id)
	if entry == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "DLQ entry not found"})
		return
	}

	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="dlq-%s.json"`, id))

	type dlqExport struct {
		ID         string `json:"id"`
		Topic      string `json:"topic"`
		Partition  int32  `json:"partition"`
		Offset     int64  `json:"offset"`
		Key        string `json:"key"`
		Group      string `json:"group"`
		Reason     string `json:"reason"`
		Attempts   int    `json:"attempts"`
		EnqueuedAt string `json:"enqueued_at"`
		PayloadB64 string `json:"payload_b64"`
	}
	out := dlqExport{
		ID:         entry.Original.ID,
		Topic:      entry.Original.Topic,
		Partition:  entry.Original.Partition,
		Offset:     entry.Original.Offset,
		Key:        entry.Original.Key,
		Group:      entry.Group,
		Reason:     entry.Reason,
		Attempts:   entry.Attempts,
		EnqueuedAt: entry.ArrivedAt.Format(time.RFC3339),
		PayloadB64: base64.StdEncoding.EncodeToString(entry.Original.Payload),
	}
	json.NewEncoder(w).Encode(out)
}
