package broker

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
)

// httpAuditRecent implements GET /audit/recent?n=100 (Part E4).
// Returns the last n audit events (default 100) as a JSON array, newest-first.
func (b *Broker) httpAuditRecent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if b.audit == nil {
		json.NewEncoder(w).Encode([]audit.Event{})
		return
	}
	n := 100
	if ns := r.URL.Query().Get("n"); ns != "" {
		if parsed, err := strconv.Atoi(ns); err == nil && parsed > 0 {
			n = parsed
		}
	}
	events := b.audit.Recent(n)
	if events == nil {
		events = []audit.Event{}
	}
	json.NewEncoder(w).Encode(events)
}
