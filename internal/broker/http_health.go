package broker

import (
	"encoding/json"
	"net/http"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// httpHealth implements GET /health and GET /status.
func (b *Broker) httpHealth(w http.ResponseWriter, r *http.Request) {
	status := b.status.Load().(types.NodeStatus)
	w.Header().Set("Content-Type", "application/json")
	switch status {
	case types.NodeActive, types.NodeDegraded:
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]string{
		"status":  string(status),
		"node_id": b.cfg.Get().Broker.NodeID,
	})
}

// httpHealthLive implements GET /healthz/live (Kubernetes liveness probe).
// Returns 200 as long as the process is running.
func (b *Broker) httpHealthLive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// httpHealthReady implements GET /healthz/ready (Kubernetes readiness probe).
// Returns 200 after Start() completes, 503 during startup.
func (b *Broker) httpHealthReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !b.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not_ready",
			"reason": "broker startup in progress",
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "ready",
		"node_id":         b.cfg.Get().Broker.NodeID,
		"cluster_enabled": b.clusterNode != nil,
	})
}

// httpHealthStartup implements GET /healthz/startup (Kubernetes startup probe).
// Returns 200 once Start() completes; never returns 503 again after that.
func (b *Broker) httpHealthStartup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !b.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// httpHealthDrain implements GET /healthz/drain.
// Returns the drain state and in-flight request count for load balancers.
func (b *Broker) httpHealthDrain(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if b.server.IsDraining() {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"draining":  true,
			"in_flight": b.inFlightRequests.Load(),
		})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{"draining": false})
	}
}
