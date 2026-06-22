package broker

import (
	"github.com/Hoot-Code/pubsub-broker/internal/cluster"
	"github.com/Hoot-Code/pubsub-broker/internal/consumer"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Status returns the current node health status.
func (b *Broker) Status() types.NodeStatus {
	return b.status.Load().(types.NodeStatus)
}

// Ready reports whether the broker has completed startup and is accepting
// connections.
func (b *Broker) Ready() bool { return b.ready.Load() }

// InFlightRequests returns the current count of requests being processed.
func (b *Broker) InFlightRequests() int64 { return b.inFlightRequests.Load() }

// HTTPAddr returns the actual bound HTTP admin server address.
func (b *Broker) HTTPAddr() string {
	v := b.httpAddr.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// Addr returns the TCP address the broker is listening on, or empty string
// if the server has not yet started.
func (b *Broker) Addr() string {
	a := b.server.Addr()
	if a == nil {
		return ""
	}
	return a.String()
}

// ConsumerDLQ exposes the broker's dead-letter queue for inspection.
func (b *Broker) ConsumerDLQ() *consumer.DeadLetterQueue {
	return b.consumers.DLQ()
}

// IsLeader reports whether this broker node is the current cluster leader.
// Always returns true in single-node mode (cluster disabled).
func (b *Broker) IsLeader() bool {
	if b.clusterNode == nil {
		return true
	}
	return b.clusterNode.IsLeader()
}

// ClusterMembers returns the current cluster membership.
// In single-node mode it returns a slice containing only this node.
func (b *Broker) ClusterMembers() []cluster.Member {
	if b.clusterNode == nil {
		cfg := b.cfg.Get()
		return []cluster.Member{{
			NodeID:   cfg.Broker.NodeID,
			Addr:     b.Addr(),
			HTTPAddr: b.HTTPAddr(),
		}}
	}
	return b.clusterNode.Members()
}

// HistoryStore returns the in-memory time-series store for /metrics/history.
func (b *Broker) HistoryStore() *metrics.HistoryStore {
	return b.historyStore
}

// ConfigPath returns the file path of the config file the broker was started
// with. Intended for testing and the PATCH /config handler.
func (b *Broker) ConfigPath() string {
	return b.cfg.Path()
}
