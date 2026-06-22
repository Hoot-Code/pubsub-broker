// Package cluster implements cluster membership, leader election, and
// partition ownership for multi-node broker deployments.
// All cluster code paths are disabled when ClusterConfig.Enabled is false.
package cluster

import "time"

// Member represents a single node in the cluster.
type Member struct {
	// NodeID is the unique, stable identifier for the node.
	NodeID string `json:"node_id"`
	// Addr is the host:port used for inter-node TCP communication.
	Addr string `json:"addr"`
	// HTTPAddr is the host:port used for admin/metrics HTTP.
	HTTPAddr string `json:"http_addr"`
	// JoinedAt is the time at which the node joined the cluster.
	JoinedAt time.Time `json:"joined_at"`
}
