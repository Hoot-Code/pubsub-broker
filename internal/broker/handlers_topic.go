package broker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// handleCreateTopic handles CmdCreateTopic.
// Requires PermCreateTopic (and topic-level ACL when applicable).
func (b *Broker) handleCreateTopic(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.CreateTopicRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid create-topic body")
	}
	if req.Name == "" {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "topic name required")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermCreateTopic), req.Name) {
		b.logAudit(audit.EventForbidden, conn, req.Name, false, "FORBIDDEN: create_topic")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to create topic")
	}

	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	if req.ReplicationFactor <= 0 {
		req.ReplicationFactor = 1
	}

	err := b.topics.Create(types.TopicConfig{
		Name:              req.Name,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		RetentionHours:    req.RetentionHours,
		CompactionMode:    req.CompactionMode,
	})
	if err != nil {
		var brokerErr *types.BrokerError
		if errors.As(err, &brokerErr) {
			return conn.SendError(f.RequestID, string(brokerErr.Code), err.Error())
		}
		if strings.Contains(err.Error(), "invalid topic name") {
			return conn.SendError(f.RequestID, "BAD_REQUEST", err.Error())
		}
		return conn.SendError(f.RequestID, string(types.ErrInternal), err.Error())
	}

	// Persist to topic WAL.
	if walErr := b.topicWAL.Append(types.TopicConfig{
		Name:              req.Name,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		RetentionHours:    req.RetentionHours,
		CompactionMode:    req.CompactionMode,
	}); walErr != nil {
		b.log.Warn("topic wal append error", "topic", req.Name, "err", walErr)
	}

	// Attach ISR trackers and register with cluster replicator.
	if b.clusterNode != nil {
		tp, tErr := b.topics.Get(req.Name)
		if tErr == nil {
			for p := 0; p < req.Partitions; p++ {
				pl, pErr := tp.PartitionLog(int32(p))
				if pErr == nil {
					b.wirePartition(req.Name, int32(p), pl)
				}
			}
		}
		if b.clusterNode.IsLeader() {
			b.clusterNode.ReassignAll()
		}
	}

	b.logAudit(audit.EventCreateTopic, conn, req.Name, true, "")
	return conn.SendOK(f.RequestID)
}

// handleDeleteTopic handles CmdDeleteTopic.
// Requires PermDeleteTopic (and topic-level ACL when applicable).
func (b *Broker) handleDeleteTopic(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.DeleteTopicRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid delete-topic body")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermDeleteTopic), req.Name) {
		b.logAudit(audit.EventForbidden, conn, req.Name, false, "FORBIDDEN: delete_topic")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to delete topic")
	}

	if err := b.topics.Delete(req.Name); err != nil {
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}

	// Persist to topic WAL.
	if walErr := b.topicWAL.Delete(req.Name); walErr != nil {
		b.log.Warn("topic wal delete error", "topic", req.Name, "err", walErr)
	}

	b.logAudit(audit.EventDeleteTopic, conn, req.Name, true, "")
	return conn.SendOK(f.RequestID)
}

// handleListTopics handles CmdListTopics.
// Requires PermListTopics; no topic-level ACL (returns only visible topics).
func (b *Broker) handleListTopics(conn *networking.Conn, f *protocol.Frame) error {
	// RBAC gate (D4).
	if !conn.Can(string(auth.PermListTopics), "") {
		b.logAudit(audit.EventForbidden, conn, "", false, "FORBIDDEN: list_topics")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to list topics")
	}

	list := b.topics.List()
	type topicInfo struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		Created    int64  `json:"created_at"`
	}
	infos := make([]topicInfo, len(list))
	for i, m := range list {
		infos[i] = topicInfo{
			Name:       m.Config.Name,
			Partitions: m.Config.Partitions,
			Created:    m.CreatedAt.UnixNano(),
		}
	}
	return conn.WriteFrame(protocol.CmdResponse, f.RequestID, infos)
}

// Silence unused import when strings/errors are only used conditionally.
var _ = fmt.Sprintf
