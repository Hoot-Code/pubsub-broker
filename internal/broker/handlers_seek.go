package broker

import (
	"fmt"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// handleSeek handles CmdSeek: seeks a consumer group to a timestamp, the
// beginning, or the end of each partition in the named topic.
// Requires PermSeek and topic-level ACL (D4, D5).
func (b *Broker) handleSeek(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.SeekRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid seek body")
	}
	if req.Topic == "" || req.Group == "" {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "topic and group are required")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermSeek), req.Topic) {
		b.logAudit(audit.EventForbidden, conn, req.Topic, false, "FORBIDDEN: seek")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to seek")
	}

	tp, err := b.topics.Get(req.Topic)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}

	offsets := make(map[string]int64, tp.Config().Partitions)
	for p := 0; p < tp.Config().Partitions; p++ {
		pl, pErr := tp.PartitionLog(int32(p))
		if pErr != nil {
			continue
		}
		var newOff int64
		switch {
		case req.Offset != nil && *req.Offset >= 0:
			// Direct offset seek: store offset-1 so PollPartitionLog starts
			// at the requested offset (committed+1 logic).
			newOff = *req.Offset - 1
			if newOff < -1 {
				newOff = -1
			}
		case req.ToEnd:
			// Seek to the last existing offset so PollPartitionLog starts from
			// NextOffset — only messages published after this seek are delivered.
			next := pl.NextOffset()
			if next > 0 {
				newOff = next - 1
			}
		case req.TimestampNs == 0:
			// Seek to beginning: store -1 so PollPartitionLog starts at 0.
			newOff = -1
		default:
			found, _ := pl.OffsetForTimestamp(req.TimestampNs)
			if found > 0 {
				newOff = found - 1
			} else {
				newOff = -1
			}
		}
		b.consumers.SeekGroupOffset(req.Group, req.Topic, int32(p), newOff)
		if walErr := b.offsetWAL.Commit(req.Group, req.Topic, int32(p), newOff); walErr != nil {
			b.log.Warn("seek: offset wal commit", "err", walErr)
		}
		reportOff := newOff + 1
		if reportOff < 0 {
			reportOff = 0
		}
		offsets[fmt.Sprintf("%d", p)] = reportOff
	}

	b.logAudit(audit.EventSeek, conn, req.Topic, true, "")
	return conn.WriteFrame(protocol.CmdResponse, f.RequestID,
		&protocol.SeekResponse{Offsets: offsets})
}

// handleResetGroup handles CmdResetGroup: resets all committed offsets for a
// consumer group and topic to 0.
// Requires PermSeek and topic-level ACL (D4, D5).
func (b *Broker) handleResetGroup(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.ResetGroupRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid reset-group body")
	}
	if req.Topic == "" || req.Group == "" {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "topic and group are required")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermSeek), req.Topic) {
		b.logAudit(audit.EventForbidden, conn, req.Topic, false, "FORBIDDEN: reset_group")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to reset group")
	}

	tp, err := b.topics.Get(req.Topic)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}

	for p := 0; p < tp.Config().Partitions; p++ {
		// Store -1 so PollPartitionLog (committed+1) starts from offset 0.
		b.consumers.SeekGroupOffset(req.Group, req.Topic, int32(p), -1)
		if walErr := b.offsetWAL.Commit(req.Group, req.Topic, int32(p), -1); walErr != nil {
			b.log.Warn("reset-group: offset wal commit", "err", walErr)
		}
	}
	return conn.SendOK(f.RequestID)
}
