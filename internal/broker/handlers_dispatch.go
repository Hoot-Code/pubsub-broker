package broker

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Handle processes all frames from a single client connection.
func (b *Broker) Handle(ctx context.Context, conn *networking.Conn) error {
	b.metrics.ActiveConnections.Add(1)
	defer b.metrics.ActiveConnections.Add(-1)

	cfg := b.cfg.Get()

	if !cfg.Auth.Enabled {
		conn.SetAuth("anonymous", nil)
		conn.SetIdentity(&auth.Identity{ClientID: "anonymous", Role: auth.RoleAdmin})
	}

	for {
		frame, err := conn.ReadFrame()
		if err != nil {
			if err == io.EOF || isClosedErr(err) {
				return nil
			}
			return err
		}

		b.log.Request(conn.RemoteAddr(), frame.Command.String(), len(frame.Body))

		if cfg.Auth.Enabled && !conn.IsAuthed() &&
			frame.Command != protocol.CmdAuth && frame.Command != protocol.CmdPing {
			_ = conn.SendError(frame.RequestID, string(types.ErrUnauthorized), "authentication required")
			continue
		}

		b.inFlightRequests.Add(1)
		dispatchErr := b.dispatch(ctx, conn, frame)
		b.inFlightRequests.Add(-1)

		if dispatchErr != nil {
			b.log.Error("dispatch error",
				"cmd", frame.Command,
				"remote", conn.RemoteAddr(),
				"err", dispatchErr,
			)
			_ = conn.SendError(frame.RequestID, string(types.ErrInternal), dispatchErr.Error())
		}
	}
}

// dispatch routes a single frame to the correct handler.
func (b *Broker) dispatch(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	switch f.Command {
	case protocol.CmdAuth:
		return b.handleAuth(conn, f)
	case protocol.CmdPing:
		return conn.WriteFrame(protocol.CmdPong, f.RequestID, nil)
	case protocol.CmdCreateTopic:
		return b.handleCreateTopic(conn, f)
	case protocol.CmdDeleteTopic:
		return b.handleDeleteTopic(conn, f)
	case protocol.CmdListTopics:
		return b.handleListTopics(conn, f)
	case protocol.CmdPublish:
		return b.handlePublish(ctx, conn, f)
	case protocol.CmdBatchPublish:
		return b.handleBatchPublish(ctx, conn, f)
	case protocol.CmdSubscribe:
		return b.handleSubscribe(ctx, conn, f)
	case protocol.CmdUnsubscribe:
		return b.handleUnsubscribe(conn, f)
	case protocol.CmdFetch:
		return b.handleFetch(ctx, conn, f)
	case protocol.CmdCommitOffset:
		return b.handleCommitOffset(conn, f)
	case protocol.CmdAck:
		return b.handleAck(conn, f)
	case protocol.CmdNack:
		return b.handleNack(ctx, conn, f)
	case protocol.CmdSeek:
		return b.handleSeek(conn, f)
	case protocol.CmdResetGroup:
		return b.handleResetGroup(conn, f)
	default:
		return conn.SendError(f.RequestID, "UNKNOWN_COMMAND",
			fmt.Sprintf("unknown command: %s", f.Command))
	}
}

// logAudit emits an audit event if the audit logger is configured.
// It is a no-op when b.audit is nil.
func (b *Broker) logAudit(evType audit.EventType, conn *networking.Conn,
	topic string, success bool, errMsg string) {
	if b.audit == nil {
		return
	}
	b.audit.Log(audit.Event{
		Type:       evType,
		ClientID:   conn.ClientID(),
		RemoteAddr: conn.RemoteAddr(),
		Topic:      topic,
		Success:    success,
		Error:      errMsg,
	})
}

// isClosedErr reports whether err is a closed-connection error.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe")
}

// partitionMutex returns the per-partition mutex used to serialise the
// TargetOffset-capture → pl.Append critical section in the publish handlers.
// The mutex is lazily allocated and cached in b.partLocks keyed by
// "topic/partition"; subsequent calls return the same *sync.Mutex so all
// concurrent publishers to a given partition contend on the same lock.
//
// Different partitions get different mutexes, so cross-partition concurrency
// is preserved — only same-partition publishes are serialised, which is the
// minimum required to keep the WAL TargetOffset in sync with the segment
// offset that pl.Append will actually assign.
func (b *Broker) partitionMutex(topic string, partition int32) *sync.Mutex {
	key := fmt.Sprintf("%s/%d", topic, partition)
	if v, ok := b.partLocks.Load(key); ok {
		return v.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, _ := b.partLocks.LoadOrStore(key, mu)
	return actual.(*sync.Mutex)
}
