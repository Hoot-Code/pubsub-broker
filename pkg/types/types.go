// Package types defines the core domain types shared across the broker.
package types

import (
	"crypto/rand"
	"fmt"
	"time"
)

// ─── Delivery Guarantees ────────────────────────────────────────────────────

// DeliveryMode controls message delivery semantics.
type DeliveryMode uint8

const (
	AtMostOnce  DeliveryMode = iota // fire-and-forget
	AtLeastOnce                     // ack-based retry
	ExactlyOnce                     // future: idempotent producer + txn
)

// String returns a human-readable delivery mode name.
func (d DeliveryMode) String() string {
	switch d {
	case AtMostOnce:
		return "at-most-once"
	case AtLeastOnce:
		return "at-least-once"
	case ExactlyOnce:
		return "exactly-once"
	default:
		return "unknown"
	}
}

// ─── Message ─────────────────────────────────────────────────────────────────

// Message is the fundamental unit of data in the broker.
type Message struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Timestamp int64             `json:"timestamp"` // Unix nanoseconds
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
	Key       string            `json:"key,omitempty"` // used for partition routing
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset,omitempty"`
	// Codec identifies the compression algorithm applied to Payload (B2).
	Codec uint8 `json:"codec,omitempty"`
}

// NewMessage creates a new Message with a unique ID and current timestamp.
func NewMessage(topic string, payload []byte, key string, headers map[string]string) *Message {
	return &Message{
		ID:        NewUUID(),
		Topic:     topic,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
		Headers:   headers,
		Key:       key,
	}
}

// ─── Topic ───────────────────────────────────────────────────────────────────

// TopicConfig defines the configuration for a topic.
type TopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	RetentionHours    int
	RetentionBytes    int64
	DeliveryMode      DeliveryMode
	// CompressionCodec is the default codec for messages published to this topic (B3).
	CompressionCodec uint8
	// MinISR is the minimum number of in-sync replicas required for this topic
	// to accept writes.  A value ≤ 1 is treated as 1 (single-node fast path).
	MinISR int `json:"min_isr,omitempty"`
	// CompactionMode selects the retention strategy for this topic.
	//   "" or "delete" = existing age/size retention (default, unchanged).
	//   "compact"      = key-based compaction; only the latest message per
	//                    key is retained. Age/size retention is skipped for
	//                    compacted topics; compaction runs on its own
	//                    schedule instead (see storage.KeyCompactor).
	CompactionMode string `json:"compaction_mode,omitempty"`
}

// TopicMetadata holds runtime metadata about a topic.
type TopicMetadata struct {
	Config           TopicConfig
	CreatedAt        time.Time
	PartitionLeaders map[int32]string // partitionID → nodeID
}

// ─── Consumer ────────────────────────────────────────────────────────────────

// ConsumerGroupOffset tracks a consumer group's read position.
type ConsumerGroupOffset struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	UpdatedAt time.Time
}

// ─── Node ────────────────────────────────────────────────────────────────────

// NodeStatus represents the health of a broker node.
type NodeStatus string

const (
	NodeActive    NodeStatus = "ACTIVE"
	NodeDegraded  NodeStatus = "DEGRADED"
	NodeUnhealthy NodeStatus = "UNHEALTHY"
)

// NodeInfo describes a broker node.
type NodeInfo struct {
	ID       string
	Address  string
	Port     int
	Status   NodeStatus
	IsLeader bool
}

// ─── Permissions ─────────────────────────────────────────────────────────────

// Permission represents an operation a client may perform.
type Permission string

const (
	PermPublish   Permission = "publish"
	PermSubscribe Permission = "subscribe"
	PermAdmin     Permission = "admin"
)

// ─── Errors ──────────────────────────────────────────────────────────────────

// BrokerError is a typed error returned by broker operations.
type BrokerError struct {
	Code    ErrorCode
	Message string
}

// Error implements the error interface.
func (e *BrokerError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// ErrorCode is a machine-readable error category.
type ErrorCode string

const (
	ErrTopicNotFound     ErrorCode = "TOPIC_NOT_FOUND"
	ErrTopicExists       ErrorCode = "TOPIC_EXISTS"
	ErrUnauthorized      ErrorCode = "UNAUTHORIZED"
	ErrInvalidMessage    ErrorCode = "INVALID_MESSAGE"
	ErrPartitionNotFound ErrorCode = "PARTITION_NOT_FOUND"
	ErrBrokerOverloaded  ErrorCode = "BROKER_OVERLOADED"
	ErrInternal          ErrorCode = "INTERNAL_ERROR"
	ErrRetryExceeded     ErrorCode = "RETRY_EXCEEDED"
)

// NewBrokerError constructs a BrokerError.
func NewBrokerError(code ErrorCode, msg string) *BrokerError {
	return &BrokerError{Code: code, Message: msg}
}

// ─── UUID ─────────────────────────────────────────────────────────────────────

// NewUUID generates a cryptographically random UUID v4.
func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
