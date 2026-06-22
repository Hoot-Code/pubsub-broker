// Package client - admin.go provides topic management and utility operations.
package client

import (
	"context"
	"fmt"
	"time"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Topic management ─────────────────────────────────────────────────────────

// CreateTopic creates a new topic with the given configuration.
// Returns ErrTopicExists if the topic already exists.
func (c *Client) CreateTopic(ctx context.Context, cfg types.TopicConfig) error {
	req := &proto.CreateTopicRequest{
		Name:              cfg.Name,
		Partitions:        cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		RetentionHours:    cfg.RetentionHours,
		CompactionMode:    cfg.CompactionMode,
	}
	_, err := c.sendRecv(ctx, proto.CmdCreateTopic, req)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}

// DeleteTopic deletes the named topic.
// Returns ErrTopicNotFound if the topic does not exist.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	req := &proto.DeleteTopicRequest{Name: name}
	_, err := c.sendRecv(ctx, proto.CmdDeleteTopic, req)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}
	return nil
}

// TopicInfo holds the details returned by ListTopics.
type TopicInfo struct {
	// Name is the topic name.
	Name string `json:"name"`
	// Partitions is the partition count for this topic.
	Partitions int `json:"partitions"`
	// CreatedAt is the topic creation time (zero if unavailable).
	CreatedAt time.Time `json:"created_at"`
}

// ListTopics returns all topics known to the broker.
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	f, err := c.sendRecv(ctx, proto.CmdListTopics, nil)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	// Broker sends created_at as UnixNano int64.
	var raw []struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		CreatedAt  int64  `json:"created_at"`
	}
	if err := proto.Unmarshal(f, &raw); err != nil {
		return nil, fmt.Errorf("list topics: decode: %w", err)
	}
	out := make([]TopicInfo, len(raw))
	for i, r := range raw {
		out[i] = TopicInfo{
			Name:       r.Name,
			Partitions: r.Partitions,
			CreatedAt:  time.Unix(0, r.CreatedAt),
		}
	}
	return out, nil
}

// ─── Direct fetch ────────────────────────────────────────────────────────────

// Fetch performs a stateless, direct read of up to limit messages from
// topic/partition starting at offset. Unlike a Consumer, this does not track
// or commit any consumer-group position — it is intended for one-shot reads
// (e.g. the HTTP gateway's REST fetch endpoint). limit <= 0 defaults to 100
// (capped at 1000 by the broker).
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, limit int) ([]*types.Message, error) {
	req := &proto.FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxCount:  limit,
	}
	f, err := c.sendRecv(ctx, proto.CmdFetch, req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	var resp proto.FetchResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return nil, fmt.Errorf("fetch: decode response: %w", err)
	}
	return resp.Messages, nil
}

// PingResult holds the latency measurement from a Ping call.
type PingResult struct {
	// RTT is the round-trip time measured for the Ping/Pong exchange.
	RTT time.Duration
}

// Ping sends a CmdPing to the broker and returns the round-trip time.
func (c *Client) Ping(ctx context.Context) (*PingResult, error) {
	start := time.Now()
	_, err := c.sendRecv(ctx, proto.CmdPing, nil)
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &PingResult{RTT: time.Since(start)}, nil
}
