// Package consumer implements consumer and consumer group management.
package consumer

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Sink is implemented by connections that support server-push delivery.
// When a Consumer has a non-nil Sink, Dispatch routes messages through the
// Sink instead of the internal msgCh channel.
type Sink interface {
	// WriteFrame encodes and sends a protocol frame to the remote client.
	WriteFrame(cmd protocol.Command, reqID uint64, body interface{}) error
}

// validGroupName rejects group names that contain "/" (which would create
// a collision in the groupKey "group/topic" format) and enforces a
// reasonable character set. A group name "a/b" on topic "c" would previously
// collide with group "a" on topic "b/c"; this regex prevents both forms.
var validGroupName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`)

// ─── Consumer ─────────────────────────────────────────────────────────────────

// Consumer represents a single subscriber.
type Consumer struct {
	ID       string
	Group    string
	Topic    string
	ClientID string

	mu          sync.Mutex
	assignments []int32 // partition IDs assigned to this consumer
	msgCh       chan *types.Message
	ctx         context.Context
	cancel      context.CancelFunc

	// Push-delivery fields (Part C).
	sinkMu sync.RWMutex
	sink   Sink                // non-nil when in push mode
	pushCh chan *types.Message // buffered channel for push goroutine; nil in pull mode
}

// Messages returns the channel on which this consumer receives messages.
func (c *Consumer) Messages() <-chan *types.Message { return c.msgCh }

// Assignments returns the partitions assigned to this consumer.
func (c *Consumer) Assignments() []int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int32, len(c.assignments))
	copy(out, c.assignments)
	return out
}

// SetSink sets (or clears) the push sink for this consumer.
// When s is non-nil, Dispatch routes messages to the push channel instead of
// msgCh. The push goroutine (managed by the broker) reads from PushMessages().
// When s is nil the push channel is kept alive until the consumer is closed;
// the goroutine detects this via Done().
func (c *Consumer) SetSink(s Sink) {
	c.sinkMu.Lock()
	defer c.sinkMu.Unlock()
	c.sink = s
	if s != nil && c.pushCh == nil {
		c.pushCh = make(chan *types.Message, 512)
	}
}

// GetSink returns the current push sink, or nil for pull-mode consumers.
func (c *Consumer) GetSink() Sink {
	c.sinkMu.RLock()
	defer c.sinkMu.RUnlock()
	return c.sink
}

// PushMessages returns the channel from which the push goroutine reads.
// Returns nil for pull-mode consumers.
func (c *Consumer) PushMessages() <-chan *types.Message {
	c.sinkMu.RLock()
	defer c.sinkMu.RUnlock()
	return c.pushCh
}

// Done returns a channel that is closed when this consumer is cancelled.
func (c *Consumer) Done() <-chan struct{} { return c.ctx.Done() }

func (c *Consumer) assign(partitions []int32) {
	c.mu.Lock()
	c.assignments = partitions
	c.mu.Unlock()
}

// deliver sends msg to the consumer.
// If a push sink is set, the message is sent to pushCh (non-blocking).
// Otherwise it falls through to the pull-mode msgCh (also non-blocking).
// Returns false on backpressure (channel full).
func (c *Consumer) deliver(msg *types.Message) bool {
	c.sinkMu.RLock()
	pushCh := c.pushCh
	c.sinkMu.RUnlock()

	if pushCh != nil {
		select {
		case pushCh <- msg:
			return true
		default:
			return false
		}
	}
	select {
	case c.msgCh <- msg:
		return true
	default:
		return false
	}
}

func (c *Consumer) close() {
	c.cancel()
	close(c.msgCh)
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

// DLQEntry wraps a message that has exhausted retries.
type DLQEntry struct {
	Original  *types.Message
	Group     string
	Reason    string
	Attempts  int
	ArrivedAt time.Time
}

// DeadLetterQueue holds messages that could not be delivered.
type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DLQEntry
	maxSize int
}

// NewDLQ creates a DLQ with the given maximum size.
func NewDLQ(maxSize int) *DeadLetterQueue {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	return &DeadLetterQueue{maxSize: maxSize}
}

// Push adds an entry to the DLQ. If the DLQ is full, the oldest entry is
// discarded (size-bounded; no OOM risk).
func (q *DeadLetterQueue) Push(entry DLQEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= q.maxSize {
		q.entries = q.entries[1:] // drop oldest
	}
	entry.ArrivedAt = time.Now()
	q.entries = append(q.entries, entry)
}

// Drain returns and removes all current DLQ entries.
func (q *DeadLetterQueue) Drain() []DLQEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.entries
	q.entries = nil
	return out
}

// Len returns the number of entries in the DLQ.
func (q *DeadLetterQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Entries returns up to limit DLQ entries matching group and topic.
// When group is empty, all groups are returned; same for topic.
func (q *DeadLetterQueue) Entries(group, topic string, limit int) []DLQEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	var out []DLQEntry
	for i := len(q.entries) - 1; i >= 0 && len(out) < limit; i-- {
		e := q.entries[i]
		if group != "" && e.Group != group {
			continue
		}
		if topic != "" && e.Original != nil && e.Original.Topic != topic {
			continue
		}
		out = append(out, e)
	}
	return out
}

// Replay removes up to limit entries matching group+topic and returns them.
// The caller is responsible for re-publishing the returned messages.
func (q *DeadLetterQueue) Replay(group, topic string, limit int) []DLQEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit <= 0 {
		limit = 10
	}
	var removed []DLQEntry
	remaining := q.entries[:0]
	for _, e := range q.entries {
		matched := len(removed) < limit
		if matched && group != "" && e.Group != group {
			matched = false
		}
		if matched && topic != "" && e.Original != nil && e.Original.Topic != topic {
			matched = false
		}
		if matched {
			removed = append(removed, e)
		} else {
			remaining = append(remaining, e)
		}
	}
	q.entries = remaining
	return removed
}

// Purge removes all entries matching group+topic and returns the count removed.
func (q *DeadLetterQueue) Purge(group, topic string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	var purged int
	remaining := q.entries[:0]
	for _, e := range q.entries {
		match := true
		if group != "" && e.Group != group {
			match = false
		}
		if match && topic != "" && e.Original != nil && e.Original.Topic != topic {
			match = false
		}
		if match {
			purged++
		} else {
			remaining = append(remaining, e)
		}
	}
	q.entries = remaining
	return purged
}

// Get returns the first DLQ entry matching group+topic+id, or nil if not found.
func (q *DeadLetterQueue) Get(group, topic, id string) *DLQEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := len(q.entries) - 1; i >= 0; i-- {
		e := &q.entries[i]
		if id != "" && e.Original != nil && e.Original.ID != id {
			continue
		}
		if group != "" && e.Group != group {
			continue
		}
		if topic != "" && e.Original != nil && e.Original.Topic != topic {
			continue
		}
		return e
	}
	return nil
}

// Delete removes the first DLQ entry matching group+topic+id.
// Returns true if an entry was found and removed.
func (q *DeadLetterQueue) Delete(group, topic, id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := len(q.entries) - 1; i >= 0; i-- {
		e := q.entries[i]
		if id != "" && e.Original != nil && e.Original.ID != id {
			continue
		}
		if group != "" && e.Group != group {
			continue
		}
		if topic != "" && e.Original != nil && e.Original.Topic != topic {
			continue
		}
		q.entries = append(q.entries[:i], q.entries[i+1:]...)
		return true
	}
	return false
}

// ─── Group ────────────────────────────────────────────────────────────────────

// Group is a named consumer group subscribed to one topic.
type Group struct {
	mu         sync.RWMutex
	id         string
	topic      string
	consumers  map[string]*Consumer // consumerID → Consumer
	offsets    *partition.OffsetStore
	partCount  int32
	dlq        *DeadLetterQueue
	maxRetries int
	retryDelay time.Duration

	// Rebalancing is true while a rebalance is in progress. Set true at the
	// start of rebalance(), false at the end, so the dashboard can display a
	// transient "rebalancing" indicator.
	Rebalancing bool
	// LastRebalanceAt is the time the most recent rebalance completed.
	LastRebalanceAt time.Time
}

// NewGroup creates a consumer group.
func NewGroup(id, topic string, partCount int32, offsets *partition.OffsetStore, dlq *DeadLetterQueue, maxRetries int, retryDelay time.Duration) *Group {
	return &Group{
		id:         id,
		topic:      topic,
		consumers:  make(map[string]*Consumer),
		offsets:    offsets,
		partCount:  partCount,
		dlq:        dlq,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// Join adds a consumer to the group and triggers rebalancing.
// Returns the consumer (caller must read Consumer.Messages()).
func (g *Group) Join(consumerID, clientID string, bufferSize int) (*Consumer, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.consumers[consumerID]; ok {
		return nil, fmt.Errorf("consumer %q already in group %q", consumerID, g.id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		ID:       consumerID,
		Group:    g.id,
		Topic:    g.topic,
		ClientID: clientID,
		msgCh:    make(chan *types.Message, bufferSize),
		ctx:      ctx,
		cancel:   cancel,
	}
	g.consumers[consumerID] = c
	g.rebalance()
	return c, nil
}

// Leave removes a consumer from the group and triggers rebalancing.
func (g *Group) Leave(consumerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.consumers[consumerID]
	if !ok {
		return
	}
	c.close()
	delete(g.consumers, consumerID)
	g.rebalance()
}

// Dispatch sends a message to the consumer assigned to its partition.
// The group lock is released before calling handleSlowConsumer so that
// retry sleeps do not block rebalancing or other dispatches.
func (g *Group) Dispatch(msg *types.Message) {
	g.mu.RLock()
	var slowC *Consumer
	for _, c := range g.consumers {
		for _, p := range c.Assignments() {
			if p == msg.Partition {
				if !c.deliver(msg) {
					slowC = c
				}
				break
			}
		}
		if slowC != nil {
			break
		}
	}
	g.mu.RUnlock()

	// handleSlowConsumer is called without the group lock so that retry sleeps
	// do not stall rebalancing or concurrent dispatches.
	if slowC != nil {
		g.handleSlowConsumer(slowC, msg)
	}
}

// CommitOffset records that consumerID has processed up to offset on partition.
func (g *Group) CommitOffset(consumerID string, partID int32, offset int64) error {
	g.mu.RLock()
	_, ok := g.consumers[consumerID]
	g.mu.RUnlock()
	if !ok {
		return fmt.Errorf("consumer %q not in group %q", consumerID, g.id)
	}
	g.offsets.Commit(g.id, g.topic, partID, offset)
	return nil
}

// Offset returns the committed offset for this group on partition partID.
func (g *Group) Offset(partID int32) int64 {
	return g.offsets.Load(g.id, g.topic, partID)
}

// ConsumerCount returns the number of active consumers.
func (g *Group) ConsumerCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.consumers)
}

// rebalance assigns partitions round-robin across all consumers in sorted
// order. Sorting makes the assignment deterministic regardless of the Go
// runtime's map iteration order (which is intentionally random).
// Must be called with g.mu held (write).
func (g *Group) rebalance() {
	g.Rebalancing = true
	defer func() {
		g.Rebalancing = false
		g.LastRebalanceAt = time.Now()
	}()

	if len(g.consumers) == 0 {
		return
	}

	// Collect consumer IDs and sort for a deterministic, stable assignment.
	ids := make([]string, 0, len(g.consumers))
	for id := range g.consumers {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic assignment

	// Assign partitions round-robin.
	assignments := make(map[string][]int32, len(ids))
	for p := int32(0); p < g.partCount; p++ {
		cid := ids[int(p)%len(ids)]
		assignments[cid] = append(assignments[cid], p)
	}
	for cid, c := range g.consumers {
		c.assign(assignments[cid])
	}
}

// handleSlowConsumer retries delivery up to g.maxRetries times, sleeping
// g.retryDelay between each attempt. The caller must NOT hold any group lock.
// Only after all attempts are exhausted is the message forwarded to the DLQ.
// DLQEntry.Attempts records the actual number of delivery attempts made.
//
// Semantics: the initial dispatch attempt has already failed in
// Dispatch (that is why we are here). We then retry up to maxRetries more
// times. With maxRetries == 0 there are no retries, so the message goes
// straight to the DLQ after the single failed initial attempt — it is never
// silently dropped.
func (g *Group) handleSlowConsumer(c *Consumer, msg *types.Message) {
	for attempt := 1; attempt <= g.maxRetries; attempt++ {
		time.Sleep(g.retryDelay)
		if c.deliver(msg) {
			return // delivery succeeded within retries
		}
	}
	// All retries exhausted, or maxRetries == 0 (loop body never ran): route
	// the message to the DLQ. Attempts records the number of retry attempts
	// actually performed (== maxRetries).
	if g.dlq != nil {
		g.dlq.Push(DLQEntry{
			Original: msg,
			Group:    g.id,
			Reason:   "consumer channel full after retries",
			Attempts: g.maxRetries,
		})
	}
}

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager manages all consumer groups across topics.
type Manager struct {
	mu         sync.RWMutex
	groups     map[string]*Group // "group/topic" → Group
	offsets    *partition.OffsetStore
	dlq        *DeadLetterQueue
	maxRetries int
	retryDelay time.Duration
	bufferSize int
}

// NewManager creates a consumer Manager.
func NewManager(offsets *partition.OffsetStore, dlq *DeadLetterQueue, maxRetries int, retryDelay time.Duration, bufferSize int) *Manager {
	return &Manager{
		groups:     make(map[string]*Group),
		offsets:    offsets,
		dlq:        dlq,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
		bufferSize: bufferSize,
	}
}

// DLQ returns the dead-letter queue shared by all consumer groups in this manager.
func (m *Manager) DLQ() *DeadLetterQueue { return m.dlq }

// DLQEntries returns up to limit DLQ entries matching group and topic.
// An empty group or topic matches all values.
func (m *Manager) DLQEntries(group, topic string, limit int) []DLQEntry {
	return m.dlq.Entries(group, topic, limit)
}

// DLQReplay removes up to limit DLQ entries matching group+topic and
// re-publishes them to their original topic via the provided republish function.
// Returns the number of entries replayed and any error.
func (m *Manager) DLQReplay(group, topic string, limit int, republish func(*types.Message) error) (int, error) {
	entries := m.dlq.Replay(group, topic, limit)
	var replayed int
	for _, e := range entries {
		if err := republish(e.Original); err != nil {
			return replayed, fmt.Errorf("dlq replay: %w", err)
		}
		replayed++
	}
	return replayed, nil
}

// DLQPurge removes all DLQ entries matching group+topic and returns the count.
func (m *Manager) DLQPurge(group, topic string) int {
	return m.dlq.Purge(group, topic)
}

// DLQDelete removes a single DLQ entry by its ID, returning true if found.
func (m *Manager) DLQDelete(group, topic, id string) bool {
	return m.dlq.Delete(group, topic, id)
}

// DLQGet returns a single DLQ entry by its ID, or nil if not found.
func (m *Manager) DLQGet(group, topic, id string) *DLQEntry {
	return m.dlq.Get(group, topic, id)
}

// Subscribe adds consumerID to groupID subscribed to topic.
// Creates the group if it doesn't exist.
// Returns an error if groupID fails the validGroupName check, because a
// group name containing "/" would silently collide with another group's key
// in the internal groupKey map (e.g. "a/b" on topic "c" == "a" on topic "b/c").
func (m *Manager) Subscribe(groupID, consumerID, clientID, topic string, partCount int32) (*Consumer, error) {
	if !validGroupName.MatchString(groupID) {
		return nil, fmt.Errorf("invalid group name %q: must match [a-zA-Z0-9][a-zA-Z0-9._-]{0,248}", groupID)
	}

	key := groupKey(groupID, topic)
	m.mu.Lock()
	g, ok := m.groups[key]
	if !ok {
		g = NewGroup(groupID, topic, partCount, m.offsets, m.dlq, m.maxRetries, m.retryDelay)
		m.groups[key] = g
	}
	m.mu.Unlock()
	return g.Join(consumerID, clientID, m.bufferSize)
}

// Unsubscribe removes consumerID from groupID on topic.
func (m *Manager) Unsubscribe(groupID, consumerID, topic string) {
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()
	if g != nil {
		g.Leave(consumerID)
	}
}

// Dispatch routes msg to all groups subscribed to msg.Topic.
func (m *Manager) Dispatch(msg *types.Message) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, g := range m.groups {
		if groupTopic(key) == msg.Topic {
			g.Dispatch(msg)
		}
	}
}

// DispatchToGroup routes msg to a single consumer group (groupID) subscribed to
// topic, ignoring every other group on the same topic. It is used by the nack
// requeue path so that a retried message is delivered only to the group that
// originally nacked it rather than fanning out to all subscribers.
//
// If the group does not exist (no active subscription and no committed-offset
// history) the message is silently dropped, mirroring the no-op semantics of
// Dispatch when no group matches.
func (m *Manager) DispatchToGroup(groupID, topic string, partition int32, msg *types.Message) {
	if msg == nil {
		return
	}
	// Ensure the message carries the intended partition in case the caller
	// supplied a msg whose Partition differs from the requeue target.
	msg.Partition = partition
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()
	if g == nil {
		return
	}
	g.Dispatch(msg)
}

// CommitOffset records an offset commit.
func (m *Manager) CommitOffset(groupID, consumerID, topic string, partID int32, offset int64) error {
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()
	if g == nil {
		return fmt.Errorf("group %q/%s not found", groupID, topic)
	}
	return g.CommitOffset(consumerID, partID, offset)
}

// HasCommittedOffset reports whether group has a committed offset for the
// given partition. It returns false for brand-new groups that have never
// committed an offset.
func (m *Manager) HasCommittedOffset(groupID, topic string, partID int32) bool {
	return m.offsets.Load(groupID, topic, partID) >= 0
}

// GroupOffset returns the committed offset for a group on a partition,
// or -1 if no offset has been committed.
func (m *Manager) GroupOffset(groupID, topic string, partID int32) int64 {
	return m.offsets.Load(groupID, topic, partID)
}

// ReplayGroup replays all messages from offset 0 (or from the next offset
// after a committed position) to the given group. This is used by the broker
// to deliver historical messages to brand-new consumer groups that have never
// committed an offset before, matching Kafka's auto.offset.reset=earliest
// semantics.
func (m *Manager) ReplayGroup(groupID, topic string, partID int32, pl storageReader, batchSize int) ([]*types.Message, error) {
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()

	var startOff int64
	if g != nil {
		startOff = g.Offset(partID)
		if startOff < 0 {
			startOff = 0
		} else {
			startOff++ // next unread
		}
	} else {
		committed := m.offsets.Load(groupID, topic, partID)
		if committed >= 0 {
			startOff = committed + 1
		}
	}

	return pl.Read(startOff, batchSize)
}

// storageReader is a minimal interface for reading messages from a partition log.
type storageReader interface {
	Read(startOffset int64, maxCount int) ([]*types.Message, error)
}

// SeekGroupOffset directly sets the committed offset for group/topic/partition
// in the shared OffsetStore.  Unlike CommitOffset it does not require an
// active subscription, making it suitable for seek and reset operations.
func (m *Manager) SeekGroupOffset(groupID, topic string, partID int32, offset int64) {
	m.offsets.Commit(groupID, topic, partID, offset)
}

// PollPartitionLog is a helper for long-poll consumers. It reads from the
// partition log starting at the group's committed offset and blocks until
// messages are available or ctx is done. It uses pl.NotifyAppend() to wake
// immediately when new data is written (rather than busy-polling at 50 ms).
func (m *Manager) PollPartitionLog(ctx context.Context, groupID, topic string, partID int32, pl *storage.PartitionLog, batchSize int) ([]*types.Message, error) {
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()

	var startOff int64
	if g != nil {
		startOff = g.Offset(partID)
		if startOff < 0 {
			startOff = 0
		} else {
			startOff++ // next unread
		}
	} else {
		// No active group subscription, but the OffsetStore may hold a
		// committed offset from a previous subscription or a seek/reset
		// operation. Read it directly so that SeekGroupOffset / Reset take
		// effect even when there is no live Group object.
		committed := m.offsets.Load(groupID, topic, partID)
		if committed >= 0 {
			startOff = committed + 1
		}
		// committed == -1 means reset to beginning: startOff stays 0.
	}

	for {
		msgs, err := pl.Read(startOff, batchSize)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pl.NotifyAppend(): // wake on new write; avoids busy-polling
		case <-time.After(500 * time.Millisecond): // fallback timeout
		}
	}
}

func groupKey(group, topic string) string { return group + "/" + topic }
func groupTopic(key string) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[i+1:]
		}
	}
	return key
}

// GroupSummary contains aggregate information about a consumer group.
type GroupSummary struct {
	// Group is the consumer group name.
	Group string
	// Topic is the topic this group is subscribed to.
	Topic string
	// ActiveConsumers is the number of active consumers in the group.
	ActiveConsumers int
	// CommittedOffsets holds the latest committed offset per partition.
	CommittedOffsets []GroupPartitionOffset
	// Rebalancing is true while a rebalance is in progress.
	Rebalancing bool
	// LastRebalanceAt is the time the most recent rebalance completed.
	LastRebalanceAt time.Time
}

// GroupPartitionOffset holds the committed offset for one partition.
type GroupPartitionOffset struct {
	// Partition is the partition number.
	Partition int32
	// Offset is the last committed offset for this group/partition.
	Offset int64
}

// GroupDetail contains detailed information about a consumer group.
type GroupDetail struct {
	// Group is the consumer group name.
	Group string
	// Topic is the topic this group is subscribed to.
	Topic string
	// Members holds one entry per active consumer in the group.
	Members []ConsumerMember
	// Rebalancing is true while a rebalance is in progress.
	Rebalancing bool
	// LastRebalanceAt is the time the most recent rebalance completed.
	LastRebalanceAt time.Time
	// MaxRetries is the maximum number of delivery retries before DLQ.
	MaxRetries int
	// RetryDelayMs is the delay between delivery retries in milliseconds.
	RetryDelayMs int
	// FailedMessageCount is the number of DLQ entries for this group+topic.
	FailedMessageCount int
	// Partitions holds per-partition offset information.
	Partitions []GroupPartitionDetail
}

// ConsumerMember holds per-consumer info within a group.
type ConsumerMember struct {
	// ConsumerID is the unique consumer identifier.
	ConsumerID string
	// Partitions is the set of partitions assigned to this consumer.
	Partitions []int32
	// ConnectedSince is the time the consumer joined (zero if unknown).
	ConnectedSince time.Time
	// PushMode is true when the consumer is in push delivery mode.
	PushMode bool
}

// GroupPartitionDetail holds per-partition offset info for a consumer group.
type GroupPartitionDetail struct {
	// Partition is the partition number.
	Partition int32
	// CommittedOffset is the last committed offset for this partition.
	CommittedOffset int64
	// CurrentOffset is the current log head offset for this partition.
	CurrentOffset int64
	// Lag is CurrentOffset - CommittedOffset.
	Lag int64
}

// GetGroupDetail returns detailed information about a specific consumer group.
// The logHead function is called with a partition number to get the current log
// head offset (NextOffset). Returns nil if the group does not exist.
func (m *Manager) GetGroupDetail(groupID, topic string, logHead func(topic string, partition int32) int64) *GroupDetail {
	key := groupKey(groupID, topic)
	m.mu.RLock()
	g := m.groups[key]
	m.mu.RUnlock()
	if g == nil {
		return nil
	}

	g.mu.RLock()
	rebalancing := g.Rebalancing
	lastRebalanceAt := g.LastRebalanceAt
	maxRetries := g.maxRetries
	retryDelayMs := int(g.retryDelay / time.Millisecond)
	partCount := g.partCount
	members := make([]ConsumerMember, 0, len(g.consumers))
	for _, c := range g.consumers {
		members = append(members, ConsumerMember{
			ConsumerID: c.ID,
			Partitions: c.Assignments(),
			PushMode:   c.GetSink() != nil,
		})
	}
	g.mu.RUnlock()

	var partitions []GroupPartitionDetail
	var failedCount int
	for i := int32(0); i < partCount; i++ {
		committed := m.offsets.Load(groupID, topic, i)
		current := logHead(topic, i)
		lag := current - committed
		if lag < 0 {
			lag = 0
		}
		partitions = append(partitions, GroupPartitionDetail{
			Partition:       i,
			CommittedOffset: committed,
			CurrentOffset:   current,
			Lag:             lag,
		})
	}

	// Count DLQ entries for this group+topic.
	dlqEntries := m.dlq.Entries(groupID, topic, 0)
	failedCount = len(dlqEntries)

	return &GroupDetail{
		Group:              groupID,
		Topic:              topic,
		Members:            members,
		Rebalancing:        rebalancing,
		LastRebalanceAt:    lastRebalanceAt,
		MaxRetries:         maxRetries,
		RetryDelayMs:       retryDelayMs,
		FailedMessageCount: failedCount,
		Partitions:         partitions,
	}
}

// ListGroups returns a snapshot of all active consumer groups with their
// committed offsets. Groups with zero active consumers are included if they
// have committed offset history.
func (m *Manager) ListGroups() []GroupSummary {
	m.mu.RLock()
	keys := make([]string, 0, len(m.groups))
	groupsCopy := make(map[string]*Group, len(m.groups))
	for k, g := range m.groups {
		keys = append(keys, k)
		groupsCopy[k] = g
	}
	m.mu.RUnlock()

	var out []GroupSummary
	for _, key := range keys {
		g := groupsCopy[key]
		g.mu.RLock()
		active := len(g.consumers)
		partCount := g.partCount
		groupID := g.id
		topicName := g.topic
		rebalancing := g.Rebalancing
		lastRebalanceAt := g.LastRebalanceAt
		g.mu.RUnlock()

		var offsets []GroupPartitionOffset
		for i := int32(0); i < partCount; i++ {
			off := m.offsets.Load(groupID, topicName, i)
			offsets = append(offsets, GroupPartitionOffset{
				Partition: i,
				Offset:    off,
			})
		}
		out = append(out, GroupSummary{
			Group:            groupID,
			Topic:            topicName,
			ActiveConsumers:  active,
			CommittedOffsets: offsets,
			Rebalancing:      rebalancing,
			LastRebalanceAt:  lastRebalanceAt,
		})
	}
	return out
}
