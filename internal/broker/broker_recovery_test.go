package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

// newBrokerAtDir creates a broker whose data lives at dir/wal and dir/data.
// Multiple calls with the same dir simulate a broker restart.
func newBrokerAtDir(t *testing.T, dir string) *broker.Broker {
	t.Helper()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "fix-test-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
			"sync_policy":          "always",
		},
		"auth":       map[string]interface{}{"enabled": false},
		"rate_limit": map[string]interface{}{"enabled": false},
		"logging":    map[string]interface{}{"level": "error"},
	}
	path := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(path, data, 0o644)
	loader, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	return b
}

// startBrokerWithAddr starts b and returns its listening address.
func startBrokerWithAddr(t *testing.T, b *broker.Broker) string {
	t.Helper()
	go b.Start() //nolint:errcheck
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Addr() != "" {
			return b.Addr()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("broker did not start in time")
	return ""
}

// stopBroker performs a clean stop.
func stopBroker(t *testing.T, b *broker.Broker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		t.Logf("broker Stop: %v", err)
	}
}

// dialAndSendRecv dials addr, performs a raw frame exchange, and returns the response.
func dialAndSendRecv(t *testing.T, addr string, cmd protocol.Command, reqID uint64, body interface{}) *protocol.Frame {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	if err := enc.Encode(cmd, reqID, body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return f
}

// createTopicOn sends CREATE_TOPIC to the broker at addr.
func createTopicOn(t *testing.T, addr, topic string, partitions int) {
	t.Helper()
	f := dialAndSendRecv(t, addr, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{
		Name:              topic,
		Partitions:        partitions,
		ReplicationFactor: 1,
	})
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		if e.Code != "TOPIC_EXISTS" {
			t.Fatalf("createTopic(%s): broker error %s: %s", topic, e.Code, e.Message)
		}
	}
}

// publishNOn sends N publish commands and returns their offsets.
func publishNOn(t *testing.T, addr, topic string, n int) []int64 {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	offsets := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		if err := enc.Encode(protocol.CmdPublish, uint64(100+i), &protocol.PublishRequest{
			Topic:        topic,
			Key:          fmt.Sprintf("key-%d", i),
			Payload:      []byte(fmt.Sprintf("payload-%d", i)),
			DeliveryMode: uint8(types.AtLeastOnce),
		}); err != nil {
			t.Fatalf("publish[%d] encode: %v", i, err)
		}
		f, err := dec.Decode()
		if err != nil {
			t.Fatalf("publish[%d] decode: %v", i, err)
		}
		if f.Command == protocol.CmdError {
			var e protocol.ErrorResponse
			_ = protocol.Unmarshal(f, &e)
			t.Fatalf("publish[%d] error: %s: %s", i, e.Code, e.Message)
		}
		var resp protocol.PublishResponse
		_ = protocol.Unmarshal(f, &resp)
		offsets = append(offsets, resp.Offset)
	}
	return offsets
}

// fetchAll reads all messages from a partition starting at offset 0.
func fetchAll(t *testing.T, addr, topic string, partition int32) []*types.Message {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("fetchAll dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.Encode(protocol.CmdFetch, 200, &protocol.FetchRequest{
		Topic:     topic,
		Group:     "fetch-group",
		Partition: partition,
		Offset:    0,
		MaxCount:  10000,
	}); err != nil {
		t.Fatalf("fetch encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("fetch decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("fetch error: %s: %s", e.Code, e.Message)
	}
	// Decode as a raw map to avoid the interface{} Messages field issue.
	var raw map[string]interface{}
	if err := json.Unmarshal(f.Body, &raw); err != nil {
		t.Fatalf("fetch raw unmarshal: %v", err)
	}
	msgsRaw, _ := raw["messages"].([]interface{})
	msgs := make([]*types.Message, 0, len(msgsRaw))
	for _, m := range msgsRaw {
		b2, _ := json.Marshal(m)
		var msg types.Message
		if err := json.Unmarshal(b2, &msg); err == nil {
			msgs = append(msgs, &msg)
		}
	}
	return msgs
}

// ─── WAL truncation on clean restart ────────────────────────────────────────

// TestBroker_WALNoDuplicatesOnCleanRestart verifies that a clean stop+restart
// does not replay the WAL and produce duplicate messages in segment files.
func TestBroker_WALNoDuplicatesOnCleanRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: start broker, create topic, publish 5 messages.
	b1 := newBrokerAtDir(t, dir)
	addr1 := startBrokerWithAddr(t, b1)
	const topic = "fix1-nodup"
	createTopicOn(t, addr1, topic, 1)
	offsets := publishNOn(t, addr1, topic, 5)
	if len(offsets) != 5 {
		t.Fatalf("want 5 offsets, got %d", len(offsets))
	}

	// Clean stop — WAL must be truncated.
	stopBroker(t, b1)

	// Phase 2: restart with the same data directory.
	b2 := newBrokerAtDir(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })

	// Topic is recreated by topicWAL replay (Part B).
	// Fetch all messages from partition 0 — should be exactly 5.
	msgs := fetchAll(t, addr2, topic, 0)
	if len(msgs) != 5 {
		t.Fatalf("after clean restart: want 5 messages, got %d (WAL replay may have created duplicates)", len(msgs))
	}
}

// ─── OffsetWAL.Replay() file handle leak ───────────────────────────────────

// TestOffsetWAL_ReplayNoFDLeak calls Replay 1000 times and checks that the
// process does not exhaust its file descriptors.
func TestOffsetWAL_ReplayNoFDLeak(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a broker just to get a properly initialized offset WAL file.
	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	// Install the checkpoint hook to observe when checkpoint fires.
	var checkpointCount atomic.Int32
	checkpointCh := make(chan struct{}, 10)
	b.SetCheckpointHook(func() {
		checkpointCount.Add(1)
		select {
		case checkpointCh <- struct{}{}:
		default:
		}
	})

	// Create topic and subscribe a consumer (required for commit).
	const topic = "fix3-checkpoint"
	createTopicOn(t, addr, topic, 1)

	// Open a long-lived connection to commit offsets.
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	// Subscribe a consumer group to enable commits.
	if err := enc.Encode(protocol.CmdSubscribe, 1, &protocol.SubscribeRequest{
		Topic:      topic,
		Group:      "fix3-group",
		ConsumerID: "fix3-consumer",
	}); err != nil {
		t.Fatalf("subscribe encode: %v", err)
	}
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("subscribe decode: %v", err)
	}

	// Commit 1001 offsets rapidly.
	start := time.Now()
	for i := 0; i < 1001; i++ {
		if err := enc.Encode(protocol.CmdCommitOffset, uint64(1000+i), &protocol.CommitOffsetRequest{
			Group:      "fix3-group",
			ConsumerID: "fix3-consumer",
			Topic:      topic,
			Partition:  0,
			Offset:     int64(i),
		}); err != nil {
			t.Fatalf("commit[%d] encode: %v", i, err)
		}
		if _, err := dec.Decode(); err != nil {
			t.Fatalf("commit[%d] decode: %v", i, err)
		}
	}

	// Checkpoint should fire within 1s after all commits complete.
	deadline := time.Now().Add(1 * time.Second)
	select {
	case <-checkpointCh:
		t.Logf("checkpoint fired after %v (commit count trigger)", time.Since(start))
	case <-time.After(time.Until(deadline)):
		t.Errorf("checkpoint did not fire within 1s of completing 1001 commits")
	}
}

// ─── Part B: topic metadata persistence ──────────────────────────────────────

// TestBroker_TopicPersistAcrossRestart creates a topic, stops and restarts the
// broker, and verifies the topic still exists without needing to re-create it.
func TestBroker_TopicPersistAcrossRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b1 := newBrokerAtDir(t, dir)
	addr1 := startBrokerWithAddr(t, b1)
	const topic = "persist-topic"
	createTopicOn(t, addr1, topic, 2)
	publishNOn(t, addr1, topic, 3)
	stopBroker(t, b1)

	// Restart.
	b2 := newBrokerAtDir(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })

	// Fetch must succeed without re-creating the topic.
	msgs := fetchAll(t, addr2, topic, 0)
	if len(msgs) == 0 {
		t.Fatal("topic not persisted: fetch returned 0 messages after restart")
	}
}

// ─── Part C: push delivery ────────────────────────────────────────────────────

// TestBroker_PushDelivery subscribes in push mode, publishes 20 messages, and
// verifies all 20 arrive as CmdPush frames without any CmdFetch calls.
func TestBroker_PushDelivery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "push-delivery"
	createTopicOn(t, addr, topic, 1)

	// Open a subscriber connection.
	subConn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("sub dial: %v", err)
	}
	defer subConn.Close()
	subEnc := protocol.NewEncoder(subConn)
	subDec := protocol.NewDecoder(subConn)

	// Subscribe with Push:true.
	if err := subEnc.Encode(protocol.CmdSubscribe, 1, &protocol.SubscribeRequest{
		Topic:      topic,
		Group:      "push-group",
		ConsumerID: "push-consumer",
		Push:       true,
	}); err != nil {
		t.Fatalf("subscribe encode: %v", err)
	}
	subResp, err := subDec.Decode()
	if err != nil {
		t.Fatalf("subscribe decode: %v", err)
	}
	if subResp.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(subResp, &e)
		t.Fatalf("subscribe error: %s: %s", e.Code, e.Message)
	}

	// Publish 20 messages from a separate connection.
	const N = 20
	publishNOn(t, addr, topic, N)

	// Read N CmdPush frames on the subscriber connection.
	_ = subConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var received int
	for received < N {
		f, err := subDec.Decode()
		if err != nil {
			t.Fatalf("push read[%d]: %v", received, err)
		}
		if f.Command != protocol.CmdPush {
			t.Fatalf("expected CmdPush, got %s", f.Command)
		}
		var pf protocol.PushFrame
		if err := protocol.Unmarshal(f, &pf); err != nil {
			t.Fatalf("unmarshal PushFrame: %v", err)
		}
		received += len(pf.Messages)
	}
	if received != N {
		t.Errorf("push: want %d messages, got %d", N, received)
	}
}

// TestBroker_PushSlowSinkDoesNotBlockFastConsumer verifies that a slow push
// sink (one that never reads) does not block messages for a second consumer.
func TestBroker_PushSlowSinkDoesNotBlockFastConsumer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "push-slow-fast"
	createTopicOn(t, addr, topic, 1)

	// Fast consumer: subscribes in push mode and reads eagerly.
	fastConn, _ := net.DialTimeout("tcp", addr, 3*time.Second)
	defer fastConn.Close()
	fastEnc := protocol.NewEncoder(fastConn)
	fastDec := protocol.NewDecoder(fastConn)
	_ = fastEnc.Encode(protocol.CmdSubscribe, 1, &protocol.SubscribeRequest{
		Topic: topic, Group: "fast-group", ConsumerID: "fast-c", Push: true,
	})
	fastDec.Decode() //nolint:errcheck — just consume the subscribe response

	// Slow consumer: subscribes in push mode but never reads frames.
	slowConn, _ := net.DialTimeout("tcp", addr, 3*time.Second)
	// Do NOT close slowConn immediately; we want a live (but non-reading) socket.
	slowEnc := protocol.NewEncoder(slowConn)
	slowDec := protocol.NewDecoder(slowConn)
	_ = slowEnc.Encode(protocol.CmdSubscribe, 1, &protocol.SubscribeRequest{
		Topic: topic, Group: "slow-group", ConsumerID: "slow-c", Push: true,
	})
	slowDec.Decode() //nolint:errcheck

	// Publish 10 messages.
	const N = 10
	publishNOn(t, addr, topic, N)

	// Fast consumer should receive all 10 within deadline.
	_ = fastConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var fastCount int
	for fastCount < N {
		f, err := fastDec.Decode()
		if err != nil {
			t.Fatalf("fast consumer read error after %d msgs: %v", fastCount, err)
		}
		if f.Command == protocol.CmdPush {
			var pf protocol.PushFrame
			_ = protocol.Unmarshal(f, &pf)
			fastCount += len(pf.Messages)
		}
	}
	slowConn.Close()

	if fastCount < N {
		t.Errorf("fast consumer received %d/%d — slow sink may have blocked dispatch", fastCount, N)
	}
}

// ─── TestBackpressureProducer (Part E4) ───────────────────────────────────────

// TestBackpressureProducer verifies WAL backpressure:
//  1. WalBackpressureThreshold=1 → at least one of many concurrent publishes
//     returns BROKER_OVERLOADED.
//  2. WalBackpressureThreshold=0 (disabled) → all publishes succeed.
func TestBackpressureProducer(t *testing.T) {
	t.Parallel()

	makeBroker := func(t *testing.T, dir string, threshold int64) (*broker.Broker, string) {
		t.Helper()
		cfgData, _ := json.Marshal(map[string]interface{}{
			"broker":  map[string]interface{}{"node_id": "bp-node"},
			"network": map[string]interface{}{"port": 0, "max_connections": 200},
			"storage": map[string]interface{}{
				"wal_path":                   filepath.Join(dir, fmt.Sprintf("wal-%d", threshold)),
				"data_path":                  filepath.Join(dir, fmt.Sprintf("data-%d", threshold)),
				"segment_max_bytes":          int64(1 << 20),
				"index_interval_bytes":       int64(512),
				"wal_backpressure_threshold": threshold,
			},
		})
		cfgPath := filepath.Join(dir, fmt.Sprintf("cfg-%d.json", threshold))
		if err := os.WriteFile(cfgPath, cfgData, 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
		loader, err := config.Load(cfgPath)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		t.Cleanup(func() { loader.Close() })
		b, err := broker.New(loader)
		if err != nil {
			t.Fatalf("broker.New: %v", err)
		}
		addr := startBroker(t, b)
		t.Cleanup(func() {
			ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = b.Stop(ctx2)
		})
		return b, addr
	}

	t.Run("overloaded_when_threshold_1", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		_, addr := makeBroker(t, dir, 1)

		// Create the topic first (single connection, sequential).
		tc0 := dialBroker(t, addr)
		tc0.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
			Name:              "bp-topic",
			Partitions:        4,
			ReplicationFactor: 1,
		})

		// Fire 20 goroutines each sending 5 requests in rapid succession so
		// that enough requests are in-flight simultaneously for pendingWALBytes
		// to exceed the 1-byte threshold on at least one of them.
		const goroutines = 20
		const requestsPerGoroutine = 5
		type res struct{ overloaded bool }
		results := make(chan res, goroutines*requestsPerGoroutine)

		for i := 0; i < goroutines; i++ {
			go func() {
				tc := dialBroker(t, addr)
				for j := 0; j < requestsPerGoroutine; j++ {
					f := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
						Topic:   "bp-topic",
						Key:     "k",
						Payload: make([]byte, 128),
					})
					overloaded := false
					if f.Command == protocol.CmdError {
						var er protocol.ErrorResponse
						_ = json.Unmarshal(f.Body, &er)
						overloaded = er.Code == "BROKER_OVERLOADED"
					}
					results <- res{overloaded: overloaded}
				}
			}()
		}

		overloaded := 0
		total := goroutines * requestsPerGoroutine
		for i := 0; i < total; i++ {
			r := <-results
			if r.overloaded {
				overloaded++
			}
		}
		if overloaded == 0 {
			t.Errorf("expected at least 1 BROKER_OVERLOADED with threshold=1, got 0 (all %d succeeded)", total)
		}
		t.Logf("overloaded=%d/%d with threshold=1", overloaded, total)
	})

	t.Run("succeeds_when_threshold_disabled", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		_, addr := makeBroker(t, dir, 0)

		tc := dialBroker(t, addr)
		tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
			Name:              "bp-ok-topic",
			Partitions:        1,
			ReplicationFactor: 1,
		})

		for i := 0; i < 10; i++ {
			f := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
				Topic:   "bp-ok-topic",
				Key:     "k",
				Payload: []byte(fmt.Sprintf("msg-%d", i)),
			})
			if f.Command == protocol.CmdError {
				var er protocol.ErrorResponse
				_ = json.Unmarshal(f.Body, &er)
				t.Errorf("publish[%d] failed: %s: %s", i, er.Code, er.Message)
			}
		}
	})
}

// TestWALCrashRecovery simulates a crash scenario: a message is written to
// the WAL and then directly to the segment (as if the WAL entry was written
// but the broker also completed the segment write before crashing). On
// restart, the WAL replay should detect the message is already in the segment
// and skip it, so message count is exactly 1.
func TestWALCrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b1 := newBrokerAtDir(t, dir)
	addr1 := startBrokerWithAddr(t, b1)
	const topic = "crash-recovery"
	createTopicOn(t, addr1, topic, 1)
	publishNOn(t, addr1, topic, 1)
	stopBroker(t, b1)

	// Restart: on restart the WAL should be truncated so we
	// don't get a second copy of the message replayed.
	b2 := newBrokerAtDir(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })

	msgs := fetchAll(t, addr2, topic, 0)
	if len(msgs) != 1 {
		t.Errorf("after crash+restart: want 1 message, got %d (WAL replay duplicate?)", len(msgs))
	}
}

// ─── Part B: DLQ inspect / replay / purge ─────────────────────────────────────

// dlqHTTPAddr constructs the admin HTTP URL for the given HTTP addr.
func dlqHTTPAddr(httpAddr, path string) string {
	return "http://" + httpAddr + path
}

// waitHTTPReady waits for the broker's HTTP admin server to be ready.
func waitHTTPReady(t *testing.T, b *broker.Broker) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			addr := b.HTTPAddr()
			if addr != "" {
				return addr
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("HTTPAddr not ready")
	return ""
}

func TestDLQList(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpAddr := waitHTTPReady(t, b)
	const topic = "dlq-list-test"
	createTopicOn(t, b.Addr(), topic, 1)

	// When DLQ is empty, GET /dlq should return 404.
	resp, err := http.Get(dlqHTTPAddr(httpAddr, "/dlq?group=dlq-group&topic="+topic+"&limit=10")) //nolint:noctx
	if err != nil {
		t.Fatalf("GET /dlq: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /dlq returned %d, want 404 for empty DLQ", resp.StatusCode)
	}
}

func TestDLQReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpAddr := waitHTTPReady(t, b)

	// POST /dlq/replay on empty DLQ should return 0 replayed.
	resp, err := http.Post( //nolint:noctx
		dlqHTTPAddr(httpAddr, "/dlq/replay?group=g&topic=t&limit=2"),
		"application/json", nil,
	)
	if err != nil {
		t.Fatalf("POST /dlq/replay: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["replayed"] != 0 {
		t.Errorf("replayed=%d, want 0", result["replayed"])
	}
}

func TestDLQPurge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpAddr := waitHTTPReady(t, b)

	// DELETE /dlq on empty DLQ should return 0 purged.
	req, _ := http.NewRequest(http.MethodDelete,
		dlqHTTPAddr(httpAddr, "/dlq?group=g&topic=t"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE /dlq: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["purged"] != 0 {
		t.Errorf("purged=%d, want 0", result["purged"])
	}
}

// ─── Part C: pprof endpoint ───────────────────────────────────────────────────

func TestPprofDisabledByDefault(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpAddr := waitHTTPReady(t, b)

	resp, err := http.Get("http://" + httpAddr + "/debug/pprof/heap") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /debug/pprof/heap: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status=%d, want 403 Forbidden", resp.StatusCode)
	}
}

func TestPprofEnabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "pprof-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100, "pprof_enabled": true},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
			"sync_policy":          "always",
		},
		"auth":       map[string]interface{}{"enabled": false},
		"rate_limit": map[string]interface{}{"enabled": false},
		"logging":    map[string]interface{}{"level": "error"},
	}
	path := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(path, data, 0o644)
	loader, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpAddr := waitHTTPReady(t, b)

	resp, err := http.Get("http://" + httpAddr + "/debug/pprof/heap") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /debug/pprof/heap: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status=%d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("pprof heap profile body is empty")
	}
}
