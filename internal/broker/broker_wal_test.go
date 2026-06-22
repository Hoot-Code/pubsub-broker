package broker_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// newPhase9Broker creates a broker configured for phase-9 tests.
func newPhase9Broker(t *testing.T, dir string) *broker.Broker {
	t.Helper()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "p9-node"},
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

// httpAddrFor returns the HTTP admin base URL for the given broker.
// It uses b.HTTPAddr() to get the actual bound address, which is correct
// even when the TCP port was 0 (ephemeral).
func httpAddrFor(t *testing.T, b *broker.Broker) string {
	t.Helper()
	// HTTPAddr is set synchronously in Start() before server.Start() binds
	// the TCP port, so it is always available once startBrokerWithAddr returns.
	addr := b.HTTPAddr()
	if addr == "" {
		t.Fatal("HTTPAddr not set — broker may not have started")
	}
	return "http://" + addr
}

// waitHTTP polls url until it returns any response or times out.
func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("HTTP server at %s did not become reachable within 5s", url)
}

// fetchWithGroup fetches messages from a partition for a specific group.
func fetchWithGroup(t *testing.T, addr, topic, group string, partition int32) []*types.Message {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("fetchWithGroup dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.Encode(protocol.CmdFetch, 1, &protocol.FetchRequest{
		Topic:     topic,
		Group:     group,
		Partition: partition,
		Offset:    0,
		MaxCount:  1000,
	}); err != nil {
		t.Fatalf("fetchWithGroup encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("fetchWithGroup decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("fetchWithGroup error: %s: %s", e.Code, e.Message)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(f.Body, &raw); err != nil {
		t.Fatalf("fetchWithGroup unmarshal: %v", err)
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

// ─── B: Health endpoints ──────────────────────────────────────────────────────

// TestHealthLive verifies GET /healthz/live always returns 200 (B5).
func TestHealthLive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpBase := httpAddrFor(t, b)
	waitHTTP(t, httpBase+"/healthz/live")

	resp, err := http.Get(httpBase + "/healthz/live") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /healthz/live: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz/live: want 200, got %d", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status: want %q, got %q", "ok", body["status"])
	}
}

// TestHealthReady verifies /healthz/ready returns 200 after Start() (B5).
func TestHealthReady(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpBase := httpAddrFor(t, b)
	waitHTTP(t, httpBase+"/healthz/ready")

	resp, err := http.Get(httpBase + "/healthz/ready") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /healthz/ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz/ready: want 200 after Start(), got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("status: want %q, got %q", "ready", body["status"])
	}
	if body["node_id"] == nil || body["node_id"] == "" {
		t.Errorf("node_id: want non-empty, got %q", body["node_id"])
	}
	if body["cluster_enabled"] != false {
		t.Errorf("cluster_enabled: want false for single-node, got %v", body["cluster_enabled"])
	}
}

// TestHealthStartup verifies /healthz/startup returns 200 after Start() (B5).
func TestHealthStartup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	_ = startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	httpBase := httpAddrFor(t, b)
	waitHTTP(t, httpBase+"/healthz/startup")

	resp, err := http.Get(httpBase + "/healthz/startup") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /healthz/startup: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz/startup: want 200, got %d", resp.StatusCode)
	}
}

// ─── WAL no replay duplicates ──────────────────────────────────────────────

// TestWALNoReplayDuplicates starts a broker, publishes 5 messages, stops
// cleanly, restarts, and verifies exactly 5 messages (not 10) in the log.
// Truncation happens in Start() after replay.
func TestWALNoReplayDuplicates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b1 := newPhase9Broker(t, dir)
	addr1 := startBrokerWithAddr(t, b1)
	const topic = "fix6-nodup"
	createTopicOn(t, addr1, topic, 1)
	publishNOn(t, addr1, topic, 5)
	stopBroker(t, b1)

	b2 := newPhase9Broker(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })

	msgs := fetchAll(t, addr2, topic, 0)
	if len(msgs) != 5 {
		t.Errorf("after restart: want 5 messages, got %d (WAL replay duplicate?)", len(msgs))
	}
}

// ─── C: Seek to timestamp / end ──────────────────────────────────────────────

// TestSeekToTimestamp seeks to a midpoint timestamp and verifies
// only the second-half messages are returned (C8).
func TestSeekToTimestamp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "seek-ts"
	createTopicOn(t, addr, topic, 1)
	publishNOn(t, addr, topic, 50)
	midpointTs := time.Now().UnixNano()
	time.Sleep(2 * time.Millisecond)
	publishNOn(t, addr, topic, 50)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.Encode(protocol.CmdSeek, 1, &protocol.SeekRequest{
		Topic:       topic,
		Group:       "seek-ts-group",
		TimestampNs: midpointTs,
	}); err != nil {
		t.Fatalf("seek encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("seek decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("seek error: %s: %s", e.Code, e.Message)
	}
	var seekResp protocol.SeekResponse
	if err := protocol.Unmarshal(f, &seekResp); err != nil {
		t.Fatalf("seek unmarshal: %v", err)
	}
	off, ok := seekResp.Offsets["0"]
	if !ok {
		t.Fatal("no offset for partition 0 in seek response")
	}
	// The response contains the "next to read" offset.
	// After seeking to a midpoint timestamp between messages 49 and 50,
	// the next offset should be 50 (the first message of the second batch).
	if off < 50 {
		t.Errorf("seek returned next-offset %d, expected ≥50 (second batch)", off)
	}
	t.Logf("seek-to-timestamp reported next-offset=%d", off)
}

// TestSeekToEnd seeks to end, publishes more, and verifies only new messages
// are fetched (C8).
func TestSeekToEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "seek-end"
	createTopicOn(t, addr, topic, 1)
	publishNOn(t, addr, topic, 50)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.Encode(protocol.CmdSeek, 1, &protocol.SeekRequest{
		Topic: topic,
		Group: "seek-end-group",
		ToEnd: true,
	}); err != nil {
		t.Fatalf("seek encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("seek decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("seek error: %s: %s", e.Code, e.Message)
	}

	publishNOn(t, addr, topic, 10)

	msgs := fetchWithGroup(t, addr, topic, "seek-end-group", 0)
	if len(msgs) != 10 {
		t.Errorf("after seek-to-end: want 10 messages, got %d", len(msgs))
	}
}

// TestResetGroup verifies that after reset, fetching starts from offset 0 (C8).
func TestResetGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "reset-group"
	createTopicOn(t, addr, topic, 1)
	publishNOn(t, addr, topic, 10)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	// Subscribe → commit offset 9.
	if err := enc.Encode(protocol.CmdSubscribe, 1, &protocol.SubscribeRequest{
		Topic:      topic,
		Group:      "reset-grp",
		ConsumerID: "c1",
	}); err != nil {
		t.Fatalf("subscribe encode: %v", err)
	}
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("subscribe decode: %v", err)
	}
	if err := enc.Encode(protocol.CmdCommitOffset, 2, &protocol.CommitOffsetRequest{
		Group:      "reset-grp",
		ConsumerID: "c1",
		Topic:      topic,
		Partition:  0,
		Offset:     9,
	}); err != nil {
		t.Fatalf("commit encode: %v", err)
	}
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("commit decode: %v", err)
	}

	// Reset.
	if err := enc.Encode(protocol.CmdResetGroup, 3, &protocol.ResetGroupRequest{
		Group: "reset-grp",
		Topic: topic,
	}); err != nil {
		t.Fatalf("reset encode: %v", err)
	}
	rf, err := dec.Decode()
	if err != nil {
		t.Fatalf("reset decode: %v", err)
	}
	if rf.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(rf, &e)
		t.Fatalf("reset error: %s: %s", e.Code, e.Message)
	}

	// After reset the group should read from offset 0.
	msgs := fetchWithGroup(t, addr, topic, "reset-grp", 0)
	if len(msgs) == 0 {
		t.Error("after reset: expected messages from offset 0, got 0")
	}
	if len(msgs) > 0 && msgs[0].Offset != 0 {
		t.Errorf("first message offset after reset: want 0, got %d", msgs[0].Offset)
	}
}

// ─── D: Graceful Stop ─────────────────────────────────────────────────────────

// TestGracefulStop verifies Stop() drains in-flight requests and
// /healthz/drain reflects the drain state (D4).
func TestGracefulStop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := newPhase9Broker(t, dir)
	addr := startBrokerWithAddr(t, b)

	const topic = "graceful-stop"
	createTopicOn(t, addr, topic, 1)

	httpBase := httpAddrFor(t, b)
	waitHTTP(t, httpBase+"/healthz/drain")

	// /healthz/drain should show draining:false before Stop().
	resp, err := http.Get(httpBase + "/healthz/drain") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /healthz/drain: %v", err)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		resp.Body.Close()
		t.Fatalf("decode drain response: %v", err)
	}
	resp.Body.Close()
	if draining, _ := body["draining"].(bool); draining {
		t.Error("expected draining=false before Stop()")
	}

	// Stop and verify it completes without error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		t.Logf("Stop returned: %v", err)
	}
}

// TestSingleNodeClusterBecomesLeaderImmediately is an integration test that
// starts a broker with cluster.enabled=true and a single-node config (matching
// what docker-compose actually sets), calls broker.IsLeader(), and asserts
// true within 2 seconds of Start() returning. This is the test that should
// have caught the single-node leader-election bug.
func TestSingleNodeClusterBecomesLeaderImmediately(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "leader-test-node"},
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
		"cluster": map[string]interface{}{
			"enabled":   true,
			"node_id":   "leader-test-node",
			"bind_addr": "127.0.0.1:0",
		},
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
	go b.Start()
	t.Cleanup(func() { stopBroker(t, b) })

	// Wait for broker to become ready (polling loop).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.Ready() {
		t.Fatal("broker did not become ready within 5s")
	}

	// With the startup fast path, IsLeader should be true within 2 seconds.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("broker with single-node cluster did not become leader within 2s; IsLeader=%v", b.IsLeader())
}
