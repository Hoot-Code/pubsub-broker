// Package main (brokectl) — integration tests that run each subcommand via
// the run() function directly (no os.Exec) against an in-process broker.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/pkg/client"
)

// ─── Test broker ─────────────────────────────────────────────────────────────

// startBroker launches an in-process broker on a random port and returns its
// binary address. It registers a Cleanup that stops the broker.
func startBroker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "brokectl-test-node"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 200,
		             "read_timeout": 10000000000, "write_timeout": 10000000000,
		             "idle_timeout": 30000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512,
		            "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth":        {"enabled": false},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	if err := os.WriteFile(cfgPath, []byte(cfgData), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	errC := make(chan error, 1)
	go func() { errC <- b.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Addr() != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
		select {
		case <-errC:
		case <-time.After(3 * time.Second):
		}
	})
	return b.Addr()
}

// ─── Helper ───────────────────────────────────────────────────────────────────

// capture runs brokectl's run() with the given args and captures stdout/stderr.
// It redirects os.Stdout and os.Stderr, runs the function, then restores them.
func capture(args []string) (stdout, stderr string, code int) {
	// Redirect stdout.
	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	// Redirect stderr.
	oldErr := os.Stderr
	rErr, wErr, _ := os.Pipe()
	os.Stderr = wErr

	code = run(args)

	wOut.Close()
	wErr.Close()
	os.Stdout = oldOut
	os.Stderr = oldErr

	var bufOut, bufErr bytes.Buffer
	io.Copy(&bufOut, rOut) //nolint:errcheck
	io.Copy(&bufErr, rErr) //nolint:errcheck
	rOut.Close()
	rErr.Close()
	return bufOut.String(), bufErr.String(), code
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestPing verifies that "brokectl ping" prints a PONG line and exits 0.
func TestPing(t *testing.T) {
	addr := startBroker(t)
	stdout, stderr, code := capture([]string{"--addr", addr, "ping"})
	if code != 0 {
		t.Fatalf("ping: exit code %d, stderr=%q", code, stderr)
	}
	if !strings.HasPrefix(stdout, "PONG") {
		t.Errorf("ping: want PONG prefix, got %q", stdout)
	}
}

// TestTopicCreate verifies that "brokectl topic create" exits 0 and prints a
// confirmation line.
func TestTopicCreate(t *testing.T) {
	addr := startBroker(t)
	stdout, stderr, code := capture([]string{
		"--addr", addr, "topic", "create", "--name", "orders", "--partitions", "4",
	})
	if code != 0 {
		t.Fatalf("topic create: exit code %d, stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "orders") {
		t.Errorf("topic create: want 'orders' in output, got %q", stdout)
	}
}

// TestTopicList verifies that "brokectl topic list" shows the created topic.
func TestTopicList(t *testing.T) {
	addr := startBroker(t)

	// Create two topics first.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "alpha", "--partitions", "2"})
	if code != 0 {
		t.Fatal("setup: create alpha failed")
	}
	_, _, code = capture([]string{"--addr", addr, "topic", "create", "--name", "beta", "--partitions", "3"})
	if code != 0 {
		t.Fatal("setup: create beta failed")
	}

	stdout, stderr, code := capture([]string{"--addr", addr, "topic", "list"})
	if code != 0 {
		t.Fatalf("topic list: exit code %d, stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "alpha") {
		t.Errorf("topic list: want 'alpha', got %q", stdout)
	}
	if !strings.Contains(stdout, "beta") {
		t.Errorf("topic list: want 'beta', got %q", stdout)
	}
	if !strings.Contains(stdout, "PARTITIONS") {
		t.Errorf("topic list: want 'PARTITIONS' header, got %q", stdout)
	}
}

// TestPublish verifies that "brokectl publish" exits 0 and prints the offset.
func TestPublish(t *testing.T) {
	addr := startBroker(t)

	// Create a topic first.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "events", "--partitions", "1"})
	if code != 0 {
		t.Fatal("setup: create topic failed")
	}

	stdout, stderr, code := capture([]string{
		"--addr", addr, "publish", "--topic", "events", "--payload", "hello-world",
	})
	if code != 0 {
		t.Fatalf("publish: exit code %d, stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "offset=") {
		t.Errorf("publish: want 'offset=' in output, got %q", stdout)
	}
}

// TestTopicCreateMissingName verifies that "topic create" without --name exits 1.
func TestTopicCreateMissingName(t *testing.T) {
	addr := startBroker(t)
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--partitions", "1"})
	if code == 0 {
		t.Error("expected non-zero exit code when --name is missing")
	}
}

// TestUnknownCommand verifies that an unknown command exits 1.
func TestUnknownCommand(t *testing.T) {
	addr := startBroker(t)
	_, _, code := capture([]string{"--addr", addr, "doesnotexist"})
	if code == 0 {
		t.Error("expected non-zero exit code for unknown command")
	}
}

// startBrokerFull launches an in-process broker on random ports and returns
// both the broker object (for HTTPAddr()) and the TCP address string.
// It registers a Cleanup that stops the broker.
func startBrokerFull(t *testing.T) (*broker.Broker, string) {
	t.Helper()
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "brokectl-test-node"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 200,
		             "read_timeout": 10000000000, "write_timeout": 10000000000,
		             "idle_timeout": 30000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512,
		            "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth":        {"enabled": false},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	if err := os.WriteFile(cfgPath, []byte(cfgData), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	errC := make(chan error, 1)
	go func() { errC <- b.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Addr() != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
		select {
		case <-errC:
		case <-time.After(3 * time.Second):
		}
	})
	return b, b.Addr()
}

// TestBrokectlHealthCommand starts an in-process broker, calls runHealth()
// directly, and verifies it returns 0 (READY) when the broker is ready.
// Uses HTTPAddr() to construct the correct fake broker addr
// (httpPort-1) so that runHealth's port+1 arithmetic reaches the actual HTTP
// admin port even when the TCP port is ephemeral (port=0 in tests).
func TestBrokectlHealthCommand(t *testing.T) {
	b, _ := startBrokerFull(t)

	// Wait for the HTTP server to be ready. HTTPAddr is set synchronously in
	// Start() before server.Start() binds the TCP port.
	deadline := time.Now().Add(5 * time.Second)
	var httpAddr string
	for time.Now().Before(deadline) {
		if httpAddr = b.HTTPAddr(); httpAddr != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if httpAddr == "" {
		t.Fatal("HTTPAddr not available after 5s")
	}

	// Construct a fake broker address where port+1 lands on the real HTTP port.
	// runHealth computes httpAddr = host:(port+1)/healthz/ready, so we pass
	// host:(httpPort-1) and it reaches the actual HTTP listener.
	host, portStr, err := splitHostPort(httpAddr)
	if err != nil {
		t.Fatalf("split HTTPAddr %q: %v", httpAddr, err)
	}
	var httpPort int
	fmt.Sscanf(portStr, "%d", &httpPort)
	fakeAddr := fmt.Sprintf("%s:%d", host, httpPort-1)

	var code int
	pollDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(pollDeadline) {
		code = runHealth(fakeAddr, nil)
		if code == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 0 {
		t.Errorf("runHealth returned %d, want 0 (READY)", code)
	}
}

// ─── Part A3: TestTailSubcommand ─────────────────────────────────────────────

func TestTailSubcommand(t *testing.T) {
	addr := startBroker(t)

	// Create topic.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "tail-test", "--partitions", "1"})
	if code != 0 {
		t.Fatal("topic create failed")
	}

	// Create a publisher client that will send messages concurrently.
	pubClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer pubClient.Close()

	// Start publishing in a goroutine. Wait 300ms for the tail consumer to subscribe.
	published := make(chan struct{})
	go func() {
		defer close(published)
		time.Sleep(300 * time.Millisecond)
		prod := pubClient.NewProducer("tail-test")
		for i := 0; i < 10; i++ {
			if _, err := prod.Publish(context.Background(), "", []byte(fmt.Sprintf("msg-%d", i)), nil); err != nil {
				return
			}
		}
	}()

	// Create a separate client for the tail consumer.
	tailClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial tail: %v", err)
	}
	defer tailClient.Close()

	// Capture stdout from runTail with --count 10 --format json.
	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	code = runTail(context.Background(), tailClient, []string{
		"--topic", "tail-test", "--count", "10", "--format", "json",
	})

	wOut.Close()
	os.Stdout = oldOut
	<-published

	var buf bytes.Buffer
	io.Copy(&buf, rOut)
	rOut.Close()

	if code != 0 {
		t.Fatalf("runTail: exit code %d", code)
	}

	// Parse 10 JSON lines and verify offsets 0-9.
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 10 {
		t.Fatalf("want 10 lines, got %d", len(lines))
	}
	offsets := make(map[int64]bool)
	for _, line := range lines {
		var msg struct {
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
			Key       string `json:"key"`
			Size      int    `json:"size"`
			Payload   string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse JSON: %v\nline: %s", err, line)
		}
		offsets[msg.Offset] = true
	}
	for i := int64(0); i < 10; i++ {
		if !offsets[i] {
			t.Errorf("missing offset %d", i)
		}
	}
}

// ─── Part D2: TestConsumeSubcommand ───────────────────────────────────────────

func TestConsumeSubcommand(t *testing.T) {
	addr := startBroker(t)

	// Create topic.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "consume-test", "--partitions", "1"})
	if code != 0 {
		t.Fatal("topic create failed")
	}

	// Create a publisher client that will send messages concurrently.
	pubClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer pubClient.Close()

	// Start publishing in a goroutine. Wait 300ms for the consumer to subscribe.
	published := make(chan struct{})
	go func() {
		defer close(published)
		time.Sleep(300 * time.Millisecond)
		prod := pubClient.NewProducer("consume-test")
		for i := 0; i < 5; i++ {
			if _, err := prod.Publish(context.Background(), "", []byte(fmt.Sprintf("consume-%d", i)), nil); err != nil {
				return
			}
		}
	}()

	// Create a separate client for the consumer.
	consumerClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer consumerClient.Close()

	// First consume: --batch 5 --commit --format json
	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	code = runConsume(context.Background(), consumerClient, []string{
		"--topic", "consume-test", "--group", "consume-grp",
		"--batch", "5", "--commit", "--format", "json",
	})

	wOut.Close()
	os.Stdout = oldOut
	<-published

	var buf bytes.Buffer
	io.Copy(&buf, rOut)
	rOut.Close()

	if code != 0 {
		t.Fatalf("runConsume: exit code %d", code)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 5 {
		t.Fatalf("want 5 JSON lines, got %d", len(lines))
	}
	for i, line := range lines {
		var msg struct {
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
			Payload   string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse line %d: %v\ncontent: %s", i, err, line)
		}
	}

	// Second consume should return 0 messages because offset was committed.
	oldOut = os.Stdout
	rOut2, wOut2, _ := os.Pipe()
	os.Stdout = wOut2

	code2 := runConsume(context.Background(), consumerClient, []string{
		"--topic", "consume-test", "--group", "consume-grp",
		"--batch", "5", "--commit", "--format", "json",
	})

	wOut2.Close()
	os.Stdout = oldOut

	var buf2 bytes.Buffer
	io.Copy(&buf2, rOut2)
	rOut2.Close()

	if code2 != 0 {
		t.Fatalf("second runConsume: exit code %d", code2)
	}
	if strings.TrimSpace(buf2.String()) != "" {
		t.Errorf("second consume should return empty, got %q", buf2.String())
	}
}

// ─── Part A3: TestTailDefaultsToAllPartitions ────────────────────────────────

func TestTailDefaultsToAllPartitions(t *testing.T) {
	addr := startBroker(t)

	// Create topic with 4 partitions.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "multi-tail", "--partitions", "4"})
	if code != 0 {
		t.Fatal("topic create failed")
	}

	pubClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer pubClient.Close()

	// Publish messages with different keys to spread across partitions.
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	prod := pubClient.NewProducer("multi-tail")
	for _, k := range keys {
		if _, err := prod.Publish(context.Background(), k, []byte("msg-"+k), nil); err != nil {
			t.Fatalf("publish key %q: %v", k, err)
		}
	}

	tailClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial tail: %v", err)
	}
	defer tailClient.Close()

	// Tail with NO --partition flag (defaults to -1 = all partitions).
	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	code = runTail(context.Background(), tailClient, []string{
		"--topic", "multi-tail", "--count", "8", "--format", "json",
	})

	wOut.Close()
	os.Stdout = oldOut

	var buf bytes.Buffer
	io.Copy(&buf, rOut)
	rOut.Close()

	if code != 0 {
		t.Fatalf("runTail: exit code %d", code)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 8 {
		t.Fatalf("want 8 lines, got %d", len(lines))
	}

	partitionsSeen := make(map[int32]bool)
	for _, line := range lines {
		var msg struct {
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
			Key       string `json:"key"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse JSON: %v\nline: %s", err, line)
		}
		partitionsSeen[msg.Partition] = true
	}
	if len(partitionsSeen) < 2 {
		t.Errorf("tail without --partition should see messages from multiple partitions, got partitions %v", partitionsSeen)
	}
}

// ─── Part A3: TestConsumeDefaultsToAllPartitions ─────────────────────────────

func TestConsumeDefaultsToAllPartitions(t *testing.T) {
	addr := startBroker(t)

	// Create topic with 4 partitions.
	_, _, code := capture([]string{"--addr", addr, "topic", "create", "--name", "multi-consume", "--partitions", "4"})
	if code != 0 {
		t.Fatal("topic create failed")
	}

	pubClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer pubClient.Close()

	// Publish messages with different keys to spread across partitions.
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	prod := pubClient.NewProducer("multi-consume")
	for _, k := range keys {
		if _, err := prod.Publish(context.Background(), k, []byte("msg-"+k), nil); err != nil {
			t.Fatalf("publish key %q: %v", k, err)
		}
	}

	consumerClient, err := client.Dial(addr, client.WithDialTimeout(5*time.Second), client.WithReadTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer consumerClient.Close()

	// Consume with NO --partition flag (defaults to -1 = all partitions).
	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	code = runConsume(context.Background(), consumerClient, []string{
		"--topic", "multi-consume", "--group", "multi-consume-grp",
		"--batch", "8", "--format", "json",
	})

	wOut.Close()
	os.Stdout = oldOut

	var buf bytes.Buffer
	io.Copy(&buf, rOut)
	rOut.Close()

	if code != 0 {
		t.Fatalf("runConsume: exit code %d", code)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 8 {
		t.Fatalf("want 8 lines, got %d", len(lines))
	}

	partitionsSeen := make(map[int32]bool)
	for _, line := range lines {
		var msg struct {
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
			Key       string `json:"key"`
		}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse JSON: %v\nline: %s", err, line)
		}
		partitionsSeen[msg.Partition] = true
	}
	if len(partitionsSeen) < 2 {
		t.Errorf("consume without --partition should see messages from multiple partitions, got partitions %v", partitionsSeen)
	}
}
