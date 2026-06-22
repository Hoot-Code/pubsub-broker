package broker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// TestConsumerGroupDetailEndpoint verifies GET /consumers/{group}/{topic}
// returns detailed consumer group information.
func TestConsumerGroupDetailEndpoint(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	addr := startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	tc := dialBroker(t, addr)

	// Create a topic.
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:       "cgroup-test",
		Partitions: 2,
	})

	// Publish a message so there's data.
	pubFrame := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
		Topic:        "cgroup-test",
		Key:          "k1",
		Payload:      []byte(`{"msg":"hello"}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	})
	var pubResp protocol.PublishResponse
	if err := json.Unmarshal(pubFrame.Body, &pubResp); err != nil {
		t.Fatalf("unmarshal publish: %v", err)
	}

	// Subscribe a consumer to create the group.
	tc.sendOK(protocol.CmdSubscribe, &protocol.SubscribeRequest{
		Group:      "test-group",
		ConsumerID: "c1",
		Topic:      "cgroup-test",
	})

	// Give a moment for the subscription to register.
	time.Sleep(50 * time.Millisecond)

	// GET /consumers/test-group/cgroup-test
	resp, err := http.Get("http://" + httpAddr + "/consumers/test-group/cgroup-test") //nolint:noctx
	if err != nil {
		t.Fatalf("GET consumer group detail: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}

	var detail map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if detail["group"] != "test-group" {
		t.Errorf("group: got %v, want test-group", detail["group"])
	}
	if detail["topic"] != "cgroup-test" {
		t.Errorf("topic: got %v, want cgroup-test", detail["topic"])
	}
	if detail["rebalancing"] != false {
		t.Error("rebalancing should be false after subscription settles")
	}
	if detail["max_retries"] == nil {
		t.Error("max_retries missing")
	}
	if detail["retry_delay_ms"] == nil {
		t.Error("retry_delay_ms missing")
	}
	if detail["failed_message_count"] == nil {
		t.Error("failed_message_count missing")
	}

	members, ok := detail["members"].([]interface{})
	if !ok || len(members) == 0 {
		t.Error("members should be a non-empty array")
	}

	partitions, ok := detail["partitions"].([]interface{})
	if !ok || len(partitions) == 0 {
		t.Error("partitions should be a non-empty array")
	}

	t.Logf("consumer group detail: %v", detail)
}

// TestConsumerGroupDetailNotFound verifies 404 for nonexistent consumer group.
func TestConsumerGroupDetailNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	resp, err := http.Get("http://" + httpAddr + "/consumers/nonexistent/notopic") //nolint:noctx
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("nonexistent group: status %d, want 404", resp.StatusCode)
	}
}
