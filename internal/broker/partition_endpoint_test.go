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

// TestPartitionDetailEndpoint verifies GET /topics/{topic}/partitions/{partition}
// returns correct partition metadata including segment stats and ISR info.
func TestPartitionDetailEndpoint(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	addr := startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	// Create a topic with 2 partitions.
	tc := dialBroker(t, addr)
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:              "part-test",
		Partitions:        2,
		ReplicationFactor: 1,
	})

	// Publish a message to partition 0.
	pubFrame := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
		Topic:        "part-test",
		Key:          "k1",
		Payload:      []byte(`{"data":"hello"}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	})
	var pubResp protocol.PublishResponse
	if err := json.Unmarshal(pubFrame.Body, &pubResp); err != nil {
		t.Fatalf("unmarshal publish: %v", err)
	}

	// GET /topics/part-test/partitions/0
	resp, err := http.Get("http://" + httpAddr + "/topics/part-test/partitions/0") //nolint:noctx
	if err != nil {
		t.Fatalf("GET partition detail: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}

	var detail map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if detail["topic"] != "part-test" {
		t.Errorf("topic: got %v, want part-test", detail["topic"])
	}
	if detail["partition"] != float64(0) {
		t.Errorf("partition: got %v, want 0", detail["partition"])
	}
	if detail["leader_node_id"] == nil || detail["leader_node_id"] == "" {
		t.Error("leader_node_id should be non-empty")
	}
	if detail["wal_status"] == nil {
		t.Error("wal_status missing")
	}
	if detail["segment_count"] == nil {
		t.Error("segment_count missing")
	}
	if detail["segment_total_bytes"] == nil {
		t.Error("segment_total_bytes missing")
	}
	if detail["active_segment_bytes"] == nil {
		t.Error("active_segment_bytes missing")
	}
	if detail["isr"] == nil {
		t.Error("isr missing")
	}
	if detail["replicas"] == nil {
		t.Error("replicas missing")
	}
	if _, ok := detail["replicas"].([]interface{}); !ok {
		t.Errorf("replicas should be a slice, got %T", detail["replicas"])
	}
	t.Logf("partition detail: %v", detail)
}

// TestPartitionDetailNotFound verifies 404 for nonexistent topic or partition.
func TestPartitionDetailNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	// Nonexistent topic.
	resp, err := http.Get("http://" + httpAddr + "/topics/nonexistent/partitions/0") //nolint:noctx
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("nonexistent topic: status %d, want 404", resp.StatusCode)
	}

	// Create a topic, then request an out-of-range partition.
	tc := dialBroker(t, b.Addr())
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:       "short-topic",
		Partitions: 1,
	})
	resp2, err := http.Get("http://" + httpAddr + "/topics/short-topic/partitions/99") //nolint:noctx
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("out-of-range partition: status %d, want 404", resp2.StatusCode)
	}
}

// TestPartitionListEndpoint verifies GET /topics/{topic}/partitions returns
// an array of partition details for every partition in the topic.
func TestPartitionListEndpoint(t *testing.T) {
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
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:       "list-test",
		Partitions: 3,
	})

	resp, err := http.Get("http://" + httpAddr + "/topics/list-test/partitions") //nolint:noctx
	if err != nil {
		t.Fatalf("GET partition list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}

	var partitions []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&partitions); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(partitions) != 3 {
		t.Fatalf("partition count: got %d, want 3", len(partitions))
	}

	for i, p := range partitions {
		if p["topic"] != "list-test" {
			t.Errorf("partition %d topic: got %v", i, p["topic"])
		}
		if p["partition"] != float64(i) {
			t.Errorf("partition %d index: got %v", i, p["partition"])
		}
		if p["segment_count"] == nil {
			t.Errorf("partition %d missing segment_count", i)
		}
	}
}
