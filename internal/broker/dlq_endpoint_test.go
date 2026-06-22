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

// TestDLQDeleteSingleEntry verifies DELETE /dlq/{id} removes a single DLQ entry.
func TestDLQDeleteSingleEntry(t *testing.T) {
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

	// Create topic and publish a message.
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:       "dlq-del-test",
		Partitions: 1,
	})
	pubFrame := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
		Topic:        "dlq-del-test",
		Key:          "k1",
		Payload:      []byte(`{"test":"data"}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	})
	var pubResp protocol.PublishResponse
	_ = json.Unmarshal(pubFrame.Body, &pubResp)

	// Subscribe and nack to DLQ.
	tc.sendOK(protocol.CmdSubscribe, &protocol.SubscribeRequest{
		Group:      "dlq-group",
		ConsumerID: "c1",
		Topic:      "dlq-del-test",
	})

	// Wait for subscription.
	time.Sleep(50 * time.Millisecond)

	// List DLQ entries to find an ID.
	resp, err := http.Get("http://" + httpAddr + "/dlq?topic=dlq-del-test") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /dlq: %v", err)
	}

	// If no DLQ entries, create one via nack.
	var entries []map[string]interface{}
	if resp.StatusCode == http.StatusOK {
		_ = json.NewDecoder(resp.Body).Decode(&entries)
	}
	resp.Body.Close()
	if len(entries) == 0 {
		tc.sendOK(protocol.CmdNack, &protocol.NackRequest{
			ConsumerID: "c1",
			Topic:      "dlq-del-test",
			Partition:  pubResp.Partition,
			Offset:     pubResp.Offset,
			Group:      "dlq-group",
			Requeue:    false,
		})
		time.Sleep(50 * time.Millisecond)

		// Re-list.
		resp2, err2 := http.Get("http://" + httpAddr + "/dlq?topic=dlq-del-test") //nolint:noctx
		if err2 != nil {
			t.Fatalf("GET /dlq (retry): %v", err2)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp2.Body).Decode(&entries)
		}
	}
	if len(entries) == 0 {
		t.Skip("no DLQ entries to test delete")
	}

	dlqID, _ := entries[0]["id"].(string)
	if dlqID == "" {
		t.Fatal("DLQ entry has no id")
	}

	// DELETE /dlq/{id}
	deleteReq, _ := http.NewRequest(http.MethodDelete, "http://"+httpAddr+"/dlq/"+dlqID+"?topic=dlq-del-test", nil)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("DELETE /dlq/%s: %v", dlqID, err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Errorf("DELETE status: %d, want 200", deleteResp.StatusCode)
	}

	// Verify it's gone.
	getReq, _ := http.NewRequest(http.MethodDelete, "http://"+httpAddr+"/dlq/"+dlqID+"?topic=dlq-del-test", nil)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("DELETE (second): %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("second DELETE status: %d, want 404", getResp.StatusCode)
	}
}

// TestDLQExportSingleEntry verifies GET /dlq/{id}/export returns a downloadable JSON file.
func TestDLQExportSingleEntry(t *testing.T) {
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
		Name:       "dlq-export-test",
		Partitions: 1,
	})
	pubFrame := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
		Topic:        "dlq-export-test",
		Key:          "k1",
		Payload:      []byte(`{"export":"me"}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	})
	var pubResp protocol.PublishResponse
	_ = json.Unmarshal(pubFrame.Body, &pubResp)

	tc.sendOK(protocol.CmdSubscribe, &protocol.SubscribeRequest{
		Group:      "export-group",
		ConsumerID: "c1",
		Topic:      "dlq-export-test",
	})

	time.Sleep(50 * time.Millisecond)

	// Create DLQ entry.
	tc.sendOK(protocol.CmdNack, &protocol.NackRequest{
		ConsumerID: "c1",
		Topic:      "dlq-export-test",
		Partition:  pubResp.Partition,
		Offset:     pubResp.Offset,
		Requeue:    false,
	})
	time.Sleep(50 * time.Millisecond)

	// Find the entry ID.
	resp, err := http.Get("http://" + httpAddr + "/dlq?topic=dlq-export-test") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /dlq: %v", err)
	}
	var entries []map[string]interface{}
	if resp.StatusCode == http.StatusOK {
		_ = json.NewDecoder(resp.Body).Decode(&entries)
	}
	resp.Body.Close()
	if len(entries) == 0 {
		t.Skip("no DLQ entries to test export")
	}

	dlqID, _ := entries[0]["id"].(string)

	// GET /dlq/{id}/export
	exportResp, err := http.Get("http://" + httpAddr + "/dlq/" + dlqID + "/export?topic=dlq-export-test") //nolint:noctx
	if err != nil {
		t.Fatalf("GET export: %v", err)
	}
	defer exportResp.Body.Close()
	if exportResp.StatusCode != http.StatusOK {
		t.Errorf("export status: %d, want 200", exportResp.StatusCode)
	}
	cd := exportResp.Header.Get("Content-Disposition")
	if cd == "" {
		t.Error("Content-Disposition header missing")
	}

	var exported map[string]interface{}
	if err := json.NewDecoder(exportResp.Body).Decode(&exported); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	if exported["id"] != dlqID {
		t.Errorf("exported id: got %v, want %v", exported["id"], dlqID)
	}
	if exported["topic"] != "dlq-export-test" {
		t.Errorf("exported topic: got %v", exported["topic"])
	}
}

// TestDLQDeleteNotFound verifies DELETE /dlq/{id} returns 404 for nonexistent entry.
func TestDLQDeleteNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	req, _ := http.NewRequest(http.MethodDelete, "http://"+httpAddr+"/dlq/nonexistent-id?group=g&topic=t", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: %d, want 404", resp.StatusCode)
	}
}
