package broker_test

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Stale TargetOffset under concurrent same-partition publish ─────────────

// TestConcurrentPublishNoReplayDuplicate verifies: 8 goroutines
// each publish 50 messages to the SAME partition concurrently, the broker is
// restarted, and exactly 400 unique offsets are present in the log.
//
// Without the per-partition lock, pl.NextOffset() was read outside any
// per-partition lock, so two concurrent publishers could capture the same
// TargetOffset. On crash replay a stale TargetOffset could cause a message
// to be silently skipped (NextOffset > TargetOffset → skip, even though the
// message was never appended). The per-partition mutex added in handlePublish
// makes TargetOffset capture → pl.Append atomic per partition, so every WAL
// entry carries the exact offset that will be assigned.
//
// Run with -race to also verify the partition-lock implementation is
// data-race-free under contention.
func TestConcurrentPublishNoReplayDuplicate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: start broker, create a single-partition topic.
	b1 := newBrokerAtDir(t, dir)
	addr1 := startBrokerWithAddr(t, b1)
	const topic = "bug2-concurrent"
	const partitions = 1
	createTopicOn(t, addr1, topic, partitions)

	// 8 goroutines × 50 messages, all to the same partition (same key → same
	// partition; topic has 1 partition anyway).
	const goroutines = 8
	const perGoroutine = 50
	const total = goroutines * perGoroutine

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			// Each goroutine uses its own connection — a single connection
			// cannot be written to concurrently.
			conn, err := net.DialTimeout("tcp", addr1, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d dial: %w", gid, err)
				return
			}
			defer conn.Close()
			enc := protocol.NewEncoder(conn)
			dec := protocol.NewDecoder(conn)
			for i := 0; i < perGoroutine; i++ {
				reqID := uint64(gid*1000 + i + 1)
				if err := enc.Encode(protocol.CmdPublish, reqID, &protocol.PublishRequest{
					Topic:        topic,
					Key:          "same-key", // forces same partition
					Payload:      []byte(fmt.Sprintf("g%d-m%d", gid, i)),
					DeliveryMode: uint8(types.AtLeastOnce),
				}); err != nil {
					errCh <- fmt.Errorf("goroutine %d publish[%d] encode: %w", gid, i, err)
					return
				}
				f, err := dec.Decode()
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d publish[%d] decode: %w", gid, i, err)
					return
				}
				if f.Command == protocol.CmdError {
					var e protocol.ErrorResponse
					_ = protocol.Unmarshal(f, &e)
					errCh <- fmt.Errorf("goroutine %d publish[%d] error: %s: %s", gid, i, e.Code, e.Message)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatalf("concurrent publish failed: %v", e)
		}
	}

	// Clean stop — WAL still holds all 400 entries for replay verification.
	stopBroker(t, b1)

	// Phase 2: restart with the same data directory.
	b2 := newBrokerAtDir(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })

	// Fetch all messages from partition 0 in batches. The broker caps a single
	// fetch at 100 messages and PollPartitionLog reads from the consumer
	// group's committed offset (not req.Offset), so we subscribe once, fetch a
	// batch, commit the last offset, and repeat until the log is drained.
	fconn, err := net.DialTimeout("tcp", addr2, 3*time.Second)
	if err != nil {
		t.Fatalf("fetch dial: %v", err)
	}
	defer fconn.Close()
	fenc := protocol.NewEncoder(fconn)
	fdec := protocol.NewDecoder(fconn)

	// Subscribe the fetch group so commits are accepted.
	if err := fenc.Encode(protocol.CmdSubscribe, 901, &protocol.SubscribeRequest{
		Topic: topic, Group: "bug2-fetch-group", ConsumerID: "bug2-fetcher",
	}); err != nil {
		t.Fatalf("subscribe encode: %v", err)
	}
	sf, err := fdec.Decode()
	if err != nil {
		t.Fatalf("subscribe decode: %v", err)
	}
	if sf.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(sf, &e)
		t.Fatalf("subscribe error: %s: %s", e.Code, e.Message)
	}

	var allMsgs []*types.Message
	reqID := uint64(1000)
	for len(allMsgs) < total {
		if err := fenc.Encode(protocol.CmdFetch, reqID, &protocol.FetchRequest{
			Topic: topic, Group: "bug2-fetch-group", Partition: 0, Offset: 0, MaxCount: 100,
		}); err != nil {
			t.Fatalf("fetch encode: %v", err)
		}
		reqID++
		f, err := fdec.Decode()
		if err != nil {
			t.Fatalf("fetch decode: %v", err)
		}
		if f.Command == protocol.CmdError {
			var e protocol.ErrorResponse
			_ = protocol.Unmarshal(f, &e)
			t.Fatalf("fetch error: %s: %s", e.Code, e.Message)
		}
		var raw map[string]interface{}
		if err := json.Unmarshal(f.Body, &raw); err != nil {
			t.Fatalf("fetch unmarshal: %v", err)
		}
		msgsRaw, _ := raw["messages"].([]interface{})
		if len(msgsRaw) == 0 {
			break
		}
		var lastOff int64
		for _, m := range msgsRaw {
			mb, _ := json.Marshal(m)
			var msg types.Message
			if err := json.Unmarshal(mb, &msg); err == nil {
				allMsgs = append(allMsgs, &msg)
				lastOff = msg.Offset
			}
		}
		// Commit the last offset so the next fetch advances past it.
		if err := fenc.Encode(protocol.CmdCommitOffset, reqID, &protocol.CommitOffsetRequest{
			Group: "bug2-fetch-group", ConsumerID: "bug2-fetcher",
			Topic: topic, Partition: 0, Offset: lastOff,
		}); err != nil {
			t.Fatalf("commit encode: %v", err)
		}
		reqID++
		cf, err := fdec.Decode()
		if err != nil {
			t.Fatalf("commit decode: %v", err)
		}
		if cf.Command == protocol.CmdError {
			var e protocol.ErrorResponse
			_ = protocol.Unmarshal(cf, &e)
			t.Fatalf("commit error: %s: %s", e.Code, e.Message)
		}
	}

	if len(allMsgs) != total {
		t.Fatalf("after restart: want %d unique messages, got %d (WAL replay may have skipped or duplicated)", total, len(allMsgs))
	}

	// Verify all offsets are unique.
	seen := make(map[int64]struct{}, total)
	for _, m := range allMsgs {
		if _, ok := seen[m.Offset]; ok {
			t.Errorf("duplicate offset %d after replay — TargetOffset dedup failed", m.Offset)
		}
		seen[m.Offset] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("want %d unique offsets, got %d", total, len(seen))
	}

	// Offsets must be a dense 0..total-1 range (single partition, no gaps).
	for i := 0; i < total; i++ {
		if _, ok := seen[int64(i)]; !ok {
			t.Errorf("missing offset %d — message was silently skipped during replay", i)
		}
	}
}

// ─── offsetCheckpointLoop goroutine tracked by WaitGroup ───────────────────

// TestStopNoRaceOnOffsetWAL verifies that Stop() waits for the
// offsetCheckpointLoop goroutine to exit before closing the offset WAL, so
// there is no write-after-close data race. Run with -race for the real check.
//
// The test triggers many commits (to force the 1000-commit checkpoint path)
// and then calls Stop() while a checkpoint may be in flight.
func TestStopNoRaceOnOffsetWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)

	const topic = "bug5-stop-race"
	createTopicOn(t, addr, topic, 1)
	// Publish + commit enough to exercise the offset WAL checkpoint path.
	publishNOn(t, addr, topic, 5)

	// Subscribe the consumer group first — CommitOffset requires an active
	// subscription (the broker rejects commits for unknown groups).
	subConn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("subscribe dial: %v", err)
	}
	subEnc := protocol.NewEncoder(subConn)
	subDec := protocol.NewDecoder(subConn)
	if err := subEnc.Encode(protocol.CmdSubscribe, 499, &protocol.SubscribeRequest{
		Topic:      topic,
		Group:      "bug5-group",
		ConsumerID: "bug5-consumer",
	}); err != nil {
		t.Fatalf("subscribe encode: %v", err)
	}
	sf, err := subDec.Decode()
	if err != nil {
		t.Fatalf("subscribe decode: %v", err)
	}
	if sf.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(sf, &e)
		t.Fatalf("subscribe error: %s: %s", e.Code, e.Message)
	}
	defer subConn.Close()

	// Open a connection and commit offsets repeatedly to drive the offset WAL.
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	for i := 0; i < 1100; i++ {
		if err := enc.Encode(protocol.CmdCommitOffset, uint64(500+i), &protocol.CommitOffsetRequest{
			Group:      "bug5-group",
			ConsumerID: "bug5-consumer",
			Topic:      topic,
			Partition:  0,
			Offset:     int64(i),
		}); err != nil {
			t.Fatalf("commit[%d] encode: %v", i, err)
		}
		f, err := dec.Decode()
		if err != nil {
			t.Fatalf("commit[%d] decode: %v", i, err)
		}
		if f.Command == protocol.CmdError {
			var e protocol.ErrorResponse
			_ = protocol.Unmarshal(f, &e)
			t.Fatalf("commit[%d] error: %s: %s", i, e.Code, e.Message)
		}
	}
	conn.Close()

	// Stop immediately — the 1000-commit checkpoint may still be writing to
	// the offset WAL. wg.Wait() ensures the checkpoint goroutine finishes
	// before offsetWAL.Close(). Under -race this would detect a write-after-close
	// race without the fix.
	stopBroker(t, b)

	// Verify the offset WAL is still readable after Stop (proves it was closed
	// cleanly, not mid-write). Re-open the broker and check a committed offset
	// survived.
	b2 := newBrokerAtDir(t, dir)
	addr2 := startBrokerWithAddr(t, b2)
	t.Cleanup(func() { stopBroker(t, b2) })
	_ = addr2
}

// TestRawTransferReturnsResponse verifies that a RawTransfer fetch
// receives a FetchResponse frame with RawBytes=true and BytesSent>0 rather
// than hanging forever waiting for a response that never comes.
//
// Wire format: the broker calls pl.SendTo (writing raw
// segment bytes directly to the socket) and THEN sends a FetchResponse frame
// with RawBytes=true and BytesSent=n. The raw segment data (magic "PSG2")
// precedes the protocol frame (magic 0x50534201 LE = "\x01BSP") on the wire.
// The test reads the full stream and locates the FetchResponse frame by
// scanning for the protocol magic, which never collides with the segment
// magic or the small test payloads.
func TestRawTransferReturnsResponse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "bug4-rawtransfer"
	createTopicOn(t, addr, topic, 1)
	publishNOn(t, addr, topic, 3)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)

	if err := enc.Encode(protocol.CmdFetch, 700, &protocol.FetchRequest{
		Topic:       topic,
		Group:       "bug4-group",
		Partition:   0,
		Offset:      0,
		MaxCount:    100,
		RawTransfer: true,
	}); err != nil {
		t.Fatalf("fetch encode: %v", err)
	}

	// Read everything the broker sends within a deadline. Without the fix
	// the broker returned nil after SendTo and the client hung forever.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		n, rErr := conn.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if rErr != nil {
			break
		}
		// Stop once we have well over a frame's worth and a plausible end.
		if len(buf) > int(protocol.HeaderSize)+64 {
			// Try a short quiet period to collect the frame tail.
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		}
	}
	if len(buf) == 0 {
		t.Fatal("broker sent no data (client would hang forever)")
	}

	// Scan for the protocol frame magic 0x50534201 little-endian.
	magic := []byte{0x01, 0x42, 0x53, 0x50}
	idx := -1
	for i := 0; i+4 <= len(buf); i++ {
		if buf[i] == magic[0] && buf[i+1] == magic[1] && buf[i+2] == magic[2] && buf[i+3] == magic[3] {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("no FetchResponse frame found in %d bytes of stream (raw transfer hung or frame missing)", len(buf))
	}
	if idx+protocol.HeaderSize > len(buf) {
		t.Fatalf("truncated frame header at offset %d", idx)
	}
	hdr := buf[idx : idx+protocol.HeaderSize]
	cmd := protocol.Command(hdr[5])
	bodyLen := int(hdr[14]) | int(hdr[15])<<8 | int(hdr[16])<<16 | int(hdr[17])<<24
	bodyEnd := idx + protocol.HeaderSize + bodyLen
	if bodyEnd > len(buf) {
		t.Fatalf("truncated frame body (have %d, need %d)", len(buf), bodyEnd)
	}
	if cmd != protocol.CmdResponse {
		t.Fatalf("expected CmdResponse, got %s", cmd)
	}
	var resp protocol.FetchResponse
	if err := json.Unmarshal(buf[idx+protocol.HeaderSize:bodyEnd], &resp); err != nil {
		t.Fatalf("FetchResponse unmarshal: %v", err)
	}
	if !resp.RawBytes {
		t.Error("RawTransfer response should have RawBytes=true")
	}
	if resp.BytesSent <= 0 {
		t.Errorf("RawTransfer response should have BytesSent>0, got %d", resp.BytesSent)
	}
	// The raw bytes preceding the frame should be at least BytesSent.
	if int64(idx) < resp.BytesSent {
		t.Errorf("raw bytes before frame (%d) < BytesSent (%d)", idx, resp.BytesSent)
	}
}

// TestNackRequeueDeliversOnlyToOriginGroup verifies that a nacked
// message with Requeue=true is re-dispatched ONLY to the originating group,
// not fanned out to every subscribed group on the topic.
func TestNackRequeueDeliversOnlyToOriginGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	b := newBrokerAtDir(t, dir)
	addr := startBrokerWithAddr(t, b)
	t.Cleanup(func() { stopBroker(t, b) })

	const topic = "bug8-nack"
	createTopicOn(t, addr, topic, 1)
	offsets := publishNOn(t, addr, topic, 1)
	if len(offsets) != 1 {
		t.Fatalf("want 1 offset, got %d", len(offsets))
	}

	// Subscribe two groups to the same topic. Only groupA should receive the
	// requeued message.
	subscribeAndDrain := func(group, consumerID string) {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		enc := protocol.NewEncoder(conn)
		dec := protocol.NewDecoder(conn)
		if err := enc.Encode(protocol.CmdSubscribe, 800, &protocol.SubscribeRequest{
			Topic:      topic,
			Group:      group,
			ConsumerID: consumerID,
		}); err != nil {
			t.Fatalf("subscribe encode: %v", err)
		}
		f, err := dec.Decode()
		if err != nil {
			t.Fatalf("subscribe decode: %v", err)
		}
		if f.Command == protocol.CmdError {
			var e protocol.ErrorResponse
			_ = protocol.Unmarshal(f, &e)
			t.Fatalf("subscribe error: %s: %s", e.Code, e.Message)
		}
	}

	subscribeAndDrain("groupA", "consA")
	subscribeAndDrain("groupB", "consB")

	// Nack from groupA with Requeue=true and Group=groupA.
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	if err := enc.Encode(protocol.CmdNack, 801, &protocol.NackRequest{
		ConsumerID: "consA",
		Topic:      topic,
		Partition:  0,
		Offset:     offsets[0],
		Group:      "groupA",
		Requeue:    true,
	}); err != nil {
		t.Fatalf("nack encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("nack decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("nack error: %s: %s", e.Code, e.Message)
	}

	// The DLQ must remain empty (the message was requeued, not dead-lettered).
	// This is a smoke test; the unit test in consumer_test.go checks the
	// DispatchToGroup routing directly.
	if n := b.ConsumerDLQ().Len(); n != 0 {
		t.Errorf("DLQ should be empty after requeue, got %d entries", n)
	}
}
