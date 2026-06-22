package storage_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// decodeAllRecords reads and decodes every v2 record from raw bytes.
// Skips the 5-byte segment file header if present.
func decodeAllRecords(t *testing.T, raw []byte) []*types.Message {
	t.Helper()
	const (
		recV2HeaderSize = 8 + 8 + 2 + 4 + 4 + 1 // 27 bytes
		segHeaderSize   = 5                     // magic(4) + version(1)
	)

	buf := bytes.NewReader(raw)
	// Skip segment file header if the first bytes look like the PSG2 magic.
	if len(raw) >= 5 && raw[0] == 0x50 && raw[1] == 0x53 && raw[2] == 0x47 && raw[3] == 0x32 {
		if _, err := io.ReadFull(buf, make([]byte, segHeaderSize)); err != nil {
			t.Fatalf("skip seg header: %v", err)
		}
	}

	var msgs []*types.Message
	for {
		hdr := make([]byte, recV2HeaderSize)
		if _, err := io.ReadFull(buf, hdr); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			t.Fatalf("read hdr: %v", err)
		}
		keyLen := int(binary.LittleEndian.Uint16(hdr[16:18]))
		payloadLen := int(binary.LittleEndian.Uint32(hdr[18:22]))
		body := make([]byte, keyLen+payloadLen)
		if _, err := io.ReadFull(buf, body); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			t.Fatalf("read body: %v", err)
		}
		var m types.Message
		if err := json.Unmarshal(body[keyLen:], &m); err == nil {
			msgs = append(msgs, &m)
		}
	}
	return msgs
}

// TestSendfileRoundTrip uses a real TCP listener to exercise the sendfile path
// on Linux (and the fallback path on other platforms).
func TestSendfileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	pl, err := storage.OpenPartitionLog(dir, 1<<20, 512, "")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}
	defer pl.Close() //nolint:errcheck

	const msgCount = 100
	for i := 0; i < msgCount; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("sf-%03d", i),
			Topic:   "sendfile-test",
			Key:     fmt.Sprintf("key-%d", i),
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}
		if _, err := pl.Append(msg); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	// Use a real TCP listener so that on Linux the sendfile(2) syscall path
	// inside sendfileSegment is exercised (net.Pipe() returns *net.pipe, not
	// *net.TCPConn, so it always falls back).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			ch <- result{err: err}
			return
		}
		defer conn.Close()
		data, err := io.ReadAll(conn)
		ch <- result{data: data, err: err}
	}()

	dst, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	const maxBytes = 16 * 1024 * 1024 // 16 MiB
	n, sendErr := pl.SendTo(dst, 0, maxBytes)
	dst.Close() // signal EOF to reader goroutine

	res := <-ch
	if sendErr != nil {
		t.Fatalf("SendTo error: %v", sendErr)
	}
	if res.err != nil {
		t.Fatalf("ReadAll error: %v", res.err)
	}
	if int64(len(res.data)) != n {
		t.Fatalf("SendTo reported %d bytes but read %d", n, len(res.data))
	}
	if n == 0 {
		t.Fatal("SendTo sent 0 bytes")
	}

	msgs := decodeAllRecords(t, res.data)
	if len(msgs) != msgCount {
		t.Errorf("decoded %d records, want %d", len(msgs), msgCount)
	}
}

// TestSendfileFallback verifies the fallback read+write path using net.Pipe()
// (which is not a *net.TCPConn, so sendfileSegment always falls back).
func TestSendfileFallback(t *testing.T) {
	dir := t.TempDir()
	pl, err := storage.OpenPartitionLog(dir, 1<<20, 512, "")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}
	defer pl.Close() //nolint:errcheck

	const msgCount = 50
	for i := 0; i < msgCount; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("fb-%03d", i),
			Topic:   "fallback-test",
			Key:     fmt.Sprintf("key-%d", i),
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}
		if _, err := pl.Append(msg); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck
	defer client.Close() //nolint:errcheck

	type result struct {
		n   int64
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := pl.SendTo(server, 0, 16*1024*1024)
		_ = server.Close()
		ch <- result{n, err}
	}()

	raw, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	res := <-ch
	if res.err != nil {
		t.Fatalf("SendTo error: %v", res.err)
	}
	if int64(len(raw)) != res.n {
		t.Fatalf("SendTo reported %d bytes but read %d", res.n, len(raw))
	}

	msgs := decodeAllRecords(t, raw)
	if len(msgs) != msgCount {
		t.Errorf("decoded %d records, want %d", len(msgs), msgCount)
	}
}

// TestSendToStartOffset verifies that SendTo with a non-zero startOffset only
// transmits records whose logical offset is >= startOffset.
func TestSendToStartOffset(t *testing.T) {
	dir := t.TempDir()
	// segMaxBytes=10 forces a rollover after each record (each record is much
	// larger than 10 bytes, so IsFull returns true after the segment header).
	// indexEvery=65536 ensures no index entries are written for these tiny
	// segments, so findPosition always falls back to dataStart.
	const (
		segMaxBytes  = 10
		indexEvery   = 65536
		totalMsgs    = 100
		startOffset  = 50
		wantReceived = totalMsgs - startOffset
	)
	pl, err := storage.OpenPartitionLog(dir, segMaxBytes, indexEvery, "always")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}
	defer pl.Close() //nolint:errcheck

	for i := 0; i < totalMsgs; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("st-%03d", i),
			Topic:   "seekto-test",
			Key:     fmt.Sprintf("k%d", i),
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}
		if _, err := pl.Append(msg); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	type result struct {
		data []byte
		err  error
	}
	rch := make(chan result, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			rch <- result{err: err}
			return
		}
		defer conn.Close()
		data, err := io.ReadAll(conn)
		rch <- result{data: data, err: err}
	}()

	dst, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_, sendErr := pl.SendTo(dst, startOffset, 0)
	dst.Close()

	res := <-rch
	if sendErr != nil {
		t.Fatalf("SendTo: %v", sendErr)
	}
	if res.err != nil {
		t.Fatalf("ReadAll: %v", res.err)
	}

	// Decode all records and verify each has Offset >= startOffset.
	// With tiny segments each holding exactly one record, there is no segment
	// file header to skip (each segment data section IS one record).
	// We use a multi-segment aware decoder.
	var allMsgs []*types.Message
	raw := res.data

	const (
		recV2HeaderSize = 27
		segHeaderSize   = 5
	)

	buf := bytes.NewReader(raw)
	for buf.Len() > 0 {
		// Each tiny segment starts with a 5-byte file header.
		peek := make([]byte, 5)
		if _, err := io.ReadFull(buf, peek); err != nil {
			break
		}
		// Detect segment header magic.
		if peek[0] == 0x50 && peek[1] == 0x53 && peek[2] == 0x47 && peek[3] == 0x32 {
			// It was a segment header; read one record.
			hdr := make([]byte, recV2HeaderSize)
			if _, err := io.ReadFull(buf, hdr); err != nil {
				break
			}
			keyLen := int(binary.LittleEndian.Uint16(hdr[16:18]))
			payloadLen := int(binary.LittleEndian.Uint32(hdr[18:22]))
			body := make([]byte, keyLen+payloadLen)
			if _, err := io.ReadFull(buf, body); err != nil {
				break
			}
			var m types.Message
			if err := json.Unmarshal(body[keyLen:], &m); err == nil {
				allMsgs = append(allMsgs, &m)
			}
		} else {
			// No magic; treat 5 bytes as start of a record header.
			rest := make([]byte, recV2HeaderSize-5)
			if _, err := io.ReadFull(buf, rest); err != nil {
				break
			}
			fullHdr := append(peek, rest...)
			keyLen := int(binary.LittleEndian.Uint16(fullHdr[16:18]))
			payloadLen := int(binary.LittleEndian.Uint32(fullHdr[18:22]))
			body := make([]byte, keyLen+payloadLen)
			if _, err := io.ReadFull(buf, body); err != nil {
				break
			}
			var m types.Message
			if err := json.Unmarshal(body[keyLen:], &m); err == nil {
				allMsgs = append(allMsgs, &m)
			}
		}
	}

	if len(allMsgs) != wantReceived {
		t.Errorf("received %d records, want %d", len(allMsgs), wantReceived)
	}
	for _, m := range allMsgs {
		if m.Offset < startOffset {
			t.Errorf("received record with Offset %d < startOffset %d", m.Offset, startOffset)
		}
	}
}
