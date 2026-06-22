package client_test

import (
	"net"
	"testing"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
)

// sendCreateTopic sends a CREATE_TOPIC command over an existing raw connection.
// This helper uses only pkg/protocol — no internal/ imports.
func sendCreateTopic(t *testing.T, conn net.Conn, topic string, partitions int) {
	t.Helper()
	enc := proto.NewEncoder(conn)
	dec := proto.NewDecoder(conn)

	if err := enc.Encode(proto.CmdCreateTopic, 1, &proto.CreateTopicRequest{
		Name:              topic,
		Partitions:        partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("sendCreateTopic encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("sendCreateTopic decode: %v", err)
	}
	if f.Command == proto.CmdError {
		var e proto.ErrorResponse
		_ = proto.Unmarshal(f, &e)
		// TOPIC_EXISTS is fine — topic was already created.
		if e.Code != "TOPIC_EXISTS" {
			t.Fatalf("sendCreateTopic: broker error %s: %s", e.Code, e.Message)
		}
	}
}
