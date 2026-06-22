// Package protocol defines the binary wire protocol used by the broker client SDK.
//
// This package is a standalone copy of the internal protocol codec so that
// pkg/client has no imports from internal/. The wire format is identical:
// both sides must agree on magic bytes, command codes, and JSON body encoding.
//
// Frame layout:
//
//	┌──────────────┬─────────┬──────────┬──────────────┬──────────┬───────────────┐
//	│ Magic   (4B) │ Ver (1B)│ Cmd (1B) │ ReqID   (8B) │ Len (4B) │ Body (Len B)  │
//	└──────────────┴─────────┴──────────┴──────────────┴──────────┴───────────────┘
//
// All multi-byte integers are little-endian.
// Body is JSON-encoded.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Wire constants ───────────────────────────────────────────────────────────

const (
	// Magic identifies valid broker frames.
	Magic uint32 = 0x50534201 // "PSB\x01"
	// Version is the current protocol version.
	Version uint8 = 1
	// HeaderSize is the fixed header length in bytes.
	HeaderSize = 4 + 1 + 1 + 8 + 4 // 18 bytes
	// MaxBodySize caps frame body size at 16 MiB.
	MaxBodySize uint32 = 16 * 1024 * 1024
)

// ─── Commands ────────────────────────────────────────────────────────────────

// Command identifies the type of a protocol frame.
type Command uint8

const (
	CmdPublish      Command = 0x01
	CmdSubscribe    Command = 0x02
	CmdUnsubscribe  Command = 0x03
	CmdFetch        Command = 0x04
	CmdAck          Command = 0x05
	CmdNack         Command = 0x06
	CmdCommitOffset Command = 0x07
	CmdCreateTopic  Command = 0x08
	CmdDeleteTopic  Command = 0x09
	CmdListTopics   Command = 0x0A
	CmdAuth         Command = 0x0B
	CmdPing         Command = 0x0C
	CmdPong         Command = 0x0D
	CmdResponse     Command = 0x0E
	CmdError        Command = 0x0F
	// CmdBatchPublish publishes multiple messages in a single round-trip.
	CmdBatchPublish Command = 0x10
	// CmdPush is sent by the broker to push messages to a subscribed connection.
	CmdPush Command = 0x20
	// CmdSeek seeks a consumer group to a timestamp or endpoint.
	CmdSeek Command = 0x30
	// CmdResetGroup resets all committed offsets for a consumer group+topic to 0.
	CmdResetGroup Command = 0x31
)

// String returns the human-readable name of a Command byte.
func (c Command) String() string {
	switch c {
	case CmdPublish:
		return "PUBLISH"
	case CmdSubscribe:
		return "SUBSCRIBE"
	case CmdUnsubscribe:
		return "UNSUBSCRIBE"
	case CmdFetch:
		return "FETCH"
	case CmdAck:
		return "ACK"
	case CmdNack:
		return "NACK"
	case CmdCommitOffset:
		return "COMMIT_OFFSET"
	case CmdCreateTopic:
		return "CREATE_TOPIC"
	case CmdDeleteTopic:
		return "DELETE_TOPIC"
	case CmdListTopics:
		return "LIST_TOPICS"
	case CmdAuth:
		return "AUTH"
	case CmdPing:
		return "PING"
	case CmdPong:
		return "PONG"
	case CmdResponse:
		return "RESPONSE"
	case CmdError:
		return "ERROR"
	case CmdBatchPublish:
		return "BATCH_PUBLISH"
	case CmdPush:
		return "PUSH"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(c))
	}
}

// ─── Frame ────────────────────────────────────────────────────────────────────

// Frame is a decoded protocol frame.
type Frame struct {
	Command   Command
	RequestID uint64
	Body      []byte
}

// ─── Request/Response bodies ──────────────────────────────────────────────────

// AuthRequest authenticates a client connection.
type AuthRequest struct {
	APIKey   string `json:"api_key"`
	ClientID string `json:"client_id"`
}

// AuthResponse confirms authentication success/failure.
type AuthResponse struct {
	OK     bool     `json:"ok"`
	Reason string   `json:"reason,omitempty"`
	Perms  []string `json:"permissions,omitempty"`
}

// PublishRequest publishes one message.
type PublishRequest struct {
	Topic        string            `json:"topic"`
	Key          string            `json:"key,omitempty"`
	Payload      []byte            `json:"payload"`
	Headers      map[string]string `json:"headers,omitempty"`
	DeliveryMode uint8             `json:"delivery_mode"`
	// SeqNum is required for ExactlyOnce delivery; 0 means not provided.
	SeqNum uint64 `json:"seq_num,omitempty"`
	// Codec overrides the topic-level compression codec for this message only (B4).
	Codec uint8 `json:"codec,omitempty"`
}

// PublishResponse confirms a message was stored.
type PublishResponse struct {
	MessageID string `json:"message_id"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// BatchPublishRequest publishes multiple messages in one round-trip.
type BatchPublishRequest struct {
	Messages []PublishRequest `json:"messages"`
}

// BatchPublishResponse contains one PublishResponse per input message.
type BatchPublishResponse struct {
	Results []PublishResponse `json:"results"`
}

// SubscribeRequest subscribes a consumer to a topic.
type SubscribeRequest struct {
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	ConsumerID string `json:"consumer_id"`
	// Push enables server-initiated push delivery.
	Push bool `json:"push,omitempty"`
}

// PushFrame is sent by the broker over CmdPush.
type PushFrame struct {
	Topic      string           `json:"topic"`
	Group      string           `json:"group"`
	ConsumerID string           `json:"consumer_id"`
	Partition  int32            `json:"partition"`
	Messages   []*types.Message `json:"messages"`
}

// FetchRequest requests messages from a topic-partition.
type FetchRequest struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxCount  int    `json:"max_count"`
	// RawTransfer enables zero-copy sendfile delivery (Linux only, opt-in).
	// When true the broker writes raw segment bytes directly to the socket
	// and the caller must decode the wire format itself.
	RawTransfer bool `json:"raw_transfer,omitempty"`
}

// FetchResponse contains fetched messages.
//
// When RawBytes is true the message payload was streamed out-of-band directly
// to the client's TCP socket (RawTransfer / sendfile fast-path) and Messages
// is empty/nil. The client MUST NOT attempt to unmarshal Messages in this case;
// instead it should read BytesSent raw bytes from the socket.
type FetchResponse struct {
	Topic     string           `json:"topic"`
	Partition int32            `json:"partition"`
	Messages  []*types.Message `json:"messages"`
	// RawBytes is true when the payload was transferred out-of-band via
	// sendfile (RawTransfer).
	RawBytes bool `json:"raw_bytes,omitempty"`
	// BytesSent is the number of raw bytes written to the socket when
	// RawBytes is true. Zero when RawBytes is false.
	BytesSent int64 `json:"bytes_sent,omitempty"`
}

// CommitOffsetRequest commits a consumer offset.
type CommitOffsetRequest struct {
	Group      string `json:"group"`
	ConsumerID string `json:"consumer_id"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
}

// CreateTopicRequest creates a new topic.
type CreateTopicRequest struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	RetentionHours    int    `json:"retention_hours,omitempty"`
	// CompactionMode selects the topic's retention strategy.
	// "" or "delete" = age/size retention (default). "compact" = key-based
	// log compaction; see pkg/types.TopicConfig.CompactionMode.
	CompactionMode string `json:"compaction_mode,omitempty"`
}

// DeleteTopicRequest deletes a topic.
type DeleteTopicRequest struct {
	Name string `json:"name"`
}

// SeekRequest seeks a consumer group to a specific timestamp, beginning, or end.
type SeekRequest struct {
	Topic       string `json:"topic"`
	Group       string `json:"group"`
	TimestampNs int64  `json:"timestamp_ns"` // 0 = seek to beginning
	ToEnd       bool   `json:"to_end"`       // seek to latest offset
	// Offset seeks directly to the given absolute offset (>= 0).
	// When non-nil it takes precedence over TimestampNs and ToEnd.
	Offset *int64 `json:"offset,omitempty"`
}

// SeekResponse carries the new offset per partition after a seek operation.
type SeekResponse struct {
	Offsets map[string]int64 `json:"offsets"`
}

// ResetGroupRequest resets all committed offsets for a consumer group+topic to 0.
type ResetGroupRequest struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
}

// ErrorResponse is returned when a command fails.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// OKResponse is a simple success acknowledgement.
type OKResponse struct {
	OK bool `json:"ok"`
}

// ─── Codec ───────────────────────────────────────────────────────────────────

// Encoder writes frames to a writer.
// Encoder is NOT goroutine-safe; callers must serialise concurrent writes.
type Encoder struct {
	w io.Writer
}

// flusher is the interface implemented by bufio.Writer and similar types.
type flusher interface {
	Flush() error
}

// Flush flushes any buffered data to the underlying writer.
// It is a no-op when the underlying writer does not implement Flush.
func (e *Encoder) Flush() error {
	if f, ok := e.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

// NewEncoder creates an Encoder.
func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

// Encode serialises and writes a frame.
func (e *Encoder) Encode(cmd Command, requestID uint64, body interface{}) error {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode body: %w", err)
		}
	}
	if uint32(len(bodyBytes)) > MaxBodySize {
		return fmt.Errorf("body too large: %d bytes", len(bodyBytes))
	}
	return e.writeFrame(Frame{Command: cmd, RequestID: requestID, Body: bodyBytes})
}

func (e *Encoder) writeFrame(f Frame) error {
	var h [HeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], Magic)
	h[4] = Version
	h[5] = uint8(f.Command)
	binary.LittleEndian.PutUint64(h[6:14], f.RequestID)
	binary.LittleEndian.PutUint32(h[14:18], uint32(len(f.Body)))
	if _, err := e.w.Write(h[:]); err != nil {
		return err
	}
	if len(f.Body) > 0 {
		_, err := e.w.Write(f.Body)
		return err
	}
	return nil
}

// Decoder reads frames from a reader.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a Decoder.
func NewDecoder(r io.Reader) *Decoder { return &Decoder{r: r} }

// Decode reads and validates the next frame from the connection.
func (d *Decoder) Decode() (*Frame, error) {
	hdr := make([]byte, HeaderSize)
	if _, err := io.ReadFull(d.r, hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("invalid magic: 0x%08X (want 0x%08X)", magic, Magic)
	}
	ver := hdr[4]
	if ver != Version {
		return nil, fmt.Errorf("unsupported version: %d", ver)
	}

	cmd := Command(hdr[5])
	reqID := binary.LittleEndian.Uint64(hdr[6:14])
	bodyLen := binary.LittleEndian.Uint32(hdr[14:18])

	if bodyLen > MaxBodySize {
		return nil, fmt.Errorf("body too large: %d bytes", bodyLen)
	}
	if bodyLen > math.MaxInt32 {
		return nil, fmt.Errorf("body length overflow")
	}

	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(d.r, body); err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
	}
	return &Frame{Command: cmd, RequestID: reqID, Body: body}, nil
}

// Unmarshal decodes the body of a frame into v.
func Unmarshal(f *Frame, v interface{}) error {
	if len(f.Body) == 0 {
		return nil
	}
	return json.Unmarshal(f.Body, v)
}
