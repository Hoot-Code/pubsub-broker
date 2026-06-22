package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/wsutil"
)

// explorerMaxConnDefault is the default maximum concurrent Explorer WebSocket
// connections when ExplorerMaxConnections is not set in config.
const explorerMaxConnDefault = 50

// explorerStatusInterval is how often the server checks whether to send a
// status frame with drop-count information.
const explorerStatusInterval = 5 * time.Second

// explorerMsgFrame is the JSON frame pushed to an Explorer client for each
// matching published message.
type explorerMsgFrame struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	Payload     string `json:"payload"`
	TimestampNs int64  `json:"timestamp_ns"`
	Producer    string `json:"producer"`
}

// explorerStatusFrame is the periodic status frame when there's nothing new
// and the drop counter has changed since the last status.
type explorerStatusFrame struct {
	Status struct {
		DroppedSinceLast uint64 `json:"dropped_since_last"`
	} `json:"status"`
}

// explorerControlFrame is a JSON control frame from the client.
type explorerControlFrame struct {
	Action string          `json:"action"`
	Filter *ExplorerFilter `json:"filter,omitempty"`
}

// httpExplorerStream implements GET /explorer/stream — a WebSocket endpoint
// that streams newly published messages matching server-side filter criteria.
func (b *Broker) httpExplorerStream(w http.ResponseWriter, r *http.Request) {
	cfg := b.cfg.Get()

	if !cfg.Network.ExplorerEnabled {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "explorer is disabled"})
		return
	}

	maxConns := cfg.Network.ExplorerMaxConnections
	if maxConns <= 0 {
		maxConns = explorerMaxConnDefault
	}
	if b.explorerActiveConns.Load() >= int64(maxConns) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "too many explorer connections"})
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "topic parameter is required"})
		return
	}

	// RBAC: require PermSubscribe or PermAdmin when auth is enabled.
	// Checked before topic existence so that unauthorised callers cannot
	// probe for topic existence.
	identity := b.resolveIdentity(r)
	if identity == nil {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "authentication required"})
		return
	}
	if !identity.Can(string(auth.PermSubscribe), topic) && !identity.Can(string(auth.PermAdmin), topic) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "insufficient permissions to explore this topic"})
		return
	}

	if _, err := b.topics.Get(topic); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("topic %q not found", topic)})
		return
	}

	filter := ExplorerFilter{
		Topic:     topic,
		Partition: -1,
		Key:       r.URL.Query().Get("key"),
		Producer:  r.URL.Query().Get("producer"),
		Search:    r.URL.Query().Get("search"),
	}
	if p := r.URL.Query().Get("partition"); p != "" {
		var partition int
		if _, err := fmt.Sscanf(p, "%d", &partition); err == nil {
			filter.Partition = int32(partition)
		}
	}

	ws, err := wsutil.UpgradeWebSocket(w, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	b.explorerActiveConns.Add(1)

	// The sink writes JSON frames to the WebSocket. The session's drain
	// goroutine calls this, so the WS write is isolated from the publish path.
	var sinkMu sync.Mutex
	sess := b.explorerHub.NewSession(filter, func(ev ExplorerEvent) error {
		frame := explorerMsgFrame{
			Topic:       ev.Message.Topic,
			Partition:   ev.Message.Partition,
			Offset:      ev.Message.Offset,
			Key:         ev.Message.Key,
			Payload:     base64.StdEncoding.EncodeToString(ev.Message.Payload),
			TimestampNs: ev.Message.Timestamp,
			Producer:    ev.ClientID,
		}
		b, err := json.Marshal(frame)
		if err != nil {
			return err
		}
		sinkMu.Lock()
		defer sinkMu.Unlock()
		return ws.WriteTextFrame(string(b))
	})

	// Cleanup on return: session close, hub removal, connection count, WS close.
	defer func() {
		sess.Close()
		b.explorerHub.removeSession(sess)
		b.explorerActiveConns.Add(-1)
		ws.Close()
	}()

	// Start a goroutine that periodically sends status frames with drop count.
	var lastDropped uint64
	statusDone := make(chan struct{})
	var statusCloseOnce sync.Once
	statusClose := func() { statusCloseOnce.Do(func() { close(statusDone) }) }
	go func() {
		defer statusClose()
		ticker := time.NewTicker(explorerStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-statusDone:
				return
			case <-ticker.C:
				dropped := sess.DroppedCount()
				if dropped != lastDropped {
					delta := dropped - lastDropped
					lastDropped = dropped
					sf := explorerStatusFrame{}
					sf.Status.DroppedSinceLast = delta
					sb, err := json.Marshal(sf)
					if err != nil {
						continue
					}
					sinkMu.Lock()
					_ = ws.WriteTextFrame(string(sb))
					sinkMu.Unlock()
				}
			}
		}
	}()

	// Read loop: accept control frames from the client until disconnect.
	for {
		text, err := ws.ReadTextFrame()
		if err != nil {
			statusClose()
			return
		}
		var frame explorerControlFrame
		if err := json.Unmarshal([]byte(text), &frame); err != nil {
			b.log.Warn("explorer: malformed control frame", "err", err)
			continue
		}
		switch frame.Action {
		case "pause":
			sess.Pause()
		case "resume":
			sess.Resume()
		case "update_filter":
			if frame.Filter != nil {
				sess.UpdateFilter(*frame.Filter)
			}
		default:
			b.log.Warn("explorer: unknown control action", "action", frame.Action)
		}
	}
}
