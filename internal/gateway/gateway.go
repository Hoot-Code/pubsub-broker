// Package gateway implements the optional HTTP/WebSocket gateway that lets
// browsers and languages without a native SDK publish/subscribe over plain
// HTTP and WebSocket instead of the binary wire protocol.
package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/pkg/client"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Logger is the minimal logging surface the gateway needs. *logging.Logger
// (internal/logging) satisfies this interface structurally.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// noopLogger discards all log output. Used when NewGateway is called with a
// nil Logger.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// poolIdleTimeout is the maximum time a pooled connection may sit unused
// before the background cleanup goroutine evicts it.
const poolIdleTimeout = 10 * time.Minute

// cleanupInterval is how often the background goroutine sweeps the pool.
const cleanupInterval = 2 * time.Minute

// pooledConn is an already-authenticated broker client connection keyed
// by the API key that was used during its creation.
type pooledConn struct {
	c        *client.Client
	lastUsed time.Time
}

// Gateway is a thin HTTP+WebSocket front-end that lets browsers and
// languages without a native SDK publish/subscribe over plain HTTP and
// WebSocket instead of the binary wire protocol. It is itself a client of
// the broker (via pkg/client over loopback TCP) and does not bypass the
// binary protocol or duplicate broker internals.
//
// Each distinct API key gets its own dedicated, already-authenticated
// connection. No two requests with different keys ever share a
// connection's identity, preventing the RBAC bypass that a single shared
// connection would allow under concurrent requests.
type Gateway struct {
	cfg        config.GatewayConfig
	brokerAddr string
	dialOpts   []client.Option
	pool       map[string]*pooledConn
	poolMu     sync.Mutex
	log        Logger

	srv *http.Server

	wsMu sync.Mutex
	wsWG sync.WaitGroup

	stopCleanCh chan struct{}
	cleanWG     sync.WaitGroup
}

// NewGateway constructs a Gateway that serves cfg.Addr and forwards every
// request to a connection obtained via connFor. brokerAddr is the TCP
// address of the broker node to dial. dialOpts are passed to client.Dial
// for each new connection. log may be nil, in which case log output is
// discarded.
func NewGateway(cfg config.GatewayConfig, brokerAddr string, dialOpts []client.Option, log Logger) *Gateway {
	if log == nil {
		log = noopLogger{}
	}
	g := &Gateway{
		cfg:        cfg,
		brokerAddr: brokerAddr,
		dialOpts:   dialOpts,
		pool:       make(map[string]*pooledConn),
		log:        log,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics", g.handleCreateTopic)
	mux.HandleFunc("GET /v1/topics", g.handleListTopics)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", g.handlePublish)
	mux.HandleFunc("POST /v1/topics/{topic}/messages/batch", g.handlePublishBatch)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{partition}/messages", g.handleFetch)
	mux.HandleFunc("GET /v1/topics/{topic}/stream", g.handleStream)
	g.srv = &http.Server{Addr: cfg.Addr, Handler: mux}
	return g
}

// connFor returns a dedicated, already-authenticated broker client for the
// given API key. Each distinct key gets its own connection, so concurrent
// requests with different keys cannot race on the connection's identity.
//
// When apiKey is "" (no Authorization header or auth disabled), a single
// shared anonymous connection is used — this is safe because there is no
// identity to race on.
func (g *Gateway) connFor(apiKey string) (*client.Client, error) {
	g.poolMu.Lock()
	pc, ok := g.pool[apiKey]
	if ok {
		pc.lastUsed = time.Now()
		g.poolMu.Unlock()
		return pc.c, nil
	}
	g.poolMu.Unlock()

	c, err := client.Dial(g.brokerAddr, g.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("gateway: dial broker: %w", err)
	}

	// Authenticate once at creation time. Each key gets its own connection,
	// so there is no identity race between concurrent requests.
	if apiKey != "" {
		if err := c.Authenticate(apiKey); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("gateway: authenticate: %w", err)
		}
	}

	g.poolMu.Lock()
	// Double-check: another goroutine may have created the same entry
	// while we were dialing.
	if existing, ok := g.pool[apiKey]; ok {
		g.poolMu.Unlock()
		_ = c.Close()
		existing.lastUsed = time.Now()
		return existing.c, nil
	}
	pc = &pooledConn{c: c, lastUsed: time.Now()}
	g.pool[apiKey] = pc
	g.poolMu.Unlock()
	return pc.c, nil
}

// Start begins serving HTTP on cfg.Addr and launches the background pool
// cleanup goroutine. It returns once the listener is closed (by Stop) or
// fails to bind. Run it in its own goroutine for a non-blocking start.
func (g *Gateway) Start(ctx context.Context) error {
	g.stopCleanCh = make(chan struct{})
	g.cleanWG.Add(1)
	go g.poolCleanupLoop(ctx)

	errCh := make(chan error, 1)
	go func() { errCh <- g.srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		_ = g.Stop()
		return ctx.Err()
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("gateway: listen %s: %w", g.cfg.Addr, err)
		}
		return nil
	}
}

// Stop gracefully shuts down the HTTP server, stops the pool cleanup
// goroutine, closes all pooled connections, and waits for any active
// WebSocket stream handlers to finish.
func (g *Gateway) Stop() error {
	if g.stopCleanCh != nil {
		select {
		case <-g.stopCleanCh:
		default:
			close(g.stopCleanCh)
		}
	}
	g.cleanWG.Wait()

	// Close all pooled connections.
	g.poolMu.Lock()
	for key, pc := range g.pool {
		_ = pc.c.Close()
		delete(g.pool, key)
	}
	g.poolMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := g.srv.Shutdown(ctx)
	g.wsWG.Wait()
	if err != nil {
		return fmt.Errorf("gateway: shutdown: %w", err)
	}
	return nil
}

// poolCleanupLoop evicts pool entries idle longer than poolIdleTimeout.
// It runs until stopCleanCh is closed.
func (g *Gateway) poolCleanupLoop(ctx context.Context) {
	defer g.cleanWG.Done()
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopCleanCh:
			return
		case <-ticker.C:
			g.evictIdle()
		}
	}
}

// evictIdle removes and closes connections that have not been used for
// longer than poolIdleTimeout. When auth is disabled, the anonymous ""
// key entry is also subject to eviction (a single shared connection is
// safe only when there is no identity to race on, but an idle anonymous
// connection is still eligible for cleanup).
func (g *Gateway) evictIdle() {
	now := time.Now()
	g.poolMu.Lock()
	for key, pc := range g.pool {
		if now.Sub(pc.lastUsed) > poolIdleTimeout {
			_ = pc.c.Close()
			delete(g.pool, key)
		}
	}
	g.poolMu.Unlock()
}

// parseAPIKey extracts the API key from the Authorization header, returning
// "" if no header is present or auth is not in Bearer form.
func parseAPIKey(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return ""
	}
	return strings.TrimPrefix(authz, prefix)
}

// ─── REST: topics ───────────────────────────────────────────────────────────

type createTopicReq struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

type createTopicResp struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (g *Gateway) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	var req createTopicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := c.CreateTopic(ctx, types.TopicConfig{
		Name:              req.Name,
		Partitions:        req.Partitions,
		ReplicationFactor: 1,
	}); err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, createTopicResp{Name: req.Name, Partitions: req.Partitions})
}

type listTopicsResp struct {
	Topics []topicInfo `json:"topics"`
}

type topicInfo struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (g *Gateway) handleListTopics(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	topics, err := c.ListTopics(ctx)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	out := listTopicsResp{Topics: make([]topicInfo, len(topics))}
	for i, t := range topics {
		out.Topics[i] = topicInfo{Name: t.Name, Partitions: t.Partitions}
	}
	writeJSON(w, http.StatusOK, out)
}

// ─── REST: publish ──────────────────────────────────────────────────────────

type publishReq struct {
	Key     string            `json:"key"`
	Payload string            `json:"payload"`
	Headers map[string]string `json:"headers"`
}

type publishResp struct {
	Offset    int64 `json:"offset"`
	Partition int32 `json:"partition"`
}

// decodePayload accepts either base64-encoded or plain-text payload bodies,
// per C3: "payload":"<base64 or plain text>".
func decodePayload(s string) []byte {
	if s == "" {
		return nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b
	}
	return []byte(s)
}

func (g *Gateway) handlePublish(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	topicName := r.PathValue("topic")
	var req publishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	prod := c.NewProducer(topicName)
	partition, offset, err := prod.PublishWithMeta(ctx, req.Key, decodePayload(req.Payload), req.Headers)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, publishResp{Offset: offset, Partition: partition})
}

type publishBatchReq struct {
	Messages []publishReq `json:"messages"`
}

type publishBatchResp struct {
	Offsets []int64 `json:"offsets"`
}

func (g *Gateway) handlePublishBatch(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	topicName := r.PathValue("topic")
	var req publishBatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	prod := c.NewProducer(topicName)
	offsets := make([]int64, 0, len(req.Messages))
	for _, m := range req.Messages {
		_, offset, err := prod.PublishWithMeta(ctx, m.Key, decodePayload(m.Payload), m.Headers)
		if err != nil {
			writeBrokerError(w, err)
			return
		}
		offsets = append(offsets, offset)
	}
	writeJSON(w, http.StatusCreated, publishBatchResp{Offsets: offsets})
}

// ─── REST: fetch ────────────────────────────────────────────────────────────

type fetchResp struct {
	Messages []fetchMessage `json:"messages"`
}

type fetchMessage struct {
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	Payload     string `json:"payload"`
	TimestampNs int64  `json:"timestamp_ns"`
}

func (g *Gateway) handleFetch(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	topicName := r.PathValue("topic")
	partition, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid partition: %w", err))
		return
	}
	offset := parseQueryInt64(r, "offset", 0)
	limit := int(parseQueryInt64(r, "limit", 100))

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	msgs, err := c.Fetch(ctx, topicName, int32(partition), offset, limit)
	if err != nil {
		writeBrokerError(w, err)
		return
	}
	out := fetchResp{Messages: make([]fetchMessage, len(msgs))}
	for i, m := range msgs {
		out.Messages[i] = fetchMessage{
			Offset:      m.Offset,
			Key:         m.Key,
			Payload:     base64.StdEncoding.EncodeToString(m.Payload),
			TimestampNs: m.Timestamp,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func parseQueryInt64(r *http.Request, name string, def int64) int64 {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// ─── WebSocket: stream ──────────────────────────────────────────────────────

// wsPushMessage is the JSON frame format for messages pushed to a streaming
// client, per C4.
type wsPushMessage struct {
	Offset      int64  `json:"offset"`
	Partition   int32  `json:"partition"`
	Key         string `json:"key"`
	Payload     string `json:"payload"`
	TimestampNs int64  `json:"timestamp_ns"`
}

// wsCommitFrame is the JSON frame a streaming client may send to commit an
// offset, per C4: {"commit":{"partition":<p>,"offset":<n>}}.
type wsCommitFrame struct {
	Commit *struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	} `json:"commit"`
}

func (g *Gateway) handleStream(w http.ResponseWriter, r *http.Request) {
	apiKey := parseAPIKey(r)
	c, err := g.connFor(apiKey)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	topicName := r.PathValue("topic")
	group := r.URL.Query().Get("group")
	if group == "" {
		group = "gateway-" + topicName
	}
	consumerID := r.URL.Query().Get("consumer")

	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	g.wsWG.Add(1)
	go g.runStream(c, ws, topicName, group, consumerID)
}

// runStream owns one WebSocket connection's lifetime: it subscribes to the
// topic via the broker client, forwards pushed messages as WS text frames,
// and applies any commit frames the client sends. It returns (and closes the
// connection) when the client disconnects or sends a close frame.
func (g *Gateway) runStream(c *client.Client, ws *wsConn, topicName, group, consumerID string) {
	defer g.wsWG.Done()
	defer ws.Close()

	// consumerID is accepted for protocol compatibility (clients can pass a
	// stable ?consumer=<id> to identify themselves in logs), but pkg/client's
	// Consumer always generates its own internal ID; there is no public hook
	// to override it.
	consumer := c.NewConsumer(group, topicName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := consumer.Subscribe(ctx); err != nil {
		g.log.Warn("gateway: stream subscribe failed", "topic", topicName, "group", group, "consumer", consumerID, "err", err)
		return
	}
	defer consumer.Close()

	// Reader goroutine: client → broker (commit frames).
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			text, err := ws.ReadTextFrame()
			if err != nil {
				cancel()
				return
			}
			var frame wsCommitFrame
			if err := json.Unmarshal([]byte(text), &frame); err != nil {
				continue
			}
			if frame.Commit != nil {
				_ = consumer.Commit(ctx, frame.Commit.Partition, frame.Commit.Offset)
			}
		}
	}()

	// Writer loop: broker → client (pushed messages).
	for {
		select {
		case <-readDone:
			return
		case <-ctx.Done():
			return
		case msg, ok := <-consumer.Messages():
			if !ok {
				return
			}
			out := wsPushMessage{
				Offset:      msg.Offset,
				Partition:   msg.Partition,
				Key:         msg.Key,
				Payload:     base64.StdEncoding.EncodeToString(msg.Payload),
				TimestampNs: msg.Timestamp,
			}
			b, err := json.Marshal(out)
			if err != nil {
				continue
			}
			if err := ws.WriteTextFrame(string(b)); err != nil {
				return
			}
		}
	}
}

// ─── response helpers ───────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorBody{Error: err.Error()})
}

// writeBrokerError maps a pkg/client error to an appropriate HTTP status.
func writeBrokerError(w http.ResponseWriter, err error) {
	var notFound *client.ErrTopicNotFound
	if errors.As(err, &notFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	var brokerErr *client.ErrBroker
	if errors.As(err, &brokerErr) && strings.Contains(strings.ToUpper(brokerErr.Code), "EXISTS") {
		writeError(w, http.StatusConflict, err)
		return
	}
	var notAuthed *client.ErrNotAuthenticated
	if errors.As(err, &notAuthed) {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}
