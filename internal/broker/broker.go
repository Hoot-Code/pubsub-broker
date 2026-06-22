// Package broker is the central orchestrator of the pub/sub broker.
// It wires all subsystems together and implements the networking.Handler
// interface, processing every protocol frame from connected clients.
package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/cluster"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/consumer"
	"github.com/Hoot-Code/pubsub-broker/internal/gateway"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/producer"
	"github.com/Hoot-Code/pubsub-broker/internal/replication"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/internal/topic"
	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
	"github.com/Hoot-Code/pubsub-broker/internal/wal"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Broker is the top-level orchestrator of the pub/sub broker.
// It wires storage, replication, auth, and cluster subsystems together and
// implements networking.Handler to process every incoming client frame.
type Broker struct {
	cfg         *config.Loader
	log         *logging.Logger
	topics      *topic.Manager
	partitioner *partition.HashPartitioner
	offsets     *partition.OffsetStore
	consumers   *consumer.Manager
	producers   *producer.Producer
	authMu      sync.RWMutex // protects auth and rateLimiter during hot-reload
	auth        *auth.Authenticator
	rateLimiter *auth.RateLimiter
	replication *replication.Manager
	metricsReg  *metrics.Registry
	metrics     *metrics.Broker
	server      *networking.Server
	httpServer  *http.Server
	status      atomic.Value // types.NodeStatus
	// ready is set to true at the end of Start() once the broker is fully
	// initialised and accepting connections.
	ready atomic.Bool
	// inFlightRequests counts the number of protocol requests currently being
	// handled. Incremented at the start of Handle(), decremented at the end.
	inFlightRequests atomic.Int64
	// stopCtx/stopCancel is the single shutdown signal for all background goroutines.
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// WAL for message durability.
	msgWAL     *wal.WAL
	walEntries []wal.Entry // recovered entries, replayed in Start()

	// WAL for consumer offset persistence.
	offsetWAL   *wal.OffsetWAL
	commitCount atomic.Int64 // total commits since last checkpoint

	// WAL for topic metadata persistence.
	topicWAL *wal.TopicWAL

	// httpAddr is the actual bound HTTP admin address, set after net.Listen.
	// Stored as atomic.Value (string) for safe concurrent reads.
	httpAddr atomic.Value

	// TLS cert/key paths for conditional ListenAndServeTLS.
	tlsCertFile string
	tlsKeyFile  string

	// onCheckpoint is called (if non-nil) after each successful offset checkpoint.
	onCheckpoint func()

	// Tracing.
	tracer    *tracing.Tracer
	spanStore *tracing.SpanStore

	// Cluster node. nil when cluster is disabled.
	clusterNode *cluster.Node

	// partTrackers holds ISR trackers keyed by topic and partition.
	partTrackersMu sync.RWMutex
	partTrackers   map[string]map[int32]*cluster.ISRTracker

	// prevOwned tracks the partition ownership snapshot before the last
	// MsgMetaSync-driven partition map update.
	prevOwnedMu sync.Mutex
	prevOwned   map[string]map[int32]bool

	// pendingWALBytes tracks the total byte size of messages currently queued
	// for WAL writes (producer-side backpressure).
	pendingWALBytes atomic.Int64

	// drainTimeoutMs and flowControlPauseMs are atomic copies of config
	// values so that Stop()'s drain loop and the consumer flow-control
	// pause logic read the CURRENT value on each use rather than a value
	// captured once at startup.
	drainTimeoutMs     atomic.Int64
	flowControlPauseMs atomic.Int64

	// audit is the structured audit event logger. nil when audit is disabled.
	audit *audit.Logger

	// wg tracks background goroutines that touch the WALs so that Stop() can
	// wait for them to exit before closing the WAL files.
	wg sync.WaitGroup

	// partLocks is a sync.Map of *sync.Mutex keyed by "topic/partition". It
	// serialises the TargetOffset-capture → pl.Append critical section in
	// handlePublish/handleBatchPublish so that concurrent publishes to the same
	// partition cannot capture a stale TargetOffset.
	partLocks sync.Map

	// explorerHub fans out newly published messages to all active Explorer
	// sessions (live-tap WebSocket clients). Nil-safe: Publish() checks
	// len(sessions) and returns immediately when empty.
	explorerHub *ExplorerHub

	// explorerActiveConns tracks the number of active Explorer WebSocket
	// connections for the connection cap enforcement.
	explorerActiveConns atomic.Int64

	// explorerPrevSent/PrevDropped track the last-seen cumulative counts
	// from the ExplorerHub so that updateDynamicMetrics can compute deltas.
	explorerPrevSent    uint64
	explorerPrevDropped uint64

	// compactor runs key-based log compaction for topics with
	// CompactionMode == "compact". Always created; CompactablePartitions
	// returns an empty slice when no topic uses compaction, so the sweep
	// loop is a cheap no-op in that case.
	compactor *storage.KeyCompactor

	// sessionStore holds authenticated dashboard sessions.
	sessionStore *SessionStore

	// historyStore is the in-memory time-series ring buffer for /metrics/history.
	historyStore *metrics.HistoryStore

	// gatewayMu guards gw, set asynchronously once the
	// embedded HTTP/WebSocket gateway (if enabled) has connected to this
	// broker's own TCP listener.
	gatewayMu sync.Mutex
	gw        *gateway.Gateway

	// configPatchLimiter rate-limits PATCH /config requests per identity.
	configPatchLimiter *configPatchRateLimiter
}

// New creates a fully wired Broker from a config loader.
func New(loader *config.Loader) (*Broker, error) {
	cfg := loader.Get()
	log := logging.New(nil, cfg.Logging.Level)

	// The HTTP admin port is tcpPort+1. A TCP port of 65535 would
	// overflow to 65536 (invalid). Config validation rejects this, but defend
	// here too so a misconfigured loader cannot produce an unbindable address.
	if cfg.Network.Port >= 65535 {
		return nil, fmt.Errorf("cannot allocate HTTP admin port: TCP port %d leaves no room", cfg.Network.Port)
	}

	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)

	partitioner := partition.NewHashPartitioner()
	storageCfg := &cfg.Storage

	topicMgr := topic.NewManager(storageCfg, partitioner)
	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(10_000)
	consumerMgr := consumer.NewManager(offsets, dlq, 3, 500*time.Millisecond, 512)

	prod := producer.NewProducer(
		topicMgr, partitioner, log, bm, 3, 200*time.Millisecond,
	)

	authenticator := auth.NewAuthenticator(cfg.Auth)
	rateLimiter := auth.NewRateLimiter(&cfg.RateLimit)

	replMgr := replication.NewManager(
		cfg.Broker.NodeID,
		cfg.Replication.Factor,
		cfg.Replication.SyncInterval,
		cfg.Replication.AckTimeout,
		log,
		bm,
	)
	replMgr.SetLeader(true)

	stopCtx, stopCancel := context.WithCancel(context.Background())

	msgWAL, walEntries, err := wal.Open(storageCfg.WALPath)
	if err != nil {
		stopCancel()
		return nil, fmt.Errorf("broker: open message wal: %w", err)
	}

	offsetWAL, err := wal.OpenOffsetWAL(storageCfg.WALPath + ".offsets")
	if err != nil {
		_ = msgWAL.Close()
		stopCancel()
		return nil, fmt.Errorf("broker: open offset wal: %w", err)
	}

	topicWAL, err := wal.OpenTopicWAL(storageCfg.WALPath + ".topics")
	if err != nil {
		_ = offsetWAL.Close()
		_ = msgWAL.Close()
		stopCancel()
		return nil, fmt.Errorf("broker: open topic wal: %w", err)
	}

	b := &Broker{
		cfg:         loader,
		log:         log,
		topics:      topicMgr,
		partitioner: partitioner,
		offsets:     offsets,
		consumers:   consumerMgr,
		producers:   prod,
		auth:        authenticator,
		rateLimiter: rateLimiter,
		replication: replMgr,
		metricsReg:  reg,
		metrics:     bm,
		stopCtx:     stopCtx,
		stopCancel:  stopCancel,
		msgWAL:      msgWAL,
		walEntries:  walEntries,
		offsetWAL:   offsetWAL,
		topicWAL:    topicWAL,
		tlsCertFile: cfg.Network.TLSCertFile,
		tlsKeyFile:  cfg.Network.TLSKeyFile,
	}
	b.status.Store(types.NodeActive)

	// Initialise atomic config values.
	b.drainTimeoutMs.Store(int64(cfg.DrainTimeoutMs))
	b.flowControlPauseMs.Store(int64(cfg.FlowControlPauseMs))

	// Explorer hub for live message tail (Phase 17).
	b.explorerHub = NewExplorerHub()

	// Session store for dashboard authentication.
	sessionTTL := cfg.Network.DashboardSessionTTL
	if sessionTTL <= 0 {
		sessionTTL = 12 * time.Hour
	}
	b.sessionStore = NewSessionStore(sessionTTL)

	// Audit logger — open file if configured.
	if cfg.AuditLogFile != "" {
		af, aerr := os.OpenFile(cfg.AuditLogFile,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if aerr != nil {
			log.Warn("audit: failed to open log file", "path", cfg.AuditLogFile, "err", aerr)
		} else {
			b.audit = audit.NewLogger(af)
		}
	}

	// Cluster node.
	if cfg.Cluster.Enabled {
		clusterCfg := cfg.Cluster
		if clusterCfg.NodeID == "" {
			clusterCfg.NodeID = cfg.Broker.NodeID
		}
		cn, cerr := cluster.NewNode(clusterCfg, bm, log)
		if cerr != nil {
			_ = topicWAL.Close()
			_ = offsetWAL.Close()
			_ = msgWAL.Close()
			stopCancel()
			return nil, fmt.Errorf("broker: new cluster node: %w", cerr)
		}
		b.clusterNode = cn

		cn.SetTopicProvider(func() []cluster.TopicInfo {
			var out []cluster.TopicInfo
			for _, t := range b.topics.List() {
				out = append(out, cluster.TopicInfo{
					Name:       t.Config.Name,
					Partitions: t.Config.Partitions,
				})
			}
			return out
		})

		cn.SetPartitionChangeCallback(func(newPM *cluster.PartitionMap) {
			b.onClusterMetaSync(newPM)
		})
	}

	// Hot-reload: update auth and rate-limiter on config change.
	loader.OnChange(func(newCfg *config.Config) {
		log.Info("config reloaded")
		b.authMu.Lock()
		b.auth = auth.NewAuthenticator(newCfg.Auth)
		b.rateLimiter = auth.NewRateLimiter(&newCfg.RateLimit)
		b.authMu.Unlock()
	})

	netCfg := &cfg.Network
	b.server = networking.NewServer(netCfg, b, log)
	b.httpServer = b.buildHTTPServer(cfg)

	b.spanStore = tracing.NewSpanStore(1000)
	b.tracer = tracing.NewTracer()
	b.tracer.SetExporter(func(sp *tracing.Span) {
		b.spanStore.Add(sp)
	})
	prod.SetTracer(b.tracer)

	topicMgr.StartRetentionLoop(stopCtx, &cfg.Retention, log)
	b.rateLimiter.StartCleanup(stopCtx, 10*time.Minute)
	b.sessionStore.StartCleanup(stopCtx, time.Minute)

	// Key-based log compaction. Always constructed; the sweep
	// loop is a cheap no-op when no topic uses CompactionMode == "compact".
	compactionInterval := time.Duration(cfg.Compaction.IntervalMs) * time.Millisecond
	b.compactor = storage.NewKeyCompactor(compactionInterval)
	b.compactor.SetTombstoneGrace(time.Duration(cfg.Compaction.TombstoneGraceMs) * time.Millisecond)
	b.compactor.Start(stopCtx, topicMgr)

	// History store for /metrics/history — collects the same values used for
	// /metrics every 10 seconds, retaining 24 hours of data.
	// ~15 metrics * 8640 samples * 16 bytes ≈ 2 MB max memory.
	b.historyStore = metrics.NewHistoryStore(24*time.Hour, 10*time.Second)
	b.historyStore.Start(stopCtx, b.collectMetricsSnapshot)

	// Config patch rate limiter for PATCH /config.
	b.configPatchLimiter = newConfigPatchRateLimiter()

	// Track the offset-checkpoint goroutine in the broker's WaitGroup so
	// that Stop() can wg.Wait() before closing the offset WAL, preventing a
	// write-after-close race on the WAL file.
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.offsetCheckpointLoop(stopCtx)
	}()

	return b, nil
}

// Start runs the broker: replays WALs, restores offsets, then begins accepting
// connections. Blocks until Stop is called.
func (b *Broker) Start() error {
	b.log.Info("broker starting",
		"node_id", b.cfg.Get().Broker.NodeID,
		"port", b.cfg.Get().Network.Port,
	)

	cfg := b.cfg.Get()
	if cfg.Network.DashboardEnabled && !b.dashboardAuthEnabled() {
		if !cfg.Auth.Enabled {
			b.log.Info("dashboard auth disabled: broker auth.enabled is false")
		} else {
			b.log.Info("dashboard auth disabled: overridden via config (network.dashboard_auth_enabled=false)")
		}
	}

	if err := b.replayOffsetWAL(); err != nil {
		return err
	}
	if err := b.replayTopicWAL(); err != nil {
		return err
	}

	b.wirePartitions()
	b.replayMessageWAL()

	if err := b.msgWAL.Truncate(nil); err != nil {
		b.log.Warn("wal: post-replay truncate failed", "err", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.replication.Start(ctx)

	if b.clusterNode != nil {
		if err := b.clusterNode.Start(b.stopCtx); err != nil {
			b.log.Warn("cluster: node start error", "err", err)
		}
	}

	httpLn, err := net.Listen("tcp", b.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("broker: http listen %s: %w", b.httpServer.Addr, err)
	}
	b.httpAddr.Store(httpLn.Addr().String())
	httpAddrStr := b.httpAddr.Load().(string)
	go func() {
		b.log.Info("http server starting", "addr", httpAddrStr)
		var serveErr error
		if b.tlsCertFile != "" {
			b.log.Info("http tls enabled", "cert", b.tlsCertFile)
			b.httpServer.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
			serveErr = b.httpServer.ServeTLS(httpLn, b.tlsCertFile, b.tlsKeyFile)
		} else {
			serveErr = b.httpServer.Serve(httpLn)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			b.log.Error("http server error", "err", serveErr)
		}
	}()

	if b.cfg.Get().Gateway.Enabled {
		go b.runEmbeddedGateway(b.cfg.Get().Gateway)
	}

	tcpAddr := fmt.Sprintf("%s:%d", b.cfg.Get().Network.Host, b.cfg.Get().Network.Port)
	httpAddrLogged := b.httpAddr.Load().(string)
	b.log.Info("broker ports bound",
		"tcp_addr", tcpAddr,
		"http_admin_addr", httpAddrLogged,
	)

	b.ready.Store(true)
	return b.server.Start()
}

// Stop shuts down the broker gracefully, draining in-flight requests before
// closing the TCP server.
func (b *Broker) Stop(ctx context.Context) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	b.log.Info("broker stopping")
	b.status.Store(types.NodeUnhealthy)

	b.server.SetDraining(true)
	drainMs := int(b.drainTimeoutMs.Load())
	if drainMs <= 0 {
		drainMs = 5000
	}
	drainDeadline := time.Now().Add(time.Duration(drainMs) * time.Millisecond)
	for time.Now().Before(drainDeadline) {
		if b.inFlightRequests.Load() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.stopCancel()
	b.replication.Stop()
	if b.compactor != nil {
		b.compactor.Stop()
	}
	b.gatewayMu.Lock()
	gw := b.gw
	b.gatewayMu.Unlock()
	if gw != nil {
		if err := gw.Stop(); err != nil {
			b.log.Warn("gateway: stop error", "err", err)
		}
	}

	b.topics.CloseAll()
	b.cfg.Close()

	if b.clusterNode != nil {
		if err := b.clusterNode.Stop(); err != nil {
			b.log.Warn("cluster: node stop error", "err", err)
		}
	}

	if err := b.httpServer.Shutdown(ctx); err != nil {
		b.log.Warn("http shutdown error", "err", err)
	}
	if err := b.server.Close(); err != nil {
		return err
	}
	// Wait for the offset-checkpoint goroutine to finish any in-flight
	// WAL write before closing the offset WAL file. Without this, the checkpoint
	// loop could call offsetWAL.Checkpoint (→ writeEntry → file.Write) after
	// offsetWAL.Close has run, producing a write-after-close error / data race.
	b.wg.Wait()
	if err := b.offsetWAL.Close(); err != nil {
		b.log.Warn("offset wal close error", "err", err)
	}
	if err := b.msgWAL.Close(); err != nil {
		b.log.Warn("message wal close error", "err", err)
	}
	if err := b.topicWAL.Close(); err != nil {
		b.log.Warn("topic wal close error", "err", err)
	}
	return nil
}
