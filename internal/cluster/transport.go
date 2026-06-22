package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Wire protocol constants ──────────────────────────────────────────────────

const (
	wireMagic0 = byte(0xCB)
	wireMagic1 = byte(0x01)
	maxMsgBody = 4 * 1024 * 1024 // 4 MiB sanity cap
)

// ─── Message types ────────────────────────────────────────────────────────────

// MsgType identifies the kind of inter-node cluster message.
type MsgType uint8

const (
	// MsgHeartbeat is sent periodically by the leader to assert leadership.
	MsgHeartbeat MsgType = 1
	// MsgVoteRequest is broadcast by a candidate to solicit votes.
	MsgVoteRequest MsgType = 2
	// MsgVoteGrant is sent to a candidate to grant its vote request.
	MsgVoteGrant MsgType = 3
	// MsgVoteDeny is sent to a candidate to deny its vote request.
	MsgVoteDeny MsgType = 4
	// MsgJoin is sent by a new node to an existing seed node.
	MsgJoin MsgType = 5
	// MsgJoinAck is sent by the seed node back to the joiner.
	MsgJoinAck MsgType = 6
	// MsgLeave is broadcast by a node that is shutting down.
	MsgLeave MsgType = 7
	// MsgMetaSync broadcasts membership or partition-map changes.
	MsgMetaSync MsgType = 8
	// MsgReplicaFetch is sent by a follower to request log entries since a given offset.
	MsgReplicaFetch MsgType = 9
	// MsgReplicaData is sent by the leader to deliver log entries to a follower.
	MsgReplicaData MsgType = 10
	// MsgReplicaAck is sent by a follower to acknowledge the highest offset written.
	MsgReplicaAck MsgType = 11
	// MsgRaftRequestVote is sent by a Raft candidate to solicit votes.
	MsgRaftRequestVote MsgType = 20
	// MsgRaftVoteResponse is sent to a Raft candidate to grant or deny a vote.
	MsgRaftVoteResponse MsgType = 21
	// MsgRaftAppendEntries is sent by a Raft leader to replicate log entries
	// and as heartbeats.
	MsgRaftAppendEntries MsgType = 22
	// MsgRaftAppendResponse is sent by a Raft follower to acknowledge
	// AppendEntries.
	MsgRaftAppendResponse MsgType = 23
)

// ─── Replication message bodies ───────────────────────────────────────────────

// ReplicaFetchBody is the JSON payload of a MsgReplicaFetch message.
type ReplicaFetchBody struct {
	// Topic identifies the replicated topic.
	Topic string `json:"topic"`
	// Partition identifies the replicated partition.
	Partition int32 `json:"partition"`
	// FromOffset is the first log offset the follower needs.
	FromOffset int64 `json:"from_offset"`
}

// ReplicaDataBody is the JSON payload of a MsgReplicaData message.
type ReplicaDataBody struct {
	// Topic identifies the replicated topic.
	Topic string `json:"topic"`
	// Partition identifies the replicated partition.
	Partition int32 `json:"partition"`
	// Messages is the ordered batch of entries to replicate.
	Messages []*types.Message `json:"messages"`
}

// ReplicaAckBody is the JSON payload of a MsgReplicaAck message.
type ReplicaAckBody struct {
	// Topic identifies the replicated topic.
	Topic string `json:"topic"`
	// Partition identifies the replicated partition.
	Partition int32 `json:"partition"`
	// Offset is the highest log offset the follower has successfully written.
	Offset int64 `json:"offset"`
}

// ClusterMsg is a single inter-node message. Body carries type-specific JSON.
type ClusterMsg struct {
	// Type identifies how Body should be interpreted.
	Type MsgType `json:"type"`
	// From is the NodeID of the sender.
	From string `json:"from"`
	// Term is the election term at the time of sending.
	Term uint64 `json:"term"`
	// Body is an opaque JSON payload whose schema depends on Type.
	Body []byte `json:"body,omitempty"`
}

// ─── Transporter interface ────────────────────────────────────────────────────

// Transporter is the interface satisfied by Transport and by test doubles.
type Transporter interface {
	// Send encodes msg and delivers it to the node listening at addr.
	Send(addr string, msg *ClusterMsg) error
	// Recv returns the channel on which incoming messages are delivered.
	Recv() <-chan *ClusterMsg
	// Close shuts down the transporter.
	Close() error
}

// ─── TransportConfig ─────────────────────────────────────────────────────────

// TransportConfig holds configuration for a Transport.
// When MTLSCertFile, MTLSKeyFile, and MTLSCAFile are all set, the transport
// enables mutual TLS for both the listener and outbound connections.
// If any field is empty, plain TCP is used (backward compatible with
// single-node or test-only deployments).
type TransportConfig struct {
	// BindAddr is the host:port on which the listener is bound.
	// Use ":0" to let the OS assign an ephemeral port.
	BindAddr string
	// MTLSCertFile is the path to the PEM-encoded TLS certificate for this node.
	MTLSCertFile string
	// MTLSKeyFile is the path to the PEM-encoded private key for MTLSCertFile.
	MTLSKeyFile string
	// MTLSCAFile is the path to the PEM-encoded CA certificate used to verify
	// peer node certificates.
	MTLSCAFile string
}

// ─── poolConn ─────────────────────────────────────────────────────────────────

// poolConn is a pooled outbound connection with its own write mutex.
type poolConn struct {
	conn net.Conn
	mu   sync.Mutex
}

// ─── Transport ───────────────────────────────────────────────────────────────

// Transport is a TCP-based (or mTLS-based) inter-node transport that pools
// outbound connections. The listener accepts inbound connections; each is read
// in a dedicated goroutine and its messages are delivered to the Recv() channel.
// Transport is safe for concurrent use.
type Transport struct {
	ln     net.Listener
	recvCh chan *ClusterMsg

	// tlsCfgServer is the cached TLS config for the listener (nil = plain TCP).
	tlsCfgServer *tls.Config
	// tlsCfgClient is the cached TLS config for outbound dials (nil = plain TCP).
	tlsCfgClient *tls.Config

	// Pooled outbound connections (for sending).
	poolMu sync.Mutex
	pool   map[string]*poolConn

	// Inbound accepted connections (tracked so Close can shut them all down).
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	done chan struct{}
	wg   sync.WaitGroup
}

// NewTransport opens a plain-TCP listener on bindAddr and starts accepting
// connections. Use ":0" to let the OS assign an ephemeral port; call Addr()
// to discover it. For mTLS, use NewTransportWithConfig.
func NewTransport(bindAddr string) (*Transport, error) {
	return NewTransportWithConfig(TransportConfig{BindAddr: bindAddr})
}

// NewTransportWithConfig creates a Transport from cfg.
// When all three MTLSCertFile / MTLSKeyFile / MTLSCAFile fields are non-empty,
// the listener is wrapped with tls.NewListener and outbound dials use tls.Dial
// with mutual TLS. Otherwise plain TCP is used.
func NewTransportWithConfig(cfg TransportConfig) (*Transport, error) {
	t := &Transport{
		recvCh: make(chan *ClusterMsg, 512),
		pool:   make(map[string]*poolConn),
		conns:  make(map[net.Conn]struct{}),
		done:   make(chan struct{}),
	}

	// Build mTLS configs if all three PEM files are provided.
	if cfg.MTLSCertFile != "" && cfg.MTLSKeyFile != "" && cfg.MTLSCAFile != "" {
		serverCfg, clientCfg, err := buildMTLSConfigs(cfg.MTLSCertFile, cfg.MTLSKeyFile, cfg.MTLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("cluster: load mTLS certs: %w", err)
		}
		t.tlsCfgServer = serverCfg
		t.tlsCfgClient = clientCfg
	}

	ln, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: transport listen %s: %w", cfg.BindAddr, err)
	}

	// Wrap the listener with TLS if mTLS is configured.
	if t.tlsCfgServer != nil {
		ln = tls.NewListener(ln, t.tlsCfgServer)
	}

	t.ln = ln
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr returns the network address the listener is bound to.
func (t *Transport) Addr() string {
	return t.ln.Addr().String()
}

// Send delivers msg to the node at addr using a pooled connection.
// It re-dials automatically on a stale connection.
func (t *Transport) Send(addr string, msg *ClusterMsg) error {
	pc, err := t.getOrDial(addr)
	if err != nil {
		return fmt.Errorf("cluster: send to %s: %w", addr, err)
	}

	pc.mu.Lock()
	writeErr := writeMsg(pc.conn, msg)
	pc.mu.Unlock()

	if writeErr != nil {
		// Evict dead connection and retry once.
		t.evict(addr, pc)
		pc2, err2 := t.getOrDial(addr)
		if err2 != nil {
			return fmt.Errorf("cluster: send retry to %s: %w", addr, writeErr)
		}
		pc2.mu.Lock()
		writeErr = writeMsg(pc2.conn, msg)
		pc2.mu.Unlock()
		if writeErr != nil {
			t.evict(addr, pc2)
			return fmt.Errorf("cluster: send after retry to %s: %w", addr, writeErr)
		}
	}
	return nil
}

// Recv returns the channel on which incoming messages are delivered.
func (t *Transport) Recv() <-chan *ClusterMsg {
	return t.recvCh
}

// Close shuts down the listener, all accepted connections, and all pooled
// outbound connections. Blocks until every background goroutine has exited.
func (t *Transport) Close() error {
	select {
	case <-t.done:
		return nil
	default:
	}
	close(t.done)

	err := t.ln.Close()

	t.connsMu.Lock()
	for conn := range t.conns {
		conn.Close()
	}
	t.connsMu.Unlock()

	t.poolMu.Lock()
	for addr, pc := range t.pool {
		pc.conn.Close()
		delete(t.pool, addr)
	}
	t.poolMu.Unlock()

	t.wg.Wait()
	return err
}

// ─── mTLS helpers ─────────────────────────────────────────────────────────────

// buildMTLSConfigs loads certFile/keyFile and builds server and client TLS
// configs that both require mutual authentication against caFile.
func buildMTLSConfigs(certFile, keyFile, caFile string) (*tls.Config, *tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read CA file: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("failed to parse CA certificate from %s", caFile)
	}

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	clientCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		// InsecureSkipVerify is false; cert validation uses RootCAs + ServerName.
		MinVersion: tls.VersionTLS13,
	}
	return serverCfg, clientCfg, nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (t *Transport) getOrDial(addr string) (*poolConn, error) {
	t.poolMu.Lock()
	if pc, ok := t.pool[addr]; ok {
		t.poolMu.Unlock()
		return pc, nil
	}
	t.poolMu.Unlock()

	var conn net.Conn
	var err error

	if t.tlsCfgClient != nil {
		// Clone the client config and set ServerName for peer cert validation.
		// The ServerName is the addr (host portion only) because self-signed
		// test certificates are issued with the IP/hostname in the SAN.
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			host = addr
		}
		dialCfg := t.tlsCfgClient.Clone()
		dialCfg.ServerName = host
		conn, err = tls.DialWithDialer(
			&net.Dialer{Timeout: 5 * time.Second},
			"tcp",
			addr,
			dialCfg,
		)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("cluster: dial %s: %w", addr, err)
	}

	t.poolMu.Lock()
	if existing, ok := t.pool[addr]; ok {
		t.poolMu.Unlock()
		conn.Close()
		return existing, nil
	}
	pc := &poolConn{conn: conn}
	t.pool[addr] = pc
	t.poolMu.Unlock()
	return pc, nil
}

func (t *Transport) evict(addr string, pc *poolConn) {
	t.poolMu.Lock()
	if current, ok := t.pool[addr]; ok && current == pc {
		delete(t.pool, addr)
	}
	t.poolMu.Unlock()
	pc.conn.Close()
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
				continue
			}
		}
		t.connsMu.Lock()
		t.conns[conn] = struct{}{}
		t.connsMu.Unlock()

		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

func (t *Transport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer func() {
		t.connsMu.Lock()
		delete(t.conns, conn)
		t.connsMu.Unlock()
		conn.Close()
	}()
	for {
		msg, err := readMsg(conn)
		if err != nil {
			return
		}
		select {
		case t.recvCh <- msg:
		case <-t.done:
			return
		}
	}
}

// ─── Wire encoding ────────────────────────────────────────────────────────────

// writeMsg encodes msg into the cluster wire format and writes it to conn.
// Wire layout: [ magic(2) | type(1) | bodyLen(4) | body(N) ]
func writeMsg(conn net.Conn, msg *ClusterMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("cluster: marshal msg: %w", err)
	}
	buf := make([]byte, 7+len(body))
	buf[0] = wireMagic0
	buf[1] = wireMagic1
	buf[2] = byte(msg.Type)
	binary.BigEndian.PutUint32(buf[3:7], uint32(len(body)))
	copy(buf[7:], body)
	_, err = conn.Write(buf)
	return err
}

// readMsg reads one cluster message from conn.
func readMsg(conn net.Conn) (*ClusterMsg, error) {
	var hdr [7]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, err
	}
	if hdr[0] != wireMagic0 || hdr[1] != wireMagic1 {
		return nil, fmt.Errorf("cluster: invalid magic bytes %02x %02x", hdr[0], hdr[1])
	}
	bodyLen := binary.BigEndian.Uint32(hdr[3:7])
	if bodyLen > maxMsgBody {
		return nil, fmt.Errorf("cluster: message body too large: %d bytes", bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	var msg ClusterMsg
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("cluster: unmarshal msg: %w", err)
	}
	return &msg, nil
}
