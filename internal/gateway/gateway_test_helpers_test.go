package gateway

import (
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/pkg/client"
)

// NewGatewayForTest constructs a Gateway for testing. It exposes internal
// state via *ForTest methods so that tests can exercise pool behaviour
// without going through the HTTP layer.
func NewGatewayForTest(cfg config.GatewayConfig, brokerAddr string, dialOpts []client.Option, log Logger) *Gateway {
	return NewGateway(cfg, brokerAddr, dialOpts, log)
}

// ConnForTest returns a pooled connection for the given API key, creating
// one if it does not exist yet.
func (g *Gateway) ConnForTest(apiKey string) (*client.Client, error) {
	return g.connFor(apiKey)
}

// BackdatePoolForTest sets lastUsed on every pooled connection to d ago.
func (g *Gateway) BackdatePoolForTest(d time.Duration) {
	g.poolMu.Lock()
	ago := time.Now().Add(-d)
	for _, pc := range g.pool {
		pc.lastUsed = ago
	}
	g.poolMu.Unlock()
}

// EvictIdleForTest triggers the eviction sweep.
func (g *Gateway) EvictIdleForTest() {
	g.evictIdle()
}

// PoolLenForTest returns the current number of entries in the pool.
func (g *Gateway) PoolLenForTest() int {
	g.poolMu.Lock()
	defer g.poolMu.Unlock()
	return len(g.pool)
}
