package auth

import (
	"context"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// bucket is a token-bucket rate-limit counter. It is NOT safe for concurrent
// use on its own; the owning RateLimiter serialises access through its mutex.
//
// The bucket refills continuously at `rate` tokens per second up to a maximum
// of `burst` tokens. lastSeen records the wall-clock time of the most recent
// Allow call so the cleanup goroutine can evict idle buckets.
type bucket struct {
	tokens   float64
	last     time.Time // last refill time
	lastSeen time.Time // last time this bucket was touched
}

// refill adds tokens accrued since last, capped at burst. now is passed in so
// the caller can supply a single consistent clock reading across a batch of
// buckets.
func (b *bucket) refill(rate, burst int, now time.Time) {
	if burst <= 0 {
		burst = 1
	}
	if b.last.IsZero() {
		b.last = now
		b.tokens = float64(burst)
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 && rate > 0 {
		b.tokens += elapsed * float64(rate)
	}
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	b.last = now
}

// allow consumes one token if available and reports whether the request is
// permitted. It assumes the caller already holds the RateLimiter mutex.
func (b *bucket) allow(rate, burst int, now time.Time) bool {
	b.refill(rate, burst, now)
	b.lastSeen = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// RateLimiter enforces per-client and per-topic request-rate limits using
// independent token buckets. When the embedded RateLimitConfig is disabled
// (Enabled == false), every Allow* call returns true immediately and no
// buckets are allocated.
//
// A single mutex guards both maps. The granularity is coarse on purpose: rate
// limiting is a coarse-grained control and the critical sections are tiny
// (a float64 add/subtract and a map lookup), so contention is negligible
// relative to the work each request performs.
//
// Idle buckets are reaped by StartCleanup, which runs until ctx is cancelled.
type RateLimiter struct {
	cfg     *config.RateLimitConfig
	mu      sync.Mutex
	clients map[string]*bucket
	topics  map[string]*bucket
}

// NewRateLimiter returns a RateLimiter configured by cfg. A nil cfg is treated
// as a disabled limiter so callers that receive an optional config never
// dereference a nil pointer.
func NewRateLimiter(cfg *config.RateLimitConfig) *RateLimiter {
	if cfg == nil {
		cfg = &config.RateLimitConfig{}
	}
	return &RateLimiter{
		cfg:     cfg,
		clients: make(map[string]*bucket),
		topics:  make(map[string]*bucket),
	}
}

// clientBurst returns the per-client burst capacity. When BurstMultiplier is
// zero or negative a sane default of PerClientRPS (or 1) is used so a
// misconfigured multiplier never produces a zero-capacity bucket that would
// reject every request.
func (r *RateLimiter) clientBurst() int {
	if r.cfg.PerClientRPS <= 0 {
		return 1
	}
	b := r.cfg.PerClientRPS * r.cfg.BurstMultiplier
	if r.cfg.BurstMultiplier <= 0 {
		b = r.cfg.PerClientRPS
	}
	if b < 1 {
		b = 1
	}
	return b
}

// topicBurst returns the per-topic burst capacity, with the same fallback
// semantics as clientBurst.
func (r *RateLimiter) topicBurst() int {
	if r.cfg.PerTopicRPS <= 0 {
		return 1
	}
	b := r.cfg.PerTopicRPS * r.cfg.BurstMultiplier
	if r.cfg.BurstMultiplier <= 0 {
		b = r.cfg.PerTopicRPS
	}
	if b < 1 {
		b = 1
	}
	return b
}

// AllowClient reports whether client id is permitted one more request under
// the per-client rate limit. When limiting is disabled it always returns true.
func (r *RateLimiter) AllowClient(id string) bool {
	if r.cfg == nil || !r.cfg.Enabled {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	b, ok := r.clients[id]
	if !ok {
		b = &bucket{}
		r.clients[id] = b
	}
	return b.allow(r.cfg.PerClientRPS, r.clientBurst(), now)
}

// AllowTopic reports whether topic is permitted one more request under the
// per-topic rate limit. When limiting is disabled it always returns true.
func (r *RateLimiter) AllowTopic(topic string) bool {
	if r.cfg == nil || !r.cfg.Enabled {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	b, ok := r.topics[topic]
	if !ok {
		b = &bucket{}
		r.topics[topic] = b
	}
	return b.allow(r.cfg.PerTopicRPS, r.topicBurst(), now)
}

// Allow reports whether the (clientID, topic) pair is permitted one more
// request. It checks BOTH the per-client and per-topic limits atomically
// under a single lock acquisition so that a topic denial does not consume a
// client token (and vice-versa). When limiting is disabled it always returns
// true without touching any map.
//
// Allow is retained for backward compatibility with existing callers that
// gate a single request on both dimensions at once.
func (r *RateLimiter) Allow(clientID, topic string) bool {
	if r.cfg == nil || !r.cfg.Enabled {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	cb, ok := r.clients[clientID]
	if !ok {
		cb = &bucket{}
		r.clients[clientID] = cb
	}
	if !cb.allow(r.cfg.PerClientRPS, r.clientBurst(), now) {
		return false
	}
	tb, ok := r.topics[topic]
	if !ok {
		tb = &bucket{}
		r.topics[topic] = tb
	}
	if !tb.allow(r.cfg.PerTopicRPS, r.topicBurst(), now) {
		// Refund the client token we just consumed: the request was denied by
		// the topic gate, so it should not count against the client budget.
		cb.tokens++
		return false
	}
	return true
}

// UpdateLimits atomically swaps the active rate-limit configuration. New
// requests immediately use the new limits. Existing in-flight bucket state
// is reset so that stale counters from the old limits do not carry over.
func (r *RateLimiter) UpdateLimits(cfg *config.RateLimitConfig) {
	if cfg == nil {
		cfg = &config.RateLimitConfig{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cfg = cfg
	// Reset all buckets so stale token counts from the old limits don't
	// carry over. This is acceptable because rate-limit updates are
	// infrequent admin actions.
	r.clients = make(map[string]*bucket)
	r.topics = make(map[string]*bucket)
}

// StartCleanup launches a background goroutine that periodically evicts
// buckets whose most recent Allow call is older than ttl. The goroutine runs
// until ctx is cancelled, at which point it exits cleanly. StartCleanup is
// idempotent in the sense that each call starts its own reaper; callers
// normally invoke it exactly once with the broker's shutdown context.
//
// A ttl <= 0 is a no-op: nothing is started and the goroutine is never
// scheduled, which lets unit tests construct a limiter without spawning
// goroutines that could outlive the test.
func (r *RateLimiter) StartCleanup(ctx context.Context, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.evictIdle(ttl)
			}
		}
	}()
}

// evictIdle removes every bucket whose lastSeen is older than ttl. It is
// called only by the cleanup goroutine.
func (r *RateLimiter) evictIdle(ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for id, b := range r.clients {
		if !b.lastSeen.IsZero() && now.Sub(b.lastSeen) > ttl {
			delete(r.clients, id)
		}
	}
	for topic, b := range r.topics {
		if !b.lastSeen.IsZero() && now.Sub(b.lastSeen) > ttl {
			delete(r.topics, topic)
		}
	}
}
