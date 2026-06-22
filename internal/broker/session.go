package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
)

// DashboardSessionTTLDefault is the default session lifetime when
// DashboardSessionTTL is not configured.
const DashboardSessionTTLDefault = 12 * time.Hour

// DashboardSession represents an authenticated dashboard session, binding a
// random token to an RBAC Identity.
type DashboardSession struct {
	Token      string
	Identity   *auth.Identity
	CreatedAt  time.Time
	ExpiresAt  time.Time
	RemoteAddr string
}

// SessionStore is a thread-safe in-memory store for dashboard sessions.
// Expired sessions are evicted lazily on read and periodically by a
// cleanup goroutine started via StartCleanup.
type SessionStore struct {
	mu       sync.Mutex
	sessions map[string]*DashboardSession
	ttl      time.Duration
}

// NewSessionStore creates a SessionStore with the given session TTL.
func NewSessionStore(ttl time.Duration) *SessionStore {
	if ttl <= 0 {
		ttl = DashboardSessionTTLDefault
	}
	return &SessionStore{
		sessions: make(map[string]*DashboardSession),
		ttl:      ttl,
	}
}

// Create generates a cryptographically random 256-bit session token, stores
// the session, and returns it. The session expires after the store's TTL.
func (s *SessionStore) Create(identity *auth.Identity, remoteAddr string) (*DashboardSession, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("session: generate token: %w", err)
	}
	token := hex.EncodeToString(b)
	now := time.Now()
	sess := &DashboardSession{
		Token:      token,
		Identity:   identity,
		CreatedAt:  now,
		ExpiresAt:  now.Add(s.ttl),
		RemoteAddr: remoteAddr,
	}
	s.mu.Lock()
	s.sessions[token] = sess
	s.mu.Unlock()
	return sess, nil
}

// Get returns the session for token if it exists and has not expired.
// Expired sessions are lazily evicted.
func (s *SessionStore) Get(token string) (*DashboardSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[token]
	if !ok {
		return nil, false
	}
	if time.Now().After(sess.ExpiresAt) {
		delete(s.sessions, token)
		return nil, false
	}
	return sess, true
}

// Revoke deletes the session for token, if present.
func (s *SessionStore) Revoke(token string) {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
}

// StartCleanup launches a background goroutine that periodically evicts
// expired sessions. The goroutine runs until ctx is cancelled.
func (s *SessionStore) StartCleanup(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.evictExpired()
			}
		}
	}()
}

// evictExpired removes all sessions whose ExpiresAt is in the past.
func (s *SessionStore) evictExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, sess := range s.sessions {
		if now.After(sess.ExpiresAt) {
			delete(s.sessions, token)
		}
	}
}
