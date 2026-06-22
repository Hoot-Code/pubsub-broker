// Package auth provides authentication and role-based access control for the
// pubsub broker.  See rbac.go for Role, Permission, and Identity definitions.
package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// Authenticator validates API keys and returns the associated Identity.
// It is safe for concurrent use. Keys are hashed with SHA-256 before being
// stored so that the plaintext key never appears in memory after initialisation.
type Authenticator struct {
	mu      sync.RWMutex
	keys    map[string]*Identity // sha256(key) → Identity
	enabled bool
}

// NewAuthenticator creates an Authenticator from the given config.
// When cfg.Enabled is false the authenticator grants RoleAdmin to every key.
func NewAuthenticator(cfg config.AuthConfig) *Authenticator {
	a := &Authenticator{
		keys:    make(map[string]*Identity, len(cfg.APIKeys)),
		enabled: cfg.Enabled,
	}
	for _, k := range cfg.APIKeys {
		id := &Identity{
			ClientID: k.ClientID,
			Topics:   k.Topics,
		}
		if k.Role != "" {
			id.Role = Role(k.Role)
		} else {
			id.Role = roleFromLegacy(k.Permissions)
		}
		a.keys[hashKey(k.Key)] = id
	}
	return a
}

// Authenticate looks up key and returns its Identity on success.
// When auth is disabled, every key is accepted and granted RoleAdmin.
// Returns an error if auth is enabled and the key is unknown.
func (a *Authenticator) Authenticate(key string) (*Identity, error) {
	if !a.enabled {
		return &Identity{ClientID: "anonymous", Role: RoleAdmin}, nil
	}
	a.mu.RLock()
	id, ok := a.keys[hashKey(key)]
	a.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("auth: unknown API key")
	}
	return id, nil
}

// AddKey adds (or replaces) a key with the given clientID, role, and optional
// topic allowlist. Safe to call concurrently.
func (a *Authenticator) AddKey(key, clientID string, role Role, topics []string) {
	id := &Identity{
		ClientID: clientID,
		Role:     role,
		Topics:   topics,
	}
	a.mu.Lock()
	a.keys[hashKey(key)] = id
	a.mu.Unlock()
}

// RemoveKey deletes key from the authenticator. Safe to call concurrently.
func (a *Authenticator) RemoveKey(key string) {
	a.mu.Lock()
	delete(a.keys, hashKey(key))
	a.mu.Unlock()
}

// RevokeKey is an alias for RemoveKey, kept for backward compatibility with
// existing call sites.
func (a *Authenticator) RevokeKey(key string) { a.RemoveKey(key) }

// hashKey returns the hex-encoded SHA-256 of key.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
