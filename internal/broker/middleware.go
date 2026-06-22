package broker

import (
	"context"
	"net/http"
	"strings"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
)

// identityKey is an unexported context key for the resolved Identity.
type identityKey struct{}

// IdentityFromContext extracts the Identity stored by requireAuth/resolveIdentity.
// Returns nil if no identity is present in the context.
func IdentityFromContext(ctx context.Context) *auth.Identity {
	id, _ := ctx.Value(identityKey{}).(*auth.Identity)
	return id
}

// resolveIdentity determines the caller's Identity from the request.
//
// Resolution order:
//  1. If dashboard auth is disabled (DashboardAuthEnabled is false), return a
//     synthetic "anonymous" admin identity — this preserves the existing
//     open-access semantics when auth is off.
//  2. Try the Authorization header first (existing behaviour for brokectl,
//     API callers, and scripts) — if present and valid, use it.
//  3. Try the dashboard session cookie — if present and valid, use its
//     Identity.
//  4. Return nil (caller responds 401 or redirects).
//
// Endpoints that must NEVER require auth (/metrics, /healthz/*,
// /dashboard/login, /dashboard/logout, /dashboard/session) are excluded
// from requireAuth and never call resolveIdentity.
func (b *Broker) resolveIdentity(r *http.Request) *auth.Identity {
	if !b.dashboardAuthEnabled() {
		return &auth.Identity{ClientID: "anonymous", Role: auth.RoleAdmin}
	}
	if id := b.resolveFromHeader(r); id != nil {
		return id
	}
	if id := b.resolveFromSessionCookie(r); id != nil {
		return id
	}
	return nil
}

// resolveFromHeader extracts and validates an API key from the Authorization
// header. Returns nil when the header is missing or the key is invalid.
func (b *Broker) resolveFromHeader(r *http.Request) *auth.Identity {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return nil
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return nil
	}
	apiKey := strings.TrimPrefix(authz, prefix)
	if apiKey == "" {
		return nil
	}

	b.authMu.RLock()
	authenticator := b.auth
	b.authMu.RUnlock()

	id, err := authenticator.Authenticate(apiKey)
	if err != nil {
		return nil
	}
	return id
}

// resolveFromSessionCookie looks up a dashboard session by cookie and returns
// the associated Identity. Returns nil when the cookie is missing or the
// session is expired.
func (b *Broker) resolveFromSessionCookie(r *http.Request) *auth.Identity {
	cookie, err := r.Cookie("pubsub_dashboard_session")
	if err != nil || cookie.Value == "" {
		return nil
	}
	sess, ok := b.sessionStore.Get(cookie.Value)
	if !ok {
		return nil
	}
	return sess.Identity
}

// requireAuth wraps an HTTP handler so that it requires a valid identity.
// When the caller is unauthenticated:
//   - GET /dashboard triggers a 302 redirect to /dashboard/login.
//   - All other routes return 401 {"error":"authentication required"}.
//
// Endpoints excluded from auth (documented here for discoverability):
//   - /healthz/*, /health, /status — Kubernetes probes, must never gate.
//   - /metrics — Prometheus scrape; scrapers can't do interactive login.
//   - /dashboard/login, /dashboard/logout, /dashboard/session — the auth
//     endpoints themselves.
func (b *Broker) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := b.resolveIdentity(r)
		if id == nil {
			if r.URL.Path == "/dashboard" || r.URL.Path == "/" {
				http.Redirect(w, r, "/dashboard", http.StatusFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = writeJSON(w, map[string]string{"error": "authentication required"})
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), identityKey{}, id))
		next(w, r)
	}
}
