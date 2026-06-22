package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
)

// loginRequest is the JSON body for POST /dashboard/login.
type loginRequest struct {
	APIKey string `json:"api_key"`
}

// httpDashboardLogin handles POST /dashboard/login.
// It validates an API key, creates a session, and sets a session cookie.
func (b *Broker) httpDashboardLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Rate-limit by remote IP using a dedicated bucket prefix.
	remoteIP := extractIP(r.RemoteAddr)
	rateKey := "dashlogin:" + remoteIP
	if !b.rateLimiter.AllowClient(rateKey) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = writeJSON(w, map[string]string{"error": "rate limit exceeded"})
		return
	}

	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, map[string]string{"error": "invalid request body"})
		return
	}
	if req.APIKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, map[string]string{"error": "api_key is required"})
		return
	}

	b.authMu.RLock()
	authenticator := b.auth
	b.authMu.RUnlock()

	id, err := authenticator.Authenticate(req.APIKey)
	if err != nil {
		b.logHTTPAudit(audit.EventAuth, r, "", false, "invalid credentials")
		w.WriteHeader(http.StatusUnauthorized)
		_ = writeJSON(w, map[string]string{"error": "invalid credentials"})
		return
	}

	sess, err := b.sessionStore.Create(id, r.RemoteAddr)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = writeJSON(w, map[string]string{"error": "failed to create session"})
		return
	}

	b.logHTTPAudit(audit.EventAuth, r, "", true, "")

	ttl := b.cfg.Get().Network.DashboardSessionTTL
	if ttl <= 0 {
		ttl = DashboardSessionTTLDefault
	}
	cookie := &http.Cookie{
		Name:     "pubsub_dashboard_session",
		Value:    sess.Token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
		MaxAge:   int(ttl.Seconds()),
	}
	http.SetCookie(w, cookie)
	_ = writeJSON(w, map[string]string{"status": "ok"})
}

// httpDashboardLogout handles POST /dashboard/logout.
// It revokes the session and clears the cookie.
func (b *Broker) httpDashboardLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cookie, err := r.Cookie("pubsub_dashboard_session")
	if err == nil && cookie.Value != "" {
		b.sessionStore.Revoke(cookie.Value)
	}

	clearCookie := &http.Cookie{
		Name:     "pubsub_dashboard_session",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
		MaxAge:   -1,
	}
	http.SetCookie(w, clearCookie)
	_ = writeJSON(w, map[string]string{"status": "ok"})
}

// httpDashboardSession handles GET /dashboard/session.
// Returns the current session's identity, or a synthetic open-access identity
// when dashboard auth is disabled.
func (b *Broker) httpDashboardSession(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !b.dashboardAuthEnabled() {
		_ = writeJSON(w, map[string]interface{}{
			"client_id": "anonymous",
			"role":      "admin",
			"topics":    []string{},
		})
		return
	}

	cookie, err := r.Cookie("pubsub_dashboard_session")
	if err != nil || cookie.Value == "" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = writeJSON(w, map[string]string{"error": "not authenticated"})
		return
	}

	sess, ok := b.sessionStore.Get(cookie.Value)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = writeJSON(w, map[string]string{"error": "not authenticated"})
		return
	}

	_ = writeJSON(w, map[string]interface{}{
		"client_id": sess.Identity.ClientID,
		"role":      string(sess.Identity.Role),
		"topics":    sess.Identity.Topics,
	})
}

// dashboardAuthEnabled reports whether the dashboard requires login.
// It is true when both DashboardEnabled and auth.Enabled are true, unless
// DashboardAuthEnabled is explicitly overridden to false via config.
func (b *Broker) dashboardAuthEnabled() bool {
	cfg := b.cfg.Get()
	if !cfg.Network.DashboardEnabled {
		return false
	}
	if !cfg.Auth.Enabled {
		return false
	}
	if cfg.Network.DashboardAuthEnabled != nil && !*cfg.Network.DashboardAuthEnabled {
		return false
	}
	return true
}

// extractIP strips the port from a "host:port" remote address string.
func extractIP(remoteAddr string) string {
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		return remoteAddr[:idx]
	}
	return remoteAddr
}

// logHTTPAudit emits an audit event for an HTTP request. It is the HTTP
// equivalent of logAudit used by the binary protocol handlers.
func (b *Broker) logHTTPAudit(evType audit.EventType, r *http.Request,
	topic string, success bool, errMsg string) {
	if b.audit == nil {
		return
	}
	clientID := "anonymous"
	if id := IdentityFromContext(r.Context()); id != nil {
		clientID = id.ClientID
	}
	details := map[string]string{"source": "dashboard"}
	if errMsg != "" {
		details["error_detail"] = errMsg
	}
	b.audit.Log(audit.Event{
		Type:       evType,
		ClientID:   clientID,
		RemoteAddr: extractIP(r.RemoteAddr),
		Topic:      topic,
		Success:    success,
		Error:      errMsg,
		Details:    details,
	})
}

// writeJSON encodes v as JSON and writes it to w.
func writeJSON(w http.ResponseWriter, v interface{}) error {
	return json.NewEncoder(w).Encode(v)
}

// Ensure fmt is used for potential future formatting needs.
var _ = fmt.Sprintf
