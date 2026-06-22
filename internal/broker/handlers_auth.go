package broker

import (
	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// handleAuth authenticates the client and attaches an RBAC Identity to conn.
// On success the connection is marked as authenticated and subsequent frames
// are dispatched normally. On failure a CmdResponse with OK=false is returned
// (not CmdError) so the client can distinguish auth failure from wire errors.
func (b *Broker) handleAuth(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.AuthRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		b.logAudit(audit.EventAuth, conn, "", false, "bad auth body")
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid auth body")
	}

	b.authMu.RLock()
	authenticator := b.auth
	b.authMu.RUnlock()

	id, err := authenticator.Authenticate(req.APIKey)
	if err != nil {
		b.logAudit(audit.EventAuth, conn, "", false, err.Error())
		return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.AuthResponse{
			OK:     false,
			Reason: err.Error(),
		})
	}

	// Derive the legacy permission list from the role for backward compatibility
	// with conn.HasPerm(). The RBAC path uses conn.Can() / conn.SetIdentity().
	perms := identityPerms(id)
	conn.SetAuth(id.ClientID, perms)
	conn.SetIdentity(id) // RBAC identity: enables conn.Can(perm, topic)

	b.logAudit(audit.EventAuth, conn, "", true, "")
	return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.AuthResponse{
		OK:    true,
		Perms: perms,
	})
}

// identityPerms converts an Identity's Role into a []string of permission
// names for backward-compatible legacy code that calls conn.HasPerm(string).
func identityPerms(id *auth.Identity) []string {
	rolePerms := auth.RolePermissions[id.Role]
	out := make([]string, len(rolePerms))
	for i, p := range rolePerms {
		out[i] = string(p)
	}
	return out
}
