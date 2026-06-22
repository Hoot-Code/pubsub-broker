// Package gateway implements the optional HTTP/WebSocket gateway.
// This file re-exports the shared WebSocket implementation from
// internal/wsutil so that gateway.go can reference the same types and
// functions without duplicating logic.
package gateway

import (
	"net/http"

	"github.com/Hoot-Code/pubsub-broker/internal/wsutil"
)

// wsConn is a type alias for the shared wsutil.Conn so that gateway.go
// references remain valid after the extraction.
type wsConn = wsutil.Conn

// upgradeWebSocket re-exports wsutil.UpgradeWebSocket for use within this
// package.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	return wsutil.UpgradeWebSocket(w, r)
}

// wsutilComputeAcceptKey re-exports wsutil.ComputeAcceptKey for white-box
// testing in websocket_internal_test.go.
func wsutilComputeAcceptKey(key string) string {
	return wsutil.ComputeAcceptKey(key)
}
