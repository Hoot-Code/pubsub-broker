// Package client provides a Go client library for the pub/sub broker.
// It has no imports from internal/; it only imports pkg/types and pkg/protocol.
package client

import "fmt"

// ─── Typed errors ────────────────────────────────────────────────────────────

// ErrNotAuthenticated is returned when an operation requires authentication
// but the client has not yet called Authenticate, or the server rejected the key.
type ErrNotAuthenticated struct {
	// Code is the broker error code string returned in the error response.
	Code string
}

// Error implements the error interface.
func (e *ErrNotAuthenticated) Error() string {
	return fmt.Sprintf("not authenticated: %s", e.Code)
}

// ErrTopicNotFound is returned when the requested topic does not exist.
type ErrTopicNotFound struct {
	Topic string
	Code  string
}

// Error implements the error interface.
func (e *ErrTopicNotFound) Error() string {
	return fmt.Sprintf("topic not found: %s (code %s)", e.Topic, e.Code)
}

// ErrBrokerOverloaded is returned when the broker rejects the request due to
// rate limiting or resource exhaustion.
type ErrBrokerOverloaded struct {
	Code string
}

// Error implements the error interface.
func (e *ErrBrokerOverloaded) Error() string {
	return fmt.Sprintf("broker overloaded: %s", e.Code)
}

// ErrConnectionClosed is returned when the operation cannot complete because
// the underlying TCP connection has already been closed.
type ErrConnectionClosed struct {
	// Cause is the underlying close reason, if available.
	Cause error
}

// Error implements the error interface.
func (e *ErrConnectionClosed) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("connection closed: %v", e.Cause)
	}
	return "connection closed"
}

// Unwrap returns the underlying cause so errors.Is/As work transitively.
func (e *ErrConnectionClosed) Unwrap() error { return e.Cause }

// ErrBroker is a generic broker-side error carrying an error code string.
type ErrBroker struct {
	Code    string
	Message string
}

// Error implements the error interface.
func (e *ErrBroker) Error() string {
	return fmt.Sprintf("broker error %s: %s", e.Code, e.Message)
}

// mapBrokerError converts a broker error-code string to a typed client error.
// Unknown codes fall back to ErrBroker.
func mapBrokerError(code, message string) error {
	switch code {
	case "UNAUTHORIZED", "AUTH_FAILED":
		return &ErrNotAuthenticated{Code: code}
	case "TOPIC_NOT_FOUND":
		return &ErrTopicNotFound{Code: code}
	case "RATE_LIMITED", "OVERLOADED":
		return &ErrBrokerOverloaded{Code: code}
	case "QUORUM_TIMEOUT":
		return &ErrQuorumTimeout{Code: code}
	default:
		return &ErrBroker{Code: code, Message: message}
	}
}

// ErrNotLeader is returned when the target partition is owned by a different
// broker node. The client should retry the request against OwnerNodeID.
type ErrNotLeader struct {
	// OwnerNodeID is the NodeID of the node that owns the partition.
	OwnerNodeID string
	// Code is the raw broker error code ("NOT_LEADER").
	Code string
}

// Error implements the error interface.
func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader: partition owned by node %s (code %s)", e.OwnerNodeID, e.Code)
}

// ErrQuorumTimeout is returned when a publish succeeded locally but the broker
// could not confirm that a quorum of ISR replicas had acknowledged the write
// before the configured deadline elapsed.
type ErrQuorumTimeout struct {
	// Code is the raw broker error code ("QUORUM_TIMEOUT").
	Code string
}

// Error implements the error interface.
func (e *ErrQuorumTimeout) Error() string {
	return fmt.Sprintf("quorum timeout: write not yet acknowledged by quorum (code %s)", e.Code)
}
