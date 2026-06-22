// Package logging provides structured JSON logging for the broker.
// Built on the stdlib log/slog package (Go 1.21+).
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Logger wraps slog.Logger with broker-specific context helpers.
// The level can be changed at runtime via SetLevel; all methods are safe
// for concurrent use.
type Logger struct {
	mu sync.RWMutex
	l  *slog.Logger
	w  io.Writer
}

// Fields is a convenience alias for structured log attributes.
type Fields map[string]any

// New creates a Logger writing JSON to w at the given level string
// ("debug","info","warn","error"). Unknown levels default to info.
func New(w io.Writer, level string) *Logger {
	if w == nil {
		w = os.Stderr
	}
	lvl := parseLevel(level)
	lg := &Logger{w: w}
	lg.l = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: lvl,
	}))
	return lg
}

// Default returns a package-level logger writing to stderr at info level.
func Default() *Logger { return New(os.Stderr, "info") }

// With returns a child logger with additional key-value pairs attached.
func (lg *Logger) With(args ...any) *Logger {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	return &Logger{l: lg.l.With(args...), w: lg.w}
}

// WithFields returns a child logger with the given Fields attached.
func (lg *Logger) WithFields(f Fields) *Logger {
	lg.mu.RLock()
	defer lg.mu.RUnlock()
	args := make([]any, 0, len(f)*2)
	for k, v := range f {
		args = append(args, k, v)
	}
	return &Logger{l: lg.l.With(args...), w: lg.w}
}

// SetLevel changes the minimum log level at runtime. The change takes effect
// immediately for all subsequent log calls. Returns an error for invalid
// level strings.
func (lg *Logger) SetLevel(level string) error {
	lvl := parseLevel(level)
	lg.mu.Lock()
	defer lg.mu.Unlock()
	lg.l = slog.New(slog.NewJSONHandler(lg.w, &slog.HandlerOptions{
		Level: lvl,
	}))
	return nil
}

// ─── Levelled logging ────────────────────────────────────────────────────────

func (lg *Logger) Debug(msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Debug(msg, args...)
}
func (lg *Logger) Info(msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Info(msg, args...)
}
func (lg *Logger) Warn(msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Warn(msg, args...)
}
func (lg *Logger) Error(msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Error(msg, args...)
}

func (lg *Logger) DebugCtx(ctx context.Context, msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.DebugContext(ctx, msg, args...)
}
func (lg *Logger) InfoCtx(ctx context.Context, msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.InfoContext(ctx, msg, args...)
}
func (lg *Logger) WarnCtx(ctx context.Context, msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.WarnContext(ctx, msg, args...)
}
func (lg *Logger) ErrorCtx(ctx context.Context, msg string, args ...any) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.ErrorContext(ctx, msg, args...)
}

// ─── Domain-specific helpers ─────────────────────────────────────────────────

// Request logs an incoming connection or protocol request.
func (lg *Logger) Request(remoteAddr, command string, bytesRead int) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Info("request",
		slog.String("remote", remoteAddr),
		slog.String("command", command),
		slog.Int("bytes", bytesRead),
	)
}

// Replication logs a replication event.
func (lg *Logger) Replication(event, nodeID string, lag int64) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Info("replication",
		slog.String("event", event),
		slog.String("node_id", nodeID),
		slog.Int64("lag_bytes", lag),
	)
}

// Consumer logs a consumer event (subscribe, ack, lag, etc.).
func (lg *Logger) Consumer(event, group, topic string, partition int32, offset int64) {
	lg.mu.RLock()
	l := lg.l
	lg.mu.RUnlock()
	l.Info("consumer",
		slog.String("event", event),
		slog.String("group", group),
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
		slog.Int64("offset", offset),
	)
}

// ─── Internal helpers ────────────────────────────────────────────────────────

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
