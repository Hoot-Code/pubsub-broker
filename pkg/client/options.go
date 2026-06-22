package client

import (
	"crypto/tls"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Client-level options ─────────────────────────────────────────────────────

// Option configures a Client.
type Option func(*clientOptions)

type clientOptions struct {
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	tlsCfg       *tls.Config
}

func defaultClientOptions() clientOptions {
	return clientOptions{
		dialTimeout:  10 * time.Second,
		readTimeout:  30 * time.Second,
		writeTimeout: 10 * time.Second,
	}
}

// WithDialTimeout sets the TCP dial timeout (default 10 s).
func WithDialTimeout(d time.Duration) Option {
	return func(o *clientOptions) { o.dialTimeout = d }
}

// WithReadTimeout sets the per-read deadline on the connection (default 30 s).
// Set to 0 to disable.
func WithReadTimeout(d time.Duration) Option {
	return func(o *clientOptions) { o.readTimeout = d }
}

// WithWriteTimeout sets the per-write deadline on the connection (default 10 s).
// Set to 0 to disable.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *clientOptions) { o.writeTimeout = d }
}

// WithTLS attaches a TLS configuration to the dialer. Pass nil to disable TLS
// (plain TCP, the default).
func WithTLS(cfg *tls.Config) Option {
	return func(o *clientOptions) { o.tlsCfg = cfg }
}

// ─── Producer options ─────────────────────────────────────────────────────────

// ProducerOption configures a Producer.
type ProducerOption func(*producerOptions)

type producerOptions struct {
	deliveryMode types.DeliveryMode
	// codec is the compression codec sent in every PublishRequest (B5).
	codec uint8
	// retryOnOverload controls automatic retry when the broker responds with
	// BROKER_OVERLOADED (E2). Zero maxRetries means no retry.
	retryMaxRetries int
	retryBackoff    time.Duration
}

func defaultProducerOptions() producerOptions {
	return producerOptions{deliveryMode: types.AtLeastOnce}
}

// WithDeliveryMode sets the delivery guarantee for published messages.
// Use types.ExactlyOnce to enable idempotent (auto-incrementing SeqNum) mode.
// Defaults to types.AtLeastOnce.
func WithDeliveryMode(m types.DeliveryMode) ProducerOption {
	return func(o *producerOptions) { o.deliveryMode = m }
}

// WithCompression sets the compression codec for all messages published by this
// Producer. Pass 0 (CodecNone) to disable compression (the default).
// Use the codec constants from internal/compress — the uint8 values are 0 (none),
// 1 (flate), 2 (zlib) — so that pkg/client remains free of internal/ imports.
func WithCompression(codec uint8) ProducerOption {
	return func(o *producerOptions) { o.codec = codec }
}

// WithRetryOnOverload instructs the Producer to automatically retry a publish
// when the broker responds with ErrBrokerOverloaded, up to maxRetries times.
// Each retry waits backoff × 2^attempt (exponential backoff, capped at 30 s).
// When maxRetries is 0 or this option is not set, ErrBrokerOverloaded is
// returned immediately to the caller.
func WithRetryOnOverload(maxRetries int, backoff time.Duration) ProducerOption {
	return func(o *producerOptions) {
		o.retryMaxRetries = maxRetries
		o.retryBackoff = backoff
	}
}

// ─── Consumer options ─────────────────────────────────────────────────────────

// ConsumerOption configures a Consumer.
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	bufferSize int
}

func defaultConsumerOptions() consumerOptions {
	return consumerOptions{bufferSize: 256}
}

// WithBufferSize sets the capacity of the Messages() channel (default 256).
func WithBufferSize(n int) ConsumerOption {
	return func(o *consumerOptions) { o.bufferSize = n }
}
