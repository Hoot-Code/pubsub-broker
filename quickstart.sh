#!/usr/bin/env bash
set -euo pipefail

# ─── pubsub-broker Quickstart ──────────────────────────────────────────────────
# Builds, starts, and demos the broker. Ctrl-C stops the broker cleanly.
#
# Usage:
#   chmod +x quickstart.sh && ./quickstart.sh
# ──────────────────────────────────────────────────────────────────────────────

BROKER_PORT=9000
HTTP_PORT=9001
DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pubsub-qs-XXXXXX")
CONFIG_FILE="$DATA_DIR/broker.json"
BROKER_PID=""

cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    echo ""
    echo "Stopping broker (pid $BROKER_PID)..."
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

info()  { printf "\033[1;34m▸ %s\033[0m\n" "$*"; }
ok()    { printf "\033[1;32m✓ %s\033[0m\n" "$*"; }
err()   { printf "\033[1;31m✗ %s\033[0m\n" "$*" >&2; }

# ─── 1. Build ──────────────────────────────────────────────────────────────────
info "Building broker and brokectl..."
mkdir -p build
go build -trimpath -o build/broker    ./cmd/broker
go build -trimpath -o build/brokectl  ./cmd/brokectl
ok "Binaries built in build/"

# ─── 2. Write config ──────────────────────────────────────────────────────────
info "Writing temporary config to $CONFIG_FILE..."
cat > "$CONFIG_FILE" <<EOF
{
  "broker":  { "node_id": "quickstart-node" },
  "network": { "host": "127.0.0.1", "port": $BROKER_PORT },
  "storage": {
    "wal_path":  "$DATA_DIR/wal",
    "data_path": "$DATA_DIR/segments",
    "segment_max_bytes": 134217728,
    "index_interval_bytes": 4096
  },
  "auth": { "enabled": false },
  "logging": { "level": "info", "format": "text" }
}
EOF
ok "Config written"

# ─── 3. Start broker ──────────────────────────────────────────────────────────
info "Starting broker on :$BROKER_PORT (HTTP admin :$HTTP_PORT)..."
./build/broker -config "$CONFIG_FILE" &
BROKER_PID=$!

# Wait for the broker to become ready.
info "Waiting for broker to become ready..."
for i in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$HTTP_PORT/healthz/live" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

if ! curl -sf "http://127.0.0.1:$HTTP_PORT/healthz/live" >/dev/null 2>&1; then
  err "Broker failed to start within 5 seconds"
  exit 1
fi
ok "Broker ready (pid $BROKER_PID)"

# ─── 4. Create a topic ────────────────────────────────────────────────────────
info "Creating topic 'orders' with 4 partitions..."
./build/brokectl --addr "127.0.0.1:$BROKER_PORT" topic create --name orders --partitions 4
ok "Topic 'orders' created"

# ─── 5. Publish messages ──────────────────────────────────────────────────────
info "Publishing 5 messages..."
for i in 1 2 3 4 5; do
  ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" publish \
    --topic orders --key "order-$i" \
    --payload "{\"id\":$i,\"amount\":$((i * 10)).00}"
done
ok "5 messages published"

# ─── 6. List topics ───────────────────────────────────────────────────────────
info "Listing topics..."
./build/brokectl --addr "127.0.0.1:$BROKER_PORT" topic list
ok "Topics listed"

# ─── 7. Fetch messages ────────────────────────────────────────────────────────
info "Fetching messages (all partitions)..."
./build/brokectl --addr "127.0.0.1:$BROKER_PORT" tail \
  --topic orders --count 5
ok "Messages fetched"

# ─── 8. Health check ──────────────────────────────────────────────────────────
info "Running health check..."
./build/brokectl --addr "127.0.0.1:$BROKER_PORT" health
ok "Health check passed"

# ─── 9. Dashboard ─────────────────────────────────────────────────────────────
info "Dashboard available at http://127.0.0.1:$HTTP_PORT/dashboard"

# ─── 10. Metrics ──────────────────────────────────────────────────────────────
info "Fetching metrics summary..."
curl -sf "http://127.0.0.1:$HTTP_PORT/metrics" | head -20
ok "Metrics endpoint working"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ok "Quickstart complete!"
echo ""
echo "  Broker:     127.0.0.1:$BROKER_PORT  (binary protocol)"
echo "  HTTP admin: 127.0.0.1:$HTTP_PORT"
echo "  Dashboard:  http://127.0.0.1:$HTTP_PORT/dashboard"
echo "  Metrics:    http://127.0.0.1:$HTTP_PORT/metrics"
echo ""
echo "  Data dir:   $DATA_DIR"
echo "  Config:     $CONFIG_FILE"
echo ""
echo "Press Ctrl-C to stop the broker."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Wait for Ctrl-C.
wait "$BROKER_PID" 2>/dev/null || true
