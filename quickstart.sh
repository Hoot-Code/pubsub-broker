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
AUTH_ENABLED=false
API_KEY=""

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
warn()  { printf "\033[1;33m⚠ %s\033[0m\n" "$*"; }
err()   { printf "\033[1;31m✗ %s\033[0m\n" "$*" >&2; }

generate_api_key() {
  if command -v openssl &>/dev/null; then
    openssl rand -hex 32 | sed 's/^/pb_/' | head -c 67
  elif [[ -r /dev/urandom ]]; then
    dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n' | sed 's/^/pb_/' | head -c 67
  else
    err "Cannot generate secure random key: no openssl or /dev/urandom"
    exit 1
  fi
}

# ─── 0. Authentication Setup ───────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Authentication Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Choose how to configure API access:"
echo ""
echo "  [1] Automatic (recommended)"
echo "      Generate a secure API key automatically."
echo ""
echo "  [2] Manual"
echo "      Enter your own API key."
echo ""
echo "  [3] Disable authentication (development only)"
echo ""
printf "  Selection [1/2/3]: "
read -r AUTH_CHOICE
AUTH_CHOICE="${AUTH_CHOICE:-1}"

case "$AUTH_CHOICE" in
  1)
    API_KEY=$(generate_api_key)
    AUTH_ENABLED=true
    info "Generated API key: $API_KEY"
    ;;
  2)
    printf "  Enter your API key (min 32 chars): "
    read -r MANUAL_KEY
    if [[ ${#MANUAL_KEY} -lt 32 ]]; then
      err "API key must be at least 32 characters."
      exit 1
    fi
    # Reject weak keys that are all the same character.
    UNIQUE_CHARS=$(echo -n "$MANUAL_KEY" | fold -w1 | sort -u | wc -l)
    if [[ "$UNIQUE_CHARS" -lt 4 ]]; then
      err "API key is too weak (not enough unique characters)."
      exit 1
    fi
    API_KEY="$MANUAL_KEY"
    AUTH_ENABLED=true
    info "Using provided API key"
    ;;
  3)
    AUTH_ENABLED=false
    warn "Authentication is disabled."
    warn "Do not use this configuration in production."
    ;;
  *)
    err "Invalid selection: $AUTH_CHOICE"
    exit 1
    ;;
esac
echo ""

# ─── 1. Build ──────────────────────────────────────────────────────────────────
info "Building broker and brokectl..."
mkdir -p build
go build -trimpath -o build/broker    ./cmd/broker
go build -trimpath -o build/brokectl  ./cmd/brokectl
ok "Binaries built in build/"

# ─── 2. Write config ──────────────────────────────────────────────────────────
info "Writing temporary config to $CONFIG_FILE..."

if [[ "$AUTH_ENABLED" == "true" ]]; then
  cat > "$CONFIG_FILE" <<EOF
{
  "broker":  { "node_id": "quickstart-node" },
  "network": { "host": "127.0.0.1", "port": $BROKER_PORT, "dashboard_enabled": true },
  "storage": {
    "wal_path":  "$DATA_DIR/wal",
    "data_path": "$DATA_DIR/segments",
    "segment_max_bytes": 134217728,
    "index_interval_bytes": 4096
  },
  "auth": {
    "enabled": true,
    "api_keys": [{
      "key": "$API_KEY",
      "client_id": "quickstart-user",
      "role": "admin"
    }]
  },
  "logging": { "level": "info", "format": "text" }
}
EOF
else
  cat > "$CONFIG_FILE" <<EOF
{
  "broker":  { "node_id": "quickstart-node" },
  "network": { "host": "127.0.0.1", "port": $BROKER_PORT, "dashboard_enabled": true },
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
fi
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
if [[ "$AUTH_ENABLED" == "true" ]]; then
  ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" --key "$API_KEY" topic create --name orders --partitions 4
else
  ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" topic create --name orders --partitions 4
fi
ok "Topic 'orders' created"

# ─── 5. Publish messages ──────────────────────────────────────────────────────
info "Publishing 5 messages..."
for i in 1 2 3 4 5; do
  if [[ "$AUTH_ENABLED" == "true" ]]; then
    ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" --key "$API_KEY" publish \
      --topic orders --key "order-$i" \
      --payload "{\"id\":$i,\"amount\":$((i * 10)).00}"
  else
    ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" publish \
      --topic orders --key "order-$i" \
      --payload "{\"id\":$i,\"amount\":$((i * 10)).00}"
  fi
done
ok "5 messages published"

# ─── 6. List topics ───────────────────────────────────────────────────────────
info "Listing topics..."
  if [[ "$AUTH_ENABLED" == "true" ]]; then
    ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" --key "$API_KEY" topic list
else
  ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" topic list
fi
ok "Topics listed"

# ─── 7. Fetch messages ────────────────────────────────────────────────────────
info "Fetching messages (all partitions)..."
  if [[ "$AUTH_ENABLED" == "true" ]]; then
    ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" --key "$API_KEY" tail \
      --topic orders --count 5
else
  ./build/brokectl --addr "127.0.0.1:$BROKER_PORT" tail \
    --topic orders --count 5
fi
ok "Messages fetched"

# ─── 8. Health check ──────────────────────────────────────────────────────────
info "Running health check..."
./build/brokectl --addr "127.0.0.1:$BROKER_PORT" health
ok "Health check passed"

# ─── 9. Metrics ──────────────────────────────────────────────────────────────
info "Fetching metrics summary..."
curl -sf "http://127.0.0.1:$HTTP_PORT/metrics" | head -20
ok "Metrics endpoint working"

# ─── 10. Summary ─────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ok "Quickstart complete!"
echo ""
echo "  Dashboard:  http://127.0.0.1:$HTTP_PORT/dashboard"
echo ""
echo "  Broker:     127.0.0.1:$BROKER_PORT  (binary protocol)"
echo "  HTTP admin: 127.0.0.1:$HTTP_PORT"
echo ""

if [[ "$AUTH_ENABLED" == "true" ]]; then
  echo "  Authentication: Enabled"
  echo "  API Key:        $API_KEY"
  echo ""
  warn "Save this key. It will not be shown again."
else
  echo "  Authentication: Disabled"
  warn "Do not use this configuration in production."
fi

echo ""
echo "  Data dir:   $DATA_DIR"
echo "  Config:     $CONFIG_FILE"
echo ""
echo "Press Ctrl-C to stop the broker."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Wait for Ctrl-C.
wait "$BROKER_PID" 2>/dev/null || true
