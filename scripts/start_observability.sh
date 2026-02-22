#!/usr/bin/env bash
# Start the observability stack (Loki, Promtail, Grafana) as background processes.
# Stop with: ./scripts/stop_observability.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="${PROJECT_DIR}/bin"
DATA_DIR="${PROJECT_DIR}/data/observability"
CONF_DIR="${PROJECT_DIR}/observability"
PID_DIR="${DATA_DIR}/pids"
LOG_DIR="${PROJECT_DIR}/logs"

mkdir -p "$PID_DIR" "$LOG_DIR" "$DATA_DIR"/{loki,promtail}

# ---------- helper ----------
start_if_not_running() {
    local name="$1" pidfile="$PID_DIR/${name}.pid"
    shift
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "$name already running (PID $(cat "$pidfile"))"
        return
    fi
    "$@" &
    echo $! > "$pidfile"
    echo "$name started (PID $!)"
}

# ---------- Loki ----------
start_if_not_running loki \
    "$BIN_DIR/loki" \
    -config.file="$CONF_DIR/loki.yml"

sleep 1

# ---------- Promtail ----------
start_if_not_running promtail \
    "$BIN_DIR/promtail" \
    -config.file="$CONF_DIR/promtail.yml"

# ---------- Grafana ----------
GRAFANA_DIR="${DATA_DIR}/grafana/install"
export GF_PATHS_DATA="${DATA_DIR}/grafana/data"
export GF_PATHS_PROVISIONING="$CONF_DIR/grafana-provisioning"
export GF_SECURITY_ADMIN_PASSWORD="admin"
export GF_AUTH_ANONYMOUS_ENABLED="true"
export GF_AUTH_ANONYMOUS_ORG_ROLE="Viewer"

mkdir -p "$GF_PATHS_DATA" "$GF_PATHS_PROVISIONING/datasources"
cp "$CONF_DIR/grafana-datasources.yml" "$GF_PATHS_PROVISIONING/datasources/ds.yml"

start_if_not_running grafana \
    "$GRAFANA_DIR/bin/grafana-server" \
    --homepath="$GRAFANA_DIR"

echo ""
echo "Loki:    http://localhost:3100"
echo "Grafana: http://localhost:3000 (admin/admin)"
