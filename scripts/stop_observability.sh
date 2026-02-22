#!/usr/bin/env bash
# Stop the observability stack.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PID_DIR="${PROJECT_DIR}/data/observability/pids"

for name in loki promtail grafana; do
    pidfile="$PID_DIR/${name}.pid"
    if [ -f "$pidfile" ]; then
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "$name stopped (PID $pid)"
        else
            echo "$name not running (stale PID)"
        fi
        rm -f "$pidfile"
    else
        echo "$name not running"
    fi
done
