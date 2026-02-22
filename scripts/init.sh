#!/usr/bin/env bash
# Start ClickHouse (if not already running) and initialise the DB schema.
# Run this before ingest/enrich to ensure the runtime is ready.
#
# Usage:
#   ./scripts/init.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# ---------- find clickhouse binary ----------
# Prefer the project-local binary (installed via official script) over system PATH,
# since the Homebrew Cask version is broken on macOS (unsigned binary).
if [ -x "$PROJECT_DIR/bin/clickhouse" ]; then
    CLICKHOUSE="$PROJECT_DIR/bin/clickhouse"
elif command -v clickhouse &>/dev/null; then
    CLICKHOUSE="$(command -v clickhouse)"
else
    echo "Error: clickhouse binary not found."
    echo "Install it:  curl -fsSL https://clickhouse.com/install.sh | sh && mkdir -p bin && mv clickhouse bin/"
    exit 1
fi

# ---------- check macOS quarantine ----------
CH_REAL="$(realpath "$CLICKHOUSE" 2>/dev/null || echo "$CLICKHOUSE")"
if [[ "$OSTYPE" == darwin* ]] && xattr "$CH_REAL" 2>/dev/null | grep -q com.apple.quarantine; then
    echo "Error: clickhouse binary is quarantined by macOS and will not run."
    echo "Fix it with:  xattr -d com.apple.quarantine $CH_REAL"
    exit 1
fi

# ---------- start clickhouse if needed ----------
echo "=== ClickHouse ==="
if curl -sf http://localhost:8123/ping &>/dev/null; then
    echo "  Already running"
else
    echo "  Starting ClickHouse..."
    "$CLICKHOUSE" server >/dev/null 2>&1 &

    # Wait up to 15 seconds for readiness
    for i in $(seq 1 15); do
        if curl -sf http://localhost:8123/ping &>/dev/null; then
            break
        fi
        if [ "$i" -eq 15 ]; then
            echo "  Error: ClickHouse did not become ready within 15 seconds" >&2
            exit 1
        fi
        sleep 1
    done
    echo "  ClickHouse ready"
fi

# ---------- initialise schema ----------
echo ""
echo "=== Database schema ==="
python scripts/setup_db.py
echo "  Schema ready"

# ---------- done ----------
echo ""
echo "=== Ready ==="
echo "  You can now run ingest/enrich:"
echo "  python scripts/ingest_once.py --from-date 2024-01-01 --to-date 2024-01-31"
echo "  python scripts/enrich_once.py --limit 10"
