#!/usr/bin/env bash
# Full bootstrap: Python deps, ClickHouse schema, Ollama + models, observability stack.
#
# Usage:
#   ./scripts/setup_all.sh                       # default setup
#   ./scripts/setup_all.sh --models llama3:8b     # also pull extra models
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# ---------- parse args ----------
EXTRA_MODELS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --models)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                EXTRA_MODELS+=("$1")
                shift
            done
            ;;
        *)
            echo "Unknown arg: $1" >&2
            exit 1
            ;;
    esac
done

# ---------- python dependencies ----------
echo "=== Python dependencies ==="
if [ -d ".venv" ]; then
    echo "✓ Virtual environment exists"
else
    python3 -m venv .venv
    echo "✓ Created virtual environment"
fi
source .venv/bin/activate
pip install -q -e ".[dev]"
echo "✓ Python packages installed"

# ---------- clickhouse schema ----------
echo ""
echo "=== ClickHouse schema ==="
python scripts/setup_db.py
echo "✓ Database schema ready"

# ---------- ollama + models ----------
echo ""
echo "=== Ollama + models ==="
bash scripts/setup_ollama.sh "${EXTRA_MODELS[@]}"

# ---------- observability (docker) ----------
echo ""
echo "=== Observability stack ==="
if command -v docker &>/dev/null && command -v docker-compose &>/dev/null || docker compose version &>/dev/null 2>&1; then
    docker compose -f docker-compose.observability.yml up -d
    echo "✓ Loki + Grafana + Promtail running"
    echo "  Grafana: http://localhost:3000 (admin/admin)"
    echo "  Loki:    http://localhost:3100"
else
    echo "⚠ Docker not found — skipping observability stack."
    echo "  Install Docker and run: docker compose -f docker-compose.observability.yml up -d"
fi

echo ""
echo "=== Setup complete ==="
echo "  Ingest:  python scripts/ingest_once.py --from-date 2024-01-01 --to-date 2024-01-31"
echo "  Enrich:  python scripts/enrich_once.py --limit 10"
echo "  Logs:    http://localhost:3000 (Grafana → Explore → Loki)"
