#!/usr/bin/env bash
# Install Loki, Promtail, and Grafana as native binaries (no Docker).
# Idempotent — skips anything already installed.
#
# Usage:
#   ./scripts/setup_observability.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${PROJECT_DIR}/data/observability"
BIN_DIR="${PROJECT_DIR}/bin"
CONF_DIR="${PROJECT_DIR}/observability"

LOKI_VERSION="3.4.2"
PROMTAIL_VERSION="3.4.2"
GRAFANA_VERSION="11.6.0"

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  ARCH_SUFFIX="amd64" ;;
    aarch64) ARCH_SUFFIX="arm64" ;;
    arm64)   ARCH_SUFFIX="arm64" ;;
    *)       echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"

mkdir -p "$BIN_DIR" "$DATA_DIR"/{loki,promtail,grafana}

# ---------- helper ----------
download_and_extract() {
    local name="$1" url="$2" dest="$3"
    if [ -x "$dest" ]; then
        echo "  $name already installed"
        return
    fi
    echo "  Downloading $name..."
    local tmp
    tmp="$(mktemp -d)"
    if [[ "$url" == *.zip ]]; then
        curl -fsSL "$url" -o "$tmp/archive.zip"
        unzip -q "$tmp/archive.zip" -d "$tmp"
        mv "$tmp/${name}-${OS}-${ARCH_SUFFIX}" "$dest" 2>/dev/null || mv "$tmp/$name" "$dest"
    else
        curl -fsSL "$url" -o "$tmp/archive.gz"
        gunzip "$tmp/archive.gz"
        mv "$tmp/archive" "$dest"
    fi
    chmod +x "$dest"
    rm -rf "$tmp"
}

# ---------- Loki ----------
echo "=== Loki ==="
LOKI_URL="https://github.com/grafana/loki/releases/download/v${LOKI_VERSION}/loki-${OS}-${ARCH_SUFFIX}.zip"
download_and_extract "loki" "$LOKI_URL" "$BIN_DIR/loki"

# ---------- Promtail ----------
echo "=== Promtail ==="
PROMTAIL_URL="https://github.com/grafana/loki/releases/download/v${PROMTAIL_VERSION}/promtail-${OS}-${ARCH_SUFFIX}.zip"
download_and_extract "promtail" "$PROMTAIL_URL" "$BIN_DIR/promtail"

# ---------- Grafana ----------
echo "=== Grafana ==="
GRAFANA_TAR="grafana-${GRAFANA_VERSION}.${OS}-${ARCH_SUFFIX}.tar.gz"
GRAFANA_URL="https://dl.grafana.com/oss/release/${GRAFANA_TAR}"
GRAFANA_DIR="${DATA_DIR}/grafana/install"
if [ -x "$GRAFANA_DIR/bin/grafana-server" ]; then
    echo "  Grafana already installed"
else
    echo "  Downloading Grafana..."
    tmp="$(mktemp -d)"
    curl -fsSL "$GRAFANA_URL" -o "$tmp/grafana.tar.gz"
    tar xzf "$tmp/grafana.tar.gz" -C "$tmp"
    rm -rf "$GRAFANA_DIR"
    mv "$tmp"/grafana-v*/ "$GRAFANA_DIR" 2>/dev/null || mv "$tmp"/grafana-*/ "$GRAFANA_DIR"
    rm -rf "$tmp"
fi

echo ""
echo "Binaries ready in ${BIN_DIR}/"
echo "Start with: ./scripts/start_observability.sh"
