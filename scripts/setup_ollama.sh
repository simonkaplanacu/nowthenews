#!/usr/bin/env bash
# Install Ollama (if not present) and pull the default enrichment model.
# Additional models can be passed as arguments for experiment comparisons.
#
# Usage:
#   ./scripts/setup_ollama.sh                    # pulls default model only
#   ./scripts/setup_ollama.sh llama3:8b gemma2   # pulls default + extras
set -euo pipefail

DEFAULT_MODEL="${ENRICH_MODEL:-qwen3:30b-a3b}"

# ---------- install ollama ----------
if command -v ollama &>/dev/null; then
    echo "✓ Ollama already installed: $(ollama --version)"
else
    echo "Installing Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
    echo "✓ Ollama installed"
fi

# ---------- ensure ollama is running ----------
if ! ollama list &>/dev/null 2>&1; then
    echo "Starting Ollama service..."
    ollama serve &>/dev/null &
    sleep 3
fi

# ---------- pull models ----------
pull_model() {
    local model="$1"
    echo "Pulling ${model}..."
    ollama pull "${model}"
    echo "✓ ${model} ready"
}

pull_model "${DEFAULT_MODEL}"

for extra in "$@"; do
    pull_model "${extra}"
done

echo ""
echo "Available models:"
ollama list
