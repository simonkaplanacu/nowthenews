"""Configuration — loads config.json for operational parameters, .env for secrets."""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env for secrets (API keys, DSNs). This is the config module —
# the one place where environment setup belongs.
load_dotenv()

_CONFIG_PATH = Path(__file__).parent.parent / "config.json"


def _load_json_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH) as f:
            return json.load(f)
    return {}


_cfg = _load_json_config()

# --- Secrets (environment only, never in config.json) ---
GUARDIAN_API_KEY = os.environ.get("GUARDIAN_API_KEY", "")

# --- Guardian API ---
_guardian = _cfg.get("guardian", {})
GUARDIAN_BASE_URL = _guardian.get("base_url", "https://content.guardianapis.com")
GUARDIAN_RATE_LIMIT_INTERVAL = _guardian.get("rate_limit_interval_seconds", 1.0)
GUARDIAN_DAILY_LIMIT = _guardian.get("daily_request_limit", 500)
GUARDIAN_SHOW_FIELDS = _guardian.get(
    "show_fields",
    "headline,standfirst,body,byline,wordcount,thumbnail,shortUrl,lang",
)
GUARDIAN_SHOW_TAGS = _guardian.get("show_tags", "keyword")
GUARDIAN_PAGE_SIZE = _guardian.get("page_size", 200)
GUARDIAN_TIMEOUT = _guardian.get("timeout_seconds", 30)
GUARDIAN_MAX_RETRIES = _guardian.get("max_retries", 3)

# --- ClickHouse ---
_clickhouse = _cfg.get("clickhouse", {})
CLICKHOUSE_DSN = os.environ.get(
    "CLICKHOUSE_DSN",
    _clickhouse.get("dsn", "clickhouse://localhost:9000"),
)
CLICKHOUSE_DATABASE = _clickhouse.get("database", "news")

# --- Ollama ---
_ollama = _cfg.get("ollama", {})
OLLAMA_HOST = os.environ.get(
    "OLLAMA_HOST", _ollama.get("host", "http://localhost:11434")
)
OLLAMA_NUM_CTX = _ollama.get("num_ctx", 8192)

# --- Ingestion ---
_ingestion = _cfg.get("ingestion", {})
INGEST_BATCH_SIZE = _ingestion.get("batch_size", 200)

# --- Logging ---
_logging = _cfg.get("logging", {})
LOG_LEVEL = _logging.get("level", "INFO")
LOG_FILE = _logging.get("file", "logs/newschat.log")
LOG_MAX_BYTES = _logging.get("max_bytes", 10_485_760)
LOG_BACKUP_COUNT = _logging.get("backup_count", 5)
