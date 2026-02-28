#!/usr/bin/env python
"""Ingest recent Guardian articles and enrich any unenriched backlog.

Designed to run unattended via launchctl every 8 hours.
"""
import logging
import subprocess
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def main():
    # --- Ingest last 2 days (ClickHouse dedupes via ReplacingMergeTree) ---
    log.info("Starting ingestion...")
    try:
        from newschat.ingest.loader import ingest

        to_date = date.today()
        from_date = to_date - timedelta(days=2)
        result = ingest(from_date=from_date, to_date=to_date)
        log.info("Ingestion complete: %s", result)
    except Exception:
        log.exception("Ingestion failed")
        # Continue to enrichment even if ingestion fails —
        # there may be unenriched articles from previous runs

    # --- Enrich unenriched articles ---
    log.info("Starting enrichment coordinator...")
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "scripts/enrich_coordinator.py",
                "--model", "groq:qwen/qwen3-32b",
                "--batch", "500",
                "--workers", "1",
            ],
            cwd="/Users/simon/GitHub/nowthenews",
            timeout=7 * 3600,  # 7 hour timeout (leaves 1h buffer before next run)
            capture_output=False,
        )
        log.info("Enrichment coordinator exited with code %d", proc.returncode)
    except subprocess.TimeoutExpired:
        log.warning("Enrichment coordinator timed out after 7 hours")
    except Exception:
        log.exception("Enrichment coordinator failed")

    log.info("Done.")


if __name__ == "__main__":
    main()
