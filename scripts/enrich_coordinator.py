#!/usr/bin/env python3
"""Enrichment coordinator — run batches until all articles are enriched.

Handles crashes, Ollama restarts, and retries automatically.
Designed to run unattended in tmux/screen/nohup.

Usage:
    python scripts/enrich_coordinator.py              # 500-article batches
    python scripts/enrich_coordinator.py --batch 200  # smaller batches
    python scripts/enrich_coordinator.py --dry-run    # show what would happen
"""

import argparse
import shutil
import subprocess
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from newschat.config import (
    CLICKHOUSE_DATABASE,
    ENRICH_MODEL,
    LOG_BACKUP_COUNT,
    LOG_FILE,
    LOG_LEVEL,
    LOG_MAX_BYTES,
)
from newschat.db import get_client
from newschat.enrich.pipeline import enrich
from newschat.enrich.prompt import PROMPT_VERSION

_DB = CLICKHOUSE_DATABASE
log = logging.getLogger("enrich_coordinator")

MAX_CONSECUTIVE_FAILURES = 5
RETRY_DELAY_SECONDS = 30


def setup_logging():
    log_path = Path(LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(fmt)
    root.addHandler(stderr_handler)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


def remaining_count(model: str) -> int:
    ch = get_client()
    row = ch.query(f"""
        SELECT count() FROM {_DB}.articles a
        LEFT ANTI JOIN (
            SELECT article_id FROM {_DB}.article_enrichment
            WHERE model_used = %(model)s AND prompt_version = %(pv)s
        ) e ON a.article_id = e.article_id
    """, parameters={"model": model, "pv": PROMPT_VERSION}).result_rows
    ch.close()
    return row[0][0]


def ensure_ollama():
    ollama = shutil.which("ollama")
    if not ollama:
        raise RuntimeError("ollama not found on PATH")
    try:
        subprocess.run([ollama, "list"], capture_output=True, timeout=5, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        log.info("Ollama not running — starting it")
        subprocess.Popen(
            [ollama, "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(3)
        subprocess.run([ollama, "list"], capture_output=True, timeout=10, check=True)
        log.info("Ollama started")


def main():
    parser = argparse.ArgumentParser(description="Run enrichment in batches until done")
    parser.add_argument("--batch", type=int, default=500, help="Articles per batch (default: 500)")
    parser.add_argument("--model", default=None, help=f"Ollama model (default: {ENRICH_MODEL})")
    parser.add_argument("--workers", type=int, default=None, help="Concurrent threads (default: 8 for Groq, 1 for Ollama)")
    parser.add_argument("--dry-run", action="store_true", help="Show remaining count and exit")
    args = parser.parse_args()

    setup_logging()
    model = args.model or ENRICH_MODEL

    remaining = remaining_count(model)
    log.info("Starting coordinator: %d remaining, model=%s, batch=%d", remaining, model, args.batch)

    if args.dry_run:
        batches = (remaining + args.batch - 1) // args.batch
        secs_per_article = 4 if model.startswith("groq:") else 25
        est_hours = remaining * secs_per_article / 3600
        log.info("Dry run: %d batches, estimated %.1f hours at ~%ds/article", batches, est_hours, secs_per_article)
        return

    batch_num = 0
    total_enriched = 0
    total_failed = 0
    consecutive_failures = 0
    start_time = time.time()

    while remaining > 0:
        batch_num += 1
        batch_size = min(args.batch, remaining)
        log.info("Batch %d: %d articles (%d remaining)", batch_num, batch_size, remaining)

        try:
            if not model.startswith("groq:"):
                ensure_ollama()
            result = enrich(model=model, limit=batch_size, workers=args.workers)

            total_enriched += result["enriched"]
            total_failed += result["failed"]
            consecutive_failures = 0

            elapsed = time.time() - start_time
            rate = total_enriched / elapsed * 3600 if elapsed > 0 else 0

            log.info(
                "Batch %d done: %d enriched, %d failed | running total: %d enriched, %d failed | %.0f articles/hour",
                batch_num, result["enriched"], result["failed"],
                total_enriched, total_failed, rate,
            )

            if result["enriched"] == 0 and result["failed"] == 0:
                log.info("Nothing to process — done")
                break

        except KeyboardInterrupt:
            log.info("Interrupted by user")
            break

        except Exception:
            consecutive_failures += 1
            log.exception("Batch %d crashed (consecutive failures: %d/%d)",
                          batch_num, consecutive_failures, MAX_CONSECUTIVE_FAILURES)

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error("%d consecutive failures — giving up", MAX_CONSECUTIVE_FAILURES)
                break

            log.info("Retrying in %ds", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)

        remaining = remaining_count(model)

    elapsed = time.time() - start_time
    remaining = remaining_count(model)
    log.info(
        "Coordinator finished: %d enriched, %d failed, %.1f hours elapsed, %d remaining",
        total_enriched, total_failed, elapsed / 3600, remaining,
    )


if __name__ == "__main__":
    main()
