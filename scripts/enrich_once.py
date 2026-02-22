#!/usr/bin/env python3
"""One-shot enrichment — process unenriched articles through the LLM pipeline."""

import argparse
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from newschat.config import (
    ENRICH_MODEL,
    LOG_BACKUP_COUNT,
    LOG_FILE,
    LOG_LEVEL,
    LOG_MAX_BYTES,
)
from newschat.enrich.pipeline import enrich


def setup_logging():
    """Configure logging to both stderr and a rotating log file."""
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


def main():
    parser = argparse.ArgumentParser(
        description="Enrich articles using an LLM via Ollama"
    )
    parser.add_argument(
        "--model",
        default=None,
        help=f"Ollama model to use (default: {ENRICH_MODEL})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max articles to process this run",
    )
    args = parser.parse_args()

    setup_logging()

    result = enrich(model=args.model, limit=args.limit)
    print(
        f"Done: {result['enriched']} enriched, {result['failed']} failed "
        f"(model={result['model']}, prompt={result['prompt_version']})"
    )


if __name__ == "__main__":
    main()
