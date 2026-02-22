#!/usr/bin/env python3
"""One-shot ingestion — fetch articles for a date range and insert into ClickHouse."""

import argparse
import logging
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path

from newschat.config import LOG_BACKUP_COUNT, LOG_FILE, LOG_LEVEL, LOG_MAX_BYTES
from newschat.db import init_schema
from newschat.ingest.loader import ingest


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
        description="Ingest Guardian articles for a date range"
    )
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--section", help="Guardian section filter (e.g. 'politics')"
    )
    parser.add_argument("--query", help="Keyword query filter")
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Create database/tables before ingesting",
    )
    args = parser.parse_args()

    setup_logging()

    from_d = date.fromisoformat(args.from_date)
    to_d = date.fromisoformat(args.to_date)

    if args.init_db:
        logging.info("Initialising ClickHouse schema...")
        init_schema()

    result = ingest(
        from_date=from_d, to_date=to_d, section=args.section, query=args.query
    )
    print(
        f"Done: {result['articles_new']} new articles ingested "
        f"({result['articles_fetched']} fetched, {result['status']})"
    )


if __name__ == "__main__":
    main()
