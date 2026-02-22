#!/usr/bin/env python3
"""One-shot ingestion — fetch articles for a date range and insert into ClickHouse."""

import argparse
import logging
from datetime import date

from newschat.db import init_schema
from newschat.ingest.coordinator import ingest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="Ingest Guardian articles for a date range")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--section", help="Guardian section filter (e.g. 'politics')")
    parser.add_argument("--query", help="Keyword query filter")
    parser.add_argument("--init-db", action="store_true", help="Create database/tables before ingesting")
    args = parser.parse_args()

    from_d = date.fromisoformat(args.from_date)
    to_d = date.fromisoformat(args.to_date)

    if args.init_db:
        logging.info("Initialising ClickHouse schema...")
        init_schema()

    result = ingest(from_date=from_d, to_date=to_d, section=args.section, query=args.query)
    print(f"Done: {result['articles_new']} new articles ingested "
          f"({result['articles_fetched']} fetched, {result['status']})")


if __name__ == "__main__":
    main()
