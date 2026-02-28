#!/usr/bin/env python
"""Backfill liveblog blocks for historical liveblog articles.

Fetches individual liveblogs from the Guardian API (with show-blocks=body)
and stores their blocks in the liveblog_blocks table. Respects rate limits
and daily API budget. Safe to run repeatedly — skips already-backfilled articles.

Usage:
    python scripts/backfill_liveblog_blocks.py [--budget 200] [--dry-run]
"""
import argparse
import logging
import sys

from newschat.config import CLICKHOUSE_DATABASE
from newschat.db import get_client
from newschat.ingest.guardian import GuardianClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_DB = CLICKHOUSE_DATABASE


def _articles_needing_blocks(ch) -> list[str]:
    """Get liveblog article_ids that don't have any blocks yet."""
    result = ch.query(f"""
        SELECT a.article_id
        FROM {_DB}.articles a FINAL
        WHERE a.guardian_type = 'liveblog'
          AND a.article_id NOT IN (
              SELECT DISTINCT article_id FROM {_DB}.liveblog_blocks FINAL
          )
        ORDER BY a.published_at DESC
    """)
    return [r[0] for r in result.result_rows]


def _insert_blocks(ch, blocks):
    """Insert blocks into ClickHouse."""
    if not blocks:
        return
    rows = [
        [b.article_id, b.block_id, b.title, b.body_text, b.published_at]
        for b in blocks
    ]
    ch.insert(
        f"{_DB}.liveblog_blocks",
        rows,
        column_names=["article_id", "block_id", "title", "body_text", "published_at"],
    )


def main():
    parser = argparse.ArgumentParser(description="Backfill liveblog blocks")
    parser.add_argument(
        "--budget", type=int, default=200,
        help="Max API requests per run (default 200, leaves room for regular ingest)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Just show how many need backfilling, don't fetch",
    )
    args = parser.parse_args()

    ch = get_client()
    try:
        pending = _articles_needing_blocks(ch)
        log.info("%d liveblogs need block backfill", len(pending))

        if args.dry_run:
            for aid in pending[:20]:
                log.info("  %s", aid)
            if len(pending) > 20:
                log.info("  ... and %d more", len(pending) - 20)
            return

        guardian = GuardianClient()
        try:
            fetched = 0
            total_blocks = 0

            for article_id in pending:
                if fetched >= args.budget:
                    log.info(
                        "Budget exhausted (%d requests). %d articles remain. "
                        "Run again to continue.",
                        args.budget, len(pending) - fetched,
                    )
                    break

                if guardian.daily_requests_remaining <= 0:
                    log.warning("Daily API limit reached. Run again tomorrow.")
                    break

                try:
                    _, blocks = guardian.get_article(
                        article_id, include_blocks=True,
                    )
                    fetched += 1

                    if blocks:
                        _insert_blocks(ch, blocks)
                        total_blocks += len(blocks)
                        log.info(
                            "[%d/%d] %s: %d blocks",
                            fetched, min(args.budget, len(pending)),
                            article_id, len(blocks),
                        )
                    else:
                        log.info(
                            "[%d/%d] %s: no blocks (not a liveblog or empty)",
                            fetched, min(args.budget, len(pending)),
                            article_id,
                        )
                except Exception:
                    log.exception("Failed to fetch %s, skipping", article_id)
                    fetched += 1  # Still counts against budget

            log.info(
                "Done: %d articles fetched, %d blocks stored. "
                "%d articles remaining.",
                fetched, total_blocks, max(0, len(pending) - fetched),
            )
        finally:
            guardian.close()
    finally:
        ch.close()


if __name__ == "__main__":
    main()
