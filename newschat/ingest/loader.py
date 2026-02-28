"""Article loader — fetch from sources, deduplicate, write to ClickHouse."""

import logging
from datetime import date, datetime, timedelta, timezone

from newschat.config import CLICKHOUSE_DATABASE, INGEST_BATCH_SIZE
from newschat.db import get_client as get_ch_client
from newschat.ingest.guardian import GuardianClient
from newschat.models import Article, LiveBlock, article_column_names, article_to_row

log = logging.getLogger(__name__)

_DB = CLICKHOUSE_DATABASE


def _check_connectivity(ch_client) -> None:
    """Verify ClickHouse is reachable before starting work."""
    ch_client.query("SELECT 1")


def _existing_ids(
    ch_client, source: str, from_date: date, to_date: date
) -> set[str]:
    """Get article IDs already in ClickHouse for the given date range."""
    from_dt = datetime.combine(from_date, datetime.min.time())
    # Use exclusive upper bound to avoid microsecond truncation issues
    to_dt = datetime.combine(to_date + timedelta(days=1), datetime.min.time())
    result = ch_client.query(
        f"SELECT article_id FROM {_DB}.articles WHERE source = %(source)s "
        "AND published_at >= %(from)s AND published_at < %(to)s",
        parameters={"source": source, "from": from_dt, "to": to_dt},
    )
    # Note: for very large date ranges this set could use significant memory.
    # ReplacingMergeTree provides a safety net — duplicate inserts are
    # eventually merged, so this is an optimisation, not the only defence.
    return {row[0] for row in result.result_rows}


def _existing_block_ids(ch_client, article_id: str) -> set[str]:
    """Get block IDs already stored for a liveblog article."""
    result = ch_client.query(
        f"SELECT block_id FROM {_DB}.liveblog_blocks FINAL "
        f"WHERE article_id = %(aid)s",
        parameters={"aid": article_id},
    )
    return {row[0] for row in result.result_rows}


def _insert_blocks(ch_client, blocks: list[LiveBlock]) -> None:
    """Insert liveblog blocks into ClickHouse."""
    if not blocks:
        return
    rows = [
        [b.article_id, b.block_id, b.title, b.body_text, b.published_at]
        for b in blocks
    ]
    ch_client.insert(
        f"{_DB}.liveblog_blocks",
        rows,
        column_names=["article_id", "block_id", "title", "body_text", "published_at"],
    )


def _insert_articles(ch_client, articles: list[Article]) -> None:
    """Insert articles and their tags into ClickHouse."""
    if not articles:
        return

    article_rows = [article_to_row(a) for a in articles]
    ch_client.insert(
        f"{_DB}.articles", article_rows, column_names=article_column_names()
    )

    tag_rows = []
    for a in articles:
        for t in a.tags:
            tag_rows.append(
                [a.article_id, t["tag_id"], t["tag_title"], t["tag_type"]]
            )
    if tag_rows:
        ch_client.insert(
            f"{_DB}.article_tags",
            tag_rows,
            column_names=["article_id", "tag_id", "tag_title", "tag_type"],
        )


def _log_ingestion(
    ch_client,
    source: str,
    from_date: date,
    to_date: date,
    fetched: int,
    new: int,
    pages: int,
    status: str,
):
    """Write a row to the ingestion log. Swallows exceptions to avoid masking
    the original error on the failure path."""
    try:
        ch_client.insert(
            f"{_DB}.ingestion_log",
            [
                [
                    source,
                    datetime.now(timezone.utc),
                    from_date,
                    to_date,
                    fetched,
                    new,
                    pages,
                    status,
                ]
            ],
            column_names=[
                "source",
                "run_at",
                "from_date",
                "to_date",
                "articles_fetched",
                "articles_new",
                "pages_fetched",
                "status",
            ],
        )
    except Exception:
        log.exception("Failed to write ingestion log entry")


def ingest(
    from_date: date,
    to_date: date,
    section: str | None = None,
    query: str | None = None,
) -> dict:
    """Ingest articles from the Guardian API into ClickHouse.

    Returns a summary dict with counts.

    Raises:
        ValueError: If from_date > to_date.
    """
    if from_date > to_date:
        raise ValueError(
            f"from_date ({from_date}) must be <= to_date ({to_date})"
        )

    ch = get_ch_client()
    guardian = GuardianClient()

    # Initialise counters before try block so they are always bound
    fetched = 0
    new_count = 0
    pages = 0
    status = "ok"
    new_blocks_by_article: dict[str, list[LiveBlock]] = {}

    try:
        _check_connectivity(ch)

        existing = _existing_ids(ch, "guardian", from_date, to_date)
        log.info(
            "Found %d existing articles for %s to %s",
            len(existing),
            from_date,
            to_date,
        )

        batch: list[Article] = []

        for article, blocks in guardian.fetch_all(
            from_date=from_date,
            to_date=to_date,
            section=section,
            query=query,
            order_by="oldest",
        ):
            fetched += 1
            if article.article_id not in existing:
                batch.append(article)
                new_count += 1

            # Process liveblog blocks — detect new ones
            if blocks:
                stored_block_ids = _existing_block_ids(ch, article.article_id)
                new_blocks = [b for b in blocks if b.block_id not in stored_block_ids]
                if new_blocks:
                    _insert_blocks(ch, new_blocks)
                    new_blocks_by_article[article.article_id] = new_blocks
                    log.info(
                        "Liveblog %s: %d new blocks (of %d total)",
                        article.article_id, len(new_blocks), len(blocks),
                    )

            if len(batch) >= INGEST_BATCH_SIZE:
                _insert_articles(ch, batch)
                existing.update(a.article_id for a in batch)
                log.info(
                    "Inserted batch of %d articles (total new: %d)",
                    len(batch),
                    new_count,
                )
                batch = []

        # Final batch
        if batch:
            _insert_articles(ch, batch)
            existing.update(a.article_id for a in batch)

        pages = guardian.pages_fetched

    except Exception:
        log.exception("Ingestion failed")
        status = "error"
        _log_ingestion(
            ch, "guardian", from_date, to_date, fetched, new_count, pages, status
        )
        raise
    else:
        _log_ingestion(
            ch, "guardian", from_date, to_date, fetched, new_count, pages, status
        )
    finally:
        guardian.close()
        ch.close()

    total_new_blocks = sum(len(bs) for bs in new_blocks_by_article.values())
    summary = {
        "source": "guardian",
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "articles_fetched": fetched,
        "articles_new": new_count,
        "pages_fetched": pages,
        "status": status,
        "liveblogs_updated": len(new_blocks_by_article),
        "new_blocks": total_new_blocks,
        "new_blocks_by_article": {
            aid: [(b.title, b.block_id) for b in bs]
            for aid, bs in new_blocks_by_article.items()
        },
    }
    log.info("Ingestion complete: %s articles_new=%d, new_blocks=%d",
             status, new_count, total_new_blocks)
    return summary
