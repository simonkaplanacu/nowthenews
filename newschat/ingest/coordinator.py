"""Ingestion coordinator — fetch from Guardian, dedup, write to ClickHouse."""

import logging
from datetime import date, datetime

from newschat.ingest.guardian import GuardianClient, Article
from newschat.db import get_client as get_ch_client

log = logging.getLogger(__name__)


def _existing_ids(ch_client, source: str, from_date: date, to_date: date) -> set[str]:
    """Get article IDs already in ClickHouse for the given date range."""
    result = ch_client.query(
        "SELECT id FROM news.articles WHERE source = %(source)s "
        "AND published_at >= %(from)s AND published_at <= %(to)s",
        parameters={
            "source": source,
            "from": datetime.combine(from_date, datetime.min.time()),
            "to": datetime.combine(to_date, datetime.max.time()),
        },
    )
    return {row[0] for row in result.result_rows}


def _insert_articles(ch_client, articles: list[Article]):
    """Insert articles and their tags into ClickHouse."""
    if not articles:
        return

    # Insert articles
    article_rows = [
        [
            a.id, a.source, a.url, a.title, a.headline, a.body_text,
            a.byline, a.section_id, a.section_name, a.pillar,
            a.published_at, a.word_count, a.lang, a.short_url,
            a.thumbnail_url,
        ]
        for a in articles
    ]
    ch_client.insert(
        "news.articles",
        article_rows,
        column_names=[
            "id", "source", "url", "title", "headline", "body_text",
            "byline", "section_id", "section_name", "pillar",
            "published_at", "word_count", "lang", "short_url",
            "thumbnail_url",
        ],
    )

    # Insert tags
    tag_rows = []
    for a in articles:
        for t in a.tags:
            tag_rows.append([a.id, t["tag_id"], t["tag_title"], t["tag_type"]])
    if tag_rows:
        ch_client.insert(
            "news.article_tags",
            tag_rows,
            column_names=["article_id", "tag_id", "tag_title", "tag_type"],
        )


def _log_ingestion(ch_client, source: str, from_date: date, to_date: date,
                   fetched: int, new: int, pages: int, status: str):
    """Write a row to the ingestion log."""
    ch_client.insert(
        "news.ingestion_log",
        [[source, datetime.utcnow(), from_date, to_date, fetched, new, pages, status]],
        column_names=[
            "source", "run_at", "from_date", "to_date",
            "articles_fetched", "articles_new", "pages_fetched", "status",
        ],
    )


def ingest(
    from_date: date,
    to_date: date,
    section: str | None = None,
    query: str | None = None,
) -> dict:
    """
    Ingest articles from the Guardian API into ClickHouse.

    Returns a summary dict with counts.
    """
    ch = get_ch_client()
    guardian = GuardianClient()

    try:
        existing = _existing_ids(ch, "guardian", from_date, to_date)
        log.info(
            "Found %d existing articles for %s to %s",
            len(existing), from_date, to_date,
        )

        fetched = 0
        new_articles: list[Article] = []
        pages = 0
        batch: list[Article] = []
        batch_size = 200

        for article in guardian.fetch_all(
            from_date=from_date,
            to_date=to_date,
            section=section,
            query=query,
            order_by="oldest",
        ):
            fetched += 1
            if article.id not in existing:
                batch.append(article)
                new_articles.append(article)

            # Write in batches
            if len(batch) >= batch_size:
                _insert_articles(ch, batch)
                log.info("Inserted batch of %d articles (total new: %d)", len(batch), len(new_articles))
                batch = []

            if fetched % 200 == 0:
                pages += 1

        # Final batch
        if batch:
            _insert_articles(ch, batch)

        pages = max(pages, 1) if fetched > 0 else 0
        status = "ok"

    except Exception as e:
        log.error("Ingestion failed: %s", e)
        status = "error"
        _log_ingestion(ch, "guardian", from_date, to_date, fetched, len(new_articles), pages, status)
        raise
    else:
        _log_ingestion(ch, "guardian", from_date, to_date, fetched, len(new_articles), pages, status)
    finally:
        guardian.close()
        ch.close()

    summary = {
        "source": "guardian",
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "articles_fetched": fetched,
        "articles_new": len(new_articles),
        "pages_fetched": pages,
        "status": status,
    }
    log.info("Ingestion complete: %s", summary)
    return summary
