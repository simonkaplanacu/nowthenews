"""ClickHouse connection and query helpers."""

import clickhouse_connect

from newschat.config import CLICKHOUSE_DSN

# Schema DDL — run these to set up the news database
SCHEMA_DDL = [
    "CREATE DATABASE IF NOT EXISTS news",

    """CREATE TABLE IF NOT EXISTS news.articles (
    id              String,
    source          LowCardinality(String),
    url             String,
    title           String,
    headline        String,
    body_text       String,
    byline          String,
    section_id      LowCardinality(String),
    section_name    String,
    pillar          LowCardinality(String),
    published_at    DateTime,
    word_count      UInt32,
    lang            LowCardinality(String),
    short_url       String,
    thumbnail_url   String,
    ingested_at     DateTime DEFAULT now(),
    enriched        UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (source, published_at, id)""",

    """CREATE TABLE IF NOT EXISTS news.article_tags (
    article_id      String,
    tag_id          String,
    tag_title       String,
    tag_type        LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (article_id, tag_id)""",

    """CREATE TABLE IF NOT EXISTS news.article_enrichment (
    article_id          String,
    enriched_at         DateTime DEFAULT now(),
    entities            Array(String),
    entity_types        Array(String),
    policy_domains      Array(String),
    policy_scores       Array(Float32),
    sentiment           LowCardinality(String),
    sentiment_score     Float32,
    framing_notes       String,
    summary             String,
    model_used          String,
    prompt_version      String
) ENGINE = MergeTree()
ORDER BY (article_id, enriched_at)""",

    """CREATE TABLE IF NOT EXISTS news.ingestion_log (
    source          LowCardinality(String),
    run_at          DateTime DEFAULT now(),
    from_date       Date,
    to_date         Date,
    articles_fetched UInt32,
    articles_new    UInt32,
    pages_fetched   UInt32,
    status          LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (source, run_at)""",
]


def get_client():
    """Create a ClickHouse client from the configured DSN."""
    return clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)


def init_schema(client=None):
    """Create the news database and all tables."""
    close = False
    if client is None:
        client = get_client()
        close = True
    try:
        for ddl in SCHEMA_DDL:
            client.command(ddl)
    finally:
        if close:
            client.close()
