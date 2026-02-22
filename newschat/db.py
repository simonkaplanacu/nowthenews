"""ClickHouse connection and schema management."""

import clickhouse_connect

from newschat.config import CLICKHOUSE_DATABASE, CLICKHOUSE_DSN

_DB = CLICKHOUSE_DATABASE

SCHEMA_DDL = [
    f"CREATE DATABASE IF NOT EXISTS {_DB}",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.articles (
    article_id      String,
    source          LowCardinality(String),
    url             String,
    title           String,
    headline        String,
    standfirst      String,
    body_text       String,
    byline          String,
    section_id      LowCardinality(String),
    section_name    String,
    pillar          LowCardinality(String),
    published_at    DateTime('UTC'),
    word_count      UInt32,
    lang            LowCardinality(String),
    short_url       String,
    thumbnail_url   String,
    ingested_at     DateTime('UTC') DEFAULT now(),
    INDEX idx_article_id article_id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, published_at, article_id)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.article_tags (
    article_id      String,
    tag_id          String,
    tag_title       String,
    tag_type        LowCardinality(String)
) ENGINE = ReplacingMergeTree()
ORDER BY (article_id, tag_id)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.article_enrichment (
    article_id          String,
    enriched_at         DateTime('UTC') DEFAULT now(),
    entities            Nested(name String, type String),
    policy              Nested(domain String, score Float32),
    sentiment           LowCardinality(String),
    sentiment_score     Float32,
    framing_notes       String,
    smoke_terms         Nested(term String, context String, rationale String),
    quotes              Nested(quote String, speaker String, context String),
    event_signature     String,
    event_date          Nullable(Date),
    summary             String,
    model_used          LowCardinality(String),
    prompt_version      LowCardinality(String)
) ENGINE = ReplacingMergeTree(enriched_at)
ORDER BY (article_id, enriched_at)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.ingestion_log (
    source          LowCardinality(String),
    run_at          DateTime('UTC') DEFAULT now(),
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
    """Create a ClickHouse client from the configured DSN.

    The DSN should NOT include a database name — all queries use
    fully qualified table names (e.g. news.articles) so the client
    works regardless of its default database.
    """
    return clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)


def init_schema():
    """Create the database and all tables.

    Safe to run on a fresh install because the client connects
    without specifying a database.
    """
    client = get_client()
    try:
        for ddl in SCHEMA_DDL:
            client.command(ddl)
    finally:
        client.close()
