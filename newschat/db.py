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
    guardian_type    LowCardinality(String) DEFAULT '',
    production_office LowCardinality(String) DEFAULT '',
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
    content_type        LowCardinality(String) DEFAULT '',
    model_used          LowCardinality(String),
    prompt_version      LowCardinality(String)
) ENGINE = ReplacingMergeTree(enriched_at)
ORDER BY (article_id, model_used, prompt_version)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.article_regions (
    article_id      String,
    region          LowCardinality(String),
    score           Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (article_id, region)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.article_topics (
    article_id      String,
    topic           LowCardinality(String)
) ENGINE = ReplacingMergeTree()
ORDER BY (article_id, topic)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.enrichment_log (
    model               LowCardinality(String),
    prompt_version      LowCardinality(String),
    run_at              DateTime('UTC') DEFAULT now(),
    articles_attempted  UInt32,
    articles_enriched   UInt32,
    articles_failed     UInt32,
    status              LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (model, run_at)""",
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
    f"""CREATE TABLE IF NOT EXISTS {_DB}.alerts (
    alert_id        UUID DEFAULT generateUUIDv4(),
    alert_type      LowCardinality(String),
    severity        LowCardinality(String),
    message         String,
    context         String DEFAULT '',
    created_at      DateTime('UTC') DEFAULT now(),
    acknowledged    UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (created_at, alert_id)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.saved_searches (
    search_id       UUID DEFAULT generateUUIDv4(),
    label           String,
    query           String,
    email           String DEFAULT '',
    active          UInt8 DEFAULT 1,
    created_at      DateTime('UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (search_id)""",
    f"""CREATE TABLE IF NOT EXISTS {_DB}.search_matches (
    match_id        UUID DEFAULT generateUUIDv4(),
    search_id       UUID,
    article_id      String,
    matched_at      DateTime('UTC') DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (matched_at, search_id, article_id)""",
]


def get_client():
    """Create a ClickHouse client from the configured DSN.

    The DSN should NOT include a database name — all queries use
    fully qualified table names (e.g. news.articles) so the client
    works regardless of its default database.
    """
    return clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)


_MIGRATIONS = [
    f"ALTER TABLE {_DB}.articles ADD COLUMN IF NOT EXISTS guardian_type LowCardinality(String) DEFAULT ''",
    f"ALTER TABLE {_DB}.articles ADD COLUMN IF NOT EXISTS production_office LowCardinality(String) DEFAULT ''",
    f"ALTER TABLE {_DB}.article_enrichment ADD COLUMN IF NOT EXISTS content_type LowCardinality(String) DEFAULT ''",
]


def write_alert(
    alert_type: str,
    severity: str,
    message: str,
    context: str = "",
):
    """Write an alert to the alerts table.

    Args:
        alert_type: enrichment_crash, api_limit, ingestion_failure, stale_db, search_match
        severity: info, warning, critical
        message: Human-readable alert message
        context: JSON string with additional details
    """
    client = get_client()
    try:
        client.command(
            f"INSERT INTO {_DB}.alerts (alert_type, severity, message, context) "
            f"VALUES (%(type)s, %(severity)s, %(message)s, %(context)s)",
            parameters={
                "type": alert_type,
                "severity": severity,
                "message": message,
                "context": context,
            },
        )
    finally:
        client.close()


def init_schema():
    """Create the database and all tables, then apply column migrations.

    Safe to run on a fresh install because the client connects
    without specifying a database.  Also safe to re-run — CREATE IF NOT
    EXISTS and ADD COLUMN IF NOT EXISTS are idempotent.
    """
    client = get_client()
    try:
        for ddl in SCHEMA_DDL:
            client.command(ddl)
        for ddl in _MIGRATIONS:
            client.command(ddl)
    finally:
        client.close()
