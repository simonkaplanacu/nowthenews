"""MCP server exposing nowthenews data over Streamable HTTP.

Start with:
    MCP_AUTH_TOKEN=secret newschat-mcp
    MCP_AUTH_TOKEN=secret MCP_PORT=9000 python -m newschat.mcp.server

Future writes gated behind MCP_ENABLE_WRITES (not yet implemented).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal

from fastmcp import FastMCP

from newschat.config import CLICKHOUSE_DATABASE, MCP_AUTH_TOKEN
from newschat.db import get_client

_DB = CLICKHOUSE_DATABASE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _serialise(val):
    """Make a ClickHouse value JSON-friendly."""
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    return val


def _query(sql: str, parameters: dict | None = None) -> list[dict]:
    """Run a read query and return rows as list of dicts."""
    client = get_client()
    try:
        result = client.query(sql, parameters=parameters)
        cols = result.column_names
        return [
            {c: _serialise(v) for c, v in zip(cols, row)}
            for row in result.result_rows
        ]
    finally:
        client.close()


def _scalar(sql: str, parameters: dict | None = None):
    """Run a query returning a single scalar value."""
    client = get_client()
    try:
        return client.command(sql, parameters=parameters)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# FastMCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "nowthenews",
    instructions=(
        "Access to news articles enriched with entities, sentiment, "
        "policy domains, smoke terms, quotes, geographic relevance, topics, "
        "and content type classifications. Also manages system alerts and saved searches."
    ),
)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def search_articles(
    query: str,
    from_date: str | None = None,
    to_date: str | None = None,
    section: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Full-text search across article titles, headlines, and body text.

    Each word is matched independently (AND logic) — all words must appear
    somewhere in the article's title, headline, or body text.

    Args:
        query: Search words (each matched independently, case-insensitive)
        from_date: Filter by publish date >= this (YYYY-MM-DD)
        to_date: Filter by publish date <= this (YYYY-MM-DD)
        section: Filter by section_id (e.g. 'politics', 'world')
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    words = [w for w in query.split() if w]
    conditions = []
    params: dict = {"limit": limit}
    for i, word in enumerate(words):
        key = f"q{i}"
        params[key] = word
        conditions.append(
            f"(positionCaseInsensitive(title, {{{key}:String}}) > 0"
            f" OR positionCaseInsensitive(headline, {{{key}:String}}) > 0"
            f" OR positionCaseInsensitive(body_text, {{{key}:String}}) > 0)"
        )
    if from_date:
        conditions.append("published_at >= toDateTime({from_date:String})")
        params["from_date"] = from_date
    if to_date:
        conditions.append("published_at <= toDateTime({to_date:String})")
        params["to_date"] = to_date
    if section:
        conditions.append("section_id = {section:String}")
        params["section"] = section

    sql = f"""
        SELECT article_id, title, headline, standfirst, section_id,
               section_name, byline, published_at, word_count, url
        FROM {_DB}.articles FINAL
        WHERE {" AND ".join(conditions)}
        ORDER BY published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def get_article(article_id: str) -> dict | None:
    """Fetch a single article by its ID.

    Args:
        article_id: The article identifier
    """
    rows = _query(
        f"""
        SELECT article_id, source, url, title, headline, standfirst,
               body_text, byline, section_id, section_name, pillar,
               published_at, word_count, lang, short_url, thumbnail_url
        FROM {_DB}.articles FINAL
        WHERE article_id = {{article_id:String}}
        LIMIT 1
        """,
        {"article_id": article_id},
    )
    return rows[0] if rows else None


@mcp.tool()
def get_enrichment(article_id: str) -> dict | None:
    """Fetch enrichment data (entities, sentiment, smoke terms, quotes, content type) for an article.

    Args:
        article_id: The article identifier
    """
    rows = _query(
        f"""
        SELECT article_id, enriched_at, sentiment, sentiment_score,
               framing_notes, event_signature, event_date, summary,
               content_type, model_used, prompt_version,
               entities.name, entities.type,
               policy.domain, policy.score,
               smoke_terms.term, smoke_terms.context, smoke_terms.rationale,
               quotes.quote, quotes.speaker, quotes.context
        FROM {_DB}.article_enrichment FINAL
        WHERE article_id = {{article_id:String}}
        LIMIT 1
        """,
        {"article_id": article_id},
    )
    if not rows:
        return None

    row = rows[0]
    result = {
        k: row[k]
        for k in (
            "article_id", "enriched_at", "sentiment", "sentiment_score",
            "framing_notes", "event_signature", "event_date", "summary",
            "content_type", "model_used", "prompt_version",
        )
    }
    # Rebuild nested arrays from parallel sub-column arrays
    result["entities"] = [
        {"name": n, "type": t}
        for n, t in zip(row["entities.name"], row["entities.type"])
    ]
    result["policy_domains"] = [
        {"domain": d, "score": s}
        for d, s in zip(row["policy.domain"], row["policy.score"])
    ]
    result["smoke_terms"] = [
        {"term": t, "context": c, "rationale": r}
        for t, c, r in zip(
            row["smoke_terms.term"],
            row["smoke_terms.context"],
            row["smoke_terms.rationale"],
        )
    ]
    result["quotes"] = [
        {"quote": q, "speaker": s, "context": c}
        for q, s, c in zip(
            row["quotes.quote"],
            row["quotes.speaker"],
            row["quotes.context"],
        )
    ]
    # Fetch geographic relevance and topics from separate tables
    result["geographic_relevance"] = _query(
        f"""SELECT region, score FROM {_DB}.article_regions
        WHERE article_id = {{article_id:String}}
        ORDER BY score DESC""",
        {"article_id": article_id},
    )
    result["topics"] = [
        r["topic"] for r in _query(
            f"""SELECT topic FROM {_DB}.article_topics
            WHERE article_id = {{article_id:String}}""",
            {"article_id": article_id},
        )
    ]
    return result


@mcp.tool()
def search_by_entity(
    name: str,
    type: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Find articles mentioning a specific entity.

    Args:
        name: Entity name to search (case-insensitive substring)
        type: Filter by entity type (person, organisation, place, event, legislation, statistic)
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions = ["positionCaseInsensitive(e_name, {name:String}) > 0"]
    params: dict = {"name": name, "limit": limit}
    if type:
        conditions.append("e_type = {type:String}")
        params["type"] = type

    sql = f"""
        SELECT e.article_id, e_name, e_type,
               a.title, a.published_at, a.section_id
        FROM {_DB}.article_enrichment AS e FINAL
        ARRAY JOIN e.entities.name AS e_name, e.entities.type AS e_type
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = e.article_id
        WHERE {" AND ".join(conditions)}
        ORDER BY a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def top_entities(
    type: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Most frequently mentioned entities across all articles.

    Args:
        type: Filter by entity type (person, organisation, place, event, legislation, statistic)
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions: list[str] = []
    params: dict = {"limit": limit}
    if type:
        conditions.append("e_type = {type:String}")
        params["type"] = type

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT e_name AS name, e_type AS type,
               count() AS mention_count,
               countDistinct(article_id) AS article_count
        FROM {_DB}.article_enrichment FINAL
        ARRAY JOIN entities.name AS e_name, entities.type AS e_type
        {where}
        GROUP BY e_name, e_type
        ORDER BY mention_count DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def sentiment_breakdown(
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict]:
    """Count of articles by sentiment category, optionally filtered by date range.

    Args:
        from_date: Filter by publish date >= this (YYYY-MM-DD)
        to_date: Filter by publish date <= this (YYYY-MM-DD)
    """
    conditions: list[str] = []
    params: dict = {}
    join = ""

    if from_date or to_date:
        join = (
            f"JOIN {_DB}.articles AS a FINAL "
            "ON a.article_id = e.article_id"
        )
        if from_date:
            conditions.append(
                "a.published_at >= toDateTime({from_date:String})"
            )
            params["from_date"] = from_date
        if to_date:
            conditions.append(
                "a.published_at <= toDateTime({to_date:String})"
            )
            params["to_date"] = to_date

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT e.sentiment, count() AS article_count,
               avg(e.sentiment_score) AS avg_score
        FROM {_DB}.article_enrichment AS e FINAL
        {join}
        {where}
        GROUP BY e.sentiment
        ORDER BY article_count DESC
    """
    return _query(sql, params)


@mcp.tool()
def find_smoke_terms(
    query: str,
    limit: int = 20,
) -> list[dict]:
    """Search smoke terms (loaded/framing language) across all articles.

    Args:
        query: Search text to match against term, context, or rationale
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    sql = f"""
        SELECT e.article_id, st_term, st_context, st_rationale,
               a.title, a.published_at
        FROM {_DB}.article_enrichment AS e FINAL
        ARRAY JOIN e.smoke_terms.term AS st_term,
                   e.smoke_terms.context AS st_context,
                   e.smoke_terms.rationale AS st_rationale
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = e.article_id
        WHERE positionCaseInsensitive(st_term, {{query:String}}) > 0
           OR positionCaseInsensitive(st_context, {{query:String}}) > 0
           OR positionCaseInsensitive(st_rationale, {{query:String}}) > 0
        ORDER BY a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, {"query": query, "limit": limit})


@mcp.tool()
def top_smoke_terms(limit: int = 20) -> list[dict]:
    """Most frequently occurring smoke terms across all articles.

    Args:
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    sql = f"""
        SELECT st_term AS term, count() AS occurrence_count,
               countDistinct(article_id) AS article_count
        FROM {_DB}.article_enrichment FINAL
        ARRAY JOIN smoke_terms.term AS st_term
        GROUP BY st_term
        ORDER BY occurrence_count DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, {"limit": limit})


@mcp.tool()
def find_quotes(
    query: str | None = None,
    speaker: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Search quotes by speaker name or quote text.

    Args:
        query: Search text to match in quote text or context
        speaker: Search text to match against speaker name
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if query:
        conditions.append(
            "(positionCaseInsensitive(q_quote, {query:String}) > 0"
            " OR positionCaseInsensitive(q_context, {query:String}) > 0)"
        )
        params["query"] = query
    if speaker:
        conditions.append(
            "positionCaseInsensitive(q_speaker, {speaker:String}) > 0"
        )
        params["speaker"] = speaker

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT e.article_id, q_quote, q_speaker, q_context,
               a.title, a.published_at
        FROM {_DB}.article_enrichment AS e FINAL
        ARRAY JOIN e.quotes.quote AS q_quote,
                   e.quotes.speaker AS q_speaker,
                   e.quotes.context AS q_context
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = e.article_id
        {where}
        ORDER BY a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def search_by_region(
    region: str,
    min_score: float = 0.3,
    topic: str | None = None,
    content_type: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Find articles relevant to a geographic region.

    Args:
        region: Region code (australia, united_kingdom, united_states, europe,
                middle_east, asia_pacific, latin_america, africa, global)
        min_score: Minimum relevance score (0.0-1.0, default 0.3)
        topic: Optional topic filter (e.g. 'trade', 'domestic_politics')
        content_type: Optional content type filter (e.g. 'news_report', 'opinion')
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions = [
        "r.region = {region:String}",
        "r.score >= {min_score:Float32}",
    ]
    params: dict = {"region": region, "min_score": min_score, "limit": limit}
    joins = ""

    if topic:
        joins += (
            f" JOIN {_DB}.article_topics AS t"
            " ON t.article_id = r.article_id"
        )
        conditions.append("t.topic = {topic:String}")
        params["topic"] = topic

    if content_type:
        joins += (
            f" JOIN {_DB}.article_enrichment AS e FINAL"
            " ON e.article_id = r.article_id"
        )
        conditions.append("e.content_type = {content_type:String}")
        params["content_type"] = content_type

    sql = f"""
        SELECT r.article_id, r.region, r.score,
               a.title, a.published_at, a.section_id, a.url
        FROM {_DB}.article_regions AS r
        JOIN {_DB}.articles AS a FINAL ON a.article_id = r.article_id
        {joins}
        WHERE {" AND ".join(conditions)}
        ORDER BY r.score DESC, a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def search_by_topic(
    topic: str,
    region: str | None = None,
    content_type: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Find articles matching a topic label.

    Args:
        topic: Topic from controlled vocabulary (domestic_politics,
               international_relations, trade, defence_security, economy,
               business, immigration, law_justice, health, education,
               environment, technology, culture_arts, sport, social_issues,
               media, religion, science, human_interest, conflict_crisis)
        region: Optional region filter (min_score 0.3 applied)
        content_type: Optional content type filter
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions = ["t.topic = {topic:String}"]
    params: dict = {"topic": topic, "limit": limit}
    joins = ""

    if region:
        joins += (
            f" JOIN {_DB}.article_regions AS r"
            " ON r.article_id = t.article_id"
        )
        conditions.append("r.region = {region:String}")
        conditions.append("r.score >= 0.3")
        params["region"] = region

    if content_type:
        joins += (
            f" JOIN {_DB}.article_enrichment AS e FINAL"
            " ON e.article_id = t.article_id"
        )
        conditions.append("e.content_type = {content_type:String}")
        params["content_type"] = content_type

    sql = f"""
        SELECT t.article_id, t.topic,
               a.title, a.published_at, a.section_id, a.url
        FROM {_DB}.article_topics AS t
        JOIN {_DB}.articles AS a FINAL ON a.article_id = t.article_id
        {joins}
        WHERE {" AND ".join(conditions)}
        ORDER BY a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def browse_by_topic(region: str | None = None) -> list[dict]:
    """Count of articles grouped by topic, optionally filtered by region.

    Args:
        region: Optional region filter (min_score 0.3 applied)
    """
    conditions: list[str] = []
    params: dict = {}
    join = ""

    if region:
        join = (
            f"JOIN {_DB}.article_regions AS r "
            "ON r.article_id = t.article_id"
        )
        conditions.append("r.region = {region:String}")
        conditions.append("r.score >= 0.3")
        params["region"] = region

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT t.topic, count() AS article_count
        FROM {_DB}.article_topics AS t
        {join}
        {where}
        GROUP BY t.topic
        ORDER BY article_count DESC
    """
    return _query(sql, params)


@mcp.tool()
def browse_by_content_type() -> list[dict]:
    """Count of articles by content type."""
    sql = f"""
        SELECT content_type, count() AS article_count
        FROM {_DB}.article_enrichment FINAL
        WHERE content_type != ''
        GROUP BY content_type
        ORDER BY article_count DESC
    """
    return _query(sql)


@mcp.tool()
def browse_by_region() -> list[dict]:
    """Count of articles by geographic region (articles can appear in multiple regions)."""
    sql = f"""
        SELECT region, count() AS article_count,
               avg(score) AS avg_score
        FROM {_DB}.article_regions
        GROUP BY region
        ORDER BY article_count DESC
    """
    return _query(sql)


@mcp.tool()
def db_stats() -> dict:
    """Row counts for all tables in the database."""
    stats = {}
    # ReplacingMergeTree tables — use FINAL for accurate deduped counts
    for table in ("articles", "article_tags", "article_enrichment",
                  "article_regions", "article_topics"):
        stats[table] = _scalar(f"SELECT count() FROM {_DB}.{table} FINAL")
    # Plain MergeTree tables
    for table in ("enrichment_log", "ingestion_log"):
        stats[table] = _scalar(f"SELECT count() FROM {_DB}.{table}")
    return stats


@mcp.tool()
def benchmark_results() -> dict:
    """Compare enrichment quality across models vs the qwen3:30b-a3b baseline.

    Returns a comparison of each candidate model's performance on a fixed
    50-article sample, including entity/topic/region overlap, sentiment and
    content type agreement, and summary length ratio.
    """
    benchmark_ids_sql = f"""
        SELECT article_id FROM {_DB}.benchmark_reference
        ORDER BY cityHash64(article_id) LIMIT 50
    """
    article_ids = [r["article_id"] for r in _query(benchmark_ids_sql)]
    if not article_ids:
        return {"error": "No benchmark reference data found"}

    placeholders = ", ".join(f"'{a}'" for a in article_ids)
    baseline_model = "qwen3:30b-a3b"

    # Find all models with benchmark data
    model_rows = _query(f"""
        SELECT model_used, count() AS cnt
        FROM {_DB}.article_enrichment FINAL
        WHERE article_id IN ({placeholders})
        GROUP BY model_used ORDER BY model_used
    """)

    # Get timing from enrichment_log
    timing = {}
    for r in _query(f"""
        SELECT model, articles_enriched,
               dateDiff('second', run_at, run_at) AS dummy
        FROM {_DB}.enrichment_log
        WHERE status = 'benchmark'
        ORDER BY run_at DESC
    """):
        if r["model"] not in timing:
            timing[r["model"]] = r["articles_enriched"]

    return {
        "baseline": baseline_model,
        "sample_size": len(article_ids),
        "models": model_rows,
        "note": "Run scripts/benchmark_compare.py for full quality analysis",
    }


# ---------------------------------------------------------------------------
# Alert & Saved Search tools
# ---------------------------------------------------------------------------


def _command(sql: str, parameters: dict | None = None):
    """Run a write command (INSERT, ALTER, etc.)."""
    client = get_client()
    try:
        client.command(sql, parameters=parameters)
    finally:
        client.close()


@mcp.tool()
def list_alerts(
    alert_type: str | None = None,
    severity: str | None = None,
    acknowledged: bool | None = None,
    limit: int = 50,
) -> list[dict]:
    """List system alerts (enrichment crashes, API limits, ingestion failures, stale DB, search matches).

    Args:
        alert_type: Filter by type (enrichment_crash, api_limit, ingestion_failure, stale_db, search_match)
        severity: Filter by severity (info, warning, critical)
        acknowledged: Filter by acknowledged status (true/false)
        limit: Max results (1-200, default 50)
    """
    limit = max(1, min(limit, 200))
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if alert_type:
        conditions.append("alert_type = {alert_type:String}")
        params["alert_type"] = alert_type
    if severity:
        conditions.append("severity = {severity:String}")
        params["severity"] = severity
    if acknowledged is not None:
        conditions.append("acknowledged = {ack:UInt8}")
        params["ack"] = 1 if acknowledged else 0

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT alert_id, alert_type, severity, message, context,
               created_at, acknowledged
        FROM {_DB}.alerts
        {where}
        ORDER BY created_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def acknowledge_alert(alert_id: str) -> dict:
    """Mark an alert as acknowledged.

    Args:
        alert_id: The UUID of the alert to acknowledge
    """
    # ClickHouse MergeTree is append-only; use ALTER to update
    _command(
        f"ALTER TABLE {_DB}.alerts UPDATE acknowledged = 1 "
        f"WHERE alert_id = %(alert_id)s",
        parameters={"alert_id": alert_id},
    )
    return {"status": "acknowledged", "alert_id": alert_id}


@mcp.tool()
def create_saved_search(
    label: str,
    query: str,
    email: str = "",
) -> dict:
    """Create a saved search to track articles matching a keyword/phrase.

    New articles matching the search query will generate alerts.

    Args:
        label: Human-readable name for this search (e.g. "David Pocock")
        query: Search term to match against article titles and body text
        email: Optional email for future notifications
    """
    search_id = str(uuid.uuid4())
    _command(
        f"INSERT INTO {_DB}.saved_searches (search_id, label, query, email) "
        f"VALUES (%(id)s, %(label)s, %(query)s, %(email)s)",
        parameters={"id": search_id, "label": label, "query": query, "email": email},
    )
    return {"search_id": search_id, "label": label, "query": query}


@mcp.tool()
def list_saved_searches() -> list[dict]:
    """List all saved searches."""
    return _query(
        f"SELECT search_id, label, query, email, active, created_at "
        f"FROM {_DB}.saved_searches FINAL "
        f"ORDER BY created_at DESC"
    )


@mcp.tool()
def delete_saved_search(search_id: str) -> dict:
    """Delete a saved search by its ID.

    Args:
        search_id: The UUID of the saved search to delete
    """
    _command(
        f"ALTER TABLE {_DB}.saved_searches UPDATE active = 0 "
        f"WHERE search_id = %(id)s",
        parameters={"id": search_id},
    )
    return {"status": "deleted", "search_id": search_id}


@mcp.tool()
def list_search_matches(
    search_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Show articles that matched a saved search.

    Args:
        search_id: Filter to a specific saved search UUID
        limit: Max results (1-200, default 50)
    """
    limit = max(1, min(limit, 200))
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if search_id:
        conditions.append("m.search_id = {search_id:String}")
        params["search_id"] = search_id

    where = f"AND {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT m.match_id, m.search_id, m.article_id, m.matched_at,
               s.label AS search_label, s.query AS search_query,
               a.title, a.published_at, a.url
        FROM {_DB}.search_matches AS m
        LEFT JOIN {_DB}.saved_searches AS s FINAL ON s.search_id = m.search_id
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = m.article_id
        WHERE 1=1 {where}
        ORDER BY m.matched_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------


def _bearer_middleware():
    """Return a Starlette Middleware that enforces Bearer token auth."""
    from starlette.middleware import Middleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import PlainTextResponse

    token = MCP_AUTH_TOKEN

    class BearerAuth(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            # Let .well-known discovery requests pass through with 404
            # so Claude Desktop doesn't enter an OAuth flow
            if request.url.path.startswith("/.well-known/"):
                return PlainTextResponse("Not Found", status_code=404)
            auth = request.headers.get("authorization", "")
            if auth != f"Bearer {token}":
                return PlainTextResponse("Unauthorized", status_code=401)
            return await call_next(request)

    return Middleware(BearerAuth)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main():
    """Start the MCP server on Streamable HTTP."""
    if not MCP_AUTH_TOKEN:
        print(
            "Error: MCP_AUTH_TOKEN environment variable is required",
            file=sys.stderr,
        )
        sys.exit(1)

    port = int(os.environ.get("MCP_PORT", "8765"))
    import uvicorn

    app = mcp.http_app(
        transport="streamable-http",
        middleware=[_bearer_middleware()],
    )
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
