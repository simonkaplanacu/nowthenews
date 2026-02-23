"""MCP server exposing nowthenews data over Streamable HTTP.

Start with:
    MCP_AUTH_TOKEN=secret newschat-mcp
    MCP_AUTH_TOKEN=secret MCP_PORT=9000 python -m newschat.mcp.server

Future writes gated behind MCP_ENABLE_WRITES (not yet implemented).
"""

from __future__ import annotations

import os
import sys
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
        "Read-only access to Guardian news articles enriched with entities, "
        "sentiment, policy domains, smoke terms, and quotes."
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

    Args:
        query: Search text (case-insensitive substring match)
        from_date: Filter by publish date >= this (YYYY-MM-DD)
        to_date: Filter by publish date <= this (YYYY-MM-DD)
        section: Filter by section_id (e.g. 'politics', 'world')
        limit: Max results (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conditions = [
        "(positionCaseInsensitive(title, {query:String}) > 0"
        " OR positionCaseInsensitive(headline, {query:String}) > 0"
        " OR positionCaseInsensitive(body_text, {query:String}) > 0)"
    ]
    params: dict = {"query": query, "limit": limit}
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
    """Fetch enrichment data (entities, sentiment, smoke terms, quotes) for an article.

    Args:
        article_id: The article identifier
    """
    rows = _query(
        f"""
        SELECT article_id, enriched_at, sentiment, sentiment_score,
               framing_notes, event_signature, event_date, summary,
               model_used, prompt_version,
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
            "model_used", "prompt_version",
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
        FROM {_DB}.article_enrichment FINAL AS e
        ARRAY JOIN e.entities.name AS e_name, e.entities.type AS e_type
        LEFT JOIN {_DB}.articles FINAL AS a ON a.article_id = e.article_id
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
            f"JOIN {_DB}.articles FINAL AS a "
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
        FROM {_DB}.article_enrichment FINAL AS e
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
        FROM {_DB}.article_enrichment FINAL AS e
        ARRAY JOIN e.smoke_terms.term AS st_term,
                   e.smoke_terms.context AS st_context,
                   e.smoke_terms.rationale AS st_rationale
        LEFT JOIN {_DB}.articles FINAL AS a ON a.article_id = e.article_id
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
        FROM {_DB}.article_enrichment FINAL AS e
        ARRAY JOIN e.quotes.quote AS q_quote,
                   e.quotes.speaker AS q_speaker,
                   e.quotes.context AS q_context
        LEFT JOIN {_DB}.articles FINAL AS a ON a.article_id = e.article_id
        {where}
        ORDER BY a.published_at DESC
        LIMIT {{limit:UInt32}}
    """
    return _query(sql, params)


@mcp.tool()
def db_stats() -> dict:
    """Row counts for all tables in the database."""
    stats = {}
    # ReplacingMergeTree tables — use FINAL for accurate deduped counts
    for table in ("articles", "article_tags", "article_enrichment"):
        stats[table] = _scalar(f"SELECT count() FROM {_DB}.{table} FINAL")
    # Plain MergeTree tables
    for table in ("enrichment_log", "ingestion_log"):
        stats[table] = _scalar(f"SELECT count() FROM {_DB}.{table}")
    return stats


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
