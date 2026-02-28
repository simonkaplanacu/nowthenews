"""NowTheNews API — FastAPI backend for the interactive GUI."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import clickhouse_connect
import httpx
import uuid

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from newschat.config import CLICKHOUSE_DATABASE, CLICKHOUSE_DSN, GROQ_API_KEY

log = logging.getLogger(__name__)
_DB = CLICKHOUSE_DATABASE

# ---------------------------------------------------------------------------
# ClickHouse connection — new client per request to avoid concurrent query errors
# ---------------------------------------------------------------------------


def _get_ch():
    return clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="NowTheNews API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_PROMPT_VERSION = "v3"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _time_filter(alias: str, time_from: str | None, time_to: str | None) -> tuple[str, dict]:
    """Build WHERE clauses for time range filtering on published_at."""
    clauses = []
    params = {}
    if time_from:
        clauses.append(f"{alias}.published_at >= %(time_from)s")
        params["time_from"] = time_from
    if time_to:
        clauses.append(f"{alias}.published_at <= %(time_to)s")
        params["time_to"] = time_to
    return (" AND ".join(clauses), params) if clauses else ("", {})


# ---------------------------------------------------------------------------
# NL query — Groq-powered natural language → structured filters
# ---------------------------------------------------------------------------

_NL_SYSTEM_PROMPT = f"""\
You are a filter parser for a news article database. Given a natural-language
query, extract structured filters. Return JSON only — no commentary.

Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

## Available vocabularies

topics (pick 0-4):
  domestic_politics, international_relations, trade, defence_security,
  economy, business, immigration, law_justice, health, education,
  environment, technology, culture_arts, sport, social_issues,
  media, religion, science, human_interest, conflict_crisis,
  transport, energy, agriculture_food, infrastructure_planning,
  tourism_travel, history_heritage, labour

regions (pick 0-3):
  north_america, latin_america_caribbean, europe, middle_east,
  asia_pacific, oceania, africa, global

entity_types (pick 0-1):
  person, organisation, place, event, legislation, statistic,
  work, product, species, substance, concept, medical_condition, technology

## Output schema
{{
  "topics": [],          // matching topic codes from the vocabulary above
  "regions": [],         // matching region codes
  "entity_type": "",     // single entity type or empty string
  "time_from": "",       // ISO date YYYY-MM-DD or empty string
  "time_to": ""          // ISO date YYYY-MM-DD or empty string
}}

Rules:
- Only use values from the vocabularies above — never invent new ones.
- For relative time ("past year", "last 3 months") compute dates from today.
- If the query doesn't imply a filter dimension, leave it empty.
- Return ONLY the JSON object, no markdown fences or explanation.
"""


@app.get("/api/nl-query")
async def nl_query(q: str = Query(..., min_length=2)):
    """Parse a natural-language query into structured graph filters via Groq."""
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY not configured"}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={
                "model": "qwen/qwen3-32b",
                "messages": [
                    {"role": "system", "content": _NL_SYSTEM_PROMPT},
                    {"role": "user", "content": q},
                ],
                "temperature": 0,
                "max_tokens": 512,
                "response_format": {"type": "json_object"},
            },
        )
        resp.raise_for_status()
        data = resp.json()

    import json as _json
    text = data["choices"][0]["message"]["content"]
    try:
        parsed = _json.loads(text)
    except _json.JSONDecodeError:
        return {"error": "LLM returned invalid JSON", "raw": text}

    # Validate against vocabularies
    from newschat.enrich.schema import TOPIC_VALUES, REGION_CODES, ENTITY_TYPES
    import typing
    valid_topics = set(typing.get_args(TOPIC_VALUES))
    valid_regions = set(typing.get_args(REGION_CODES))
    valid_entity_types = set(typing.get_args(ENTITY_TYPES))

    return {
        "topics": [t for t in parsed.get("topics", []) if t in valid_topics],
        "regions": [r for r in parsed.get("regions", []) if r in valid_regions],
        "entity_type": parsed.get("entity_type", "") if parsed.get("entity_type", "") in valid_entity_types else "",
        "time_from": parsed.get("time_from", ""),
        "time_to": parsed.get("time_to", ""),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/entities")
def get_entities(
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    entity_type: str | None = Query(None),
    min_articles: int = Query(1),
    limit: int = Query(200),
):
    """List entities ranked by article count."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [f"e.prompt_version = %(pv)s"]
    params["pv"] = _PROMPT_VERSION
    if time_clause:
        where_parts.append(time_clause)

    type_filter = ""
    if entity_type:
        type_filter = "AND ent_type = %(etype)s"
        params["etype"] = entity_type

    where = " AND ".join(where_parts)
    params["min_articles"] = min_articles
    params["limit"] = limit

    query = f"""
        SELECT
            lower(ent_name) AS entity_name,
            ent_type AS entity_type,
            count(DISTINCT e.article_id) AS article_count
        FROM {_DB}.article_enrichment e FINAL
        ARRAY JOIN e.`entities.name` AS ent_name, e.`entities.type` AS ent_type
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        WHERE {where} {type_filter}
        GROUP BY entity_name, entity_type
        HAVING article_count >= %(min_articles)s
        ORDER BY article_count DESC
        LIMIT %(limit)s
    """
    rows = ch.query(query, parameters=params).result_rows
    return [
        {"name": r[0], "type": r[1], "article_count": r[2]}
        for r in rows
    ]


@app.get("/api/entity-graph")
def get_entity_graph(
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    entity_type: str | None = Query(None),
    topic: str | None = Query(None),
    region: str | None = Query(None),
    min_cooccurrence: int = Query(2),
    min_articles: int = Query(3),
    limit: int = Query(150),
):
    """Return nodes and edges for the entity co-occurrence graph."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [f"e.prompt_version = %(pv)s"]
    params["pv"] = _PROMPT_VERSION
    if time_clause:
        where_parts.append(time_clause)

    type_filter = ""
    if entity_type:
        type_filter = "AND ent_type = %(etype)s"
        params["etype"] = entity_type

    # Topic / region joins narrow the article set
    extra_joins = ""
    if topic:
        extra_joins += f" INNER JOIN {_DB}.article_topics t ON t.article_id = e.article_id AND t.topic = %(topic)s"
        params["topic"] = topic
    if region:
        extra_joins += f" INNER JOIN {_DB}.article_regions r ON r.article_id = e.article_id AND r.region = %(region)s"
        params["region"] = region

    where = " AND ".join(where_parts)
    params["min_articles"] = min_articles
    params["limit"] = limit
    params["min_cooccurrence"] = min_cooccurrence

    # Step 1: Get top entities (nodes)
    nodes_query = f"""
        SELECT
            lower(ent_name) AS entity_name,
            ent_type AS entity_type,
            count(DISTINCT e.article_id) AS article_count
        FROM {_DB}.article_enrichment e FINAL
        ARRAY JOIN e.`entities.name` AS ent_name, e.`entities.type` AS ent_type
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        {extra_joins}
        WHERE {where} {type_filter}
        GROUP BY entity_name, entity_type
        HAVING article_count >= %(min_articles)s
        ORDER BY article_count DESC
        LIMIT %(limit)s
    """
    node_rows = ch.query(nodes_query, parameters=params).result_rows
    nodes = [
        {"id": r[0], "name": r[0], "type": r[1], "article_count": r[2]}
        for r in node_rows
    ]
    node_names = {r[0] for r in node_rows}

    if len(node_names) < 2:
        return {"nodes": nodes, "edges": []}

    # Step 2: Co-occurrence edges between top entities
    edges_query = f"""
        SELECT
            e1_name, e2_name, count(DISTINCT aid) AS weight
        FROM (
            SELECT
                e.article_id AS aid,
                lower(n1) AS e1_name,
                lower(n2) AS e2_name
            FROM {_DB}.article_enrichment e FINAL
            ARRAY JOIN e.`entities.name` AS n1
            INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
            {extra_joins}
            ARRAY JOIN e.`entities.name` AS n2
            WHERE {where}
              AND e1_name < e2_name
        )
        WHERE e1_name IN %(names)s AND e2_name IN %(names)s
        GROUP BY e1_name, e2_name
        HAVING weight >= %(min_cooccurrence)s
        ORDER BY weight DESC
        LIMIT 300
    """
    params["names"] = list(node_names)
    edge_rows = ch.query(edges_query, parameters=params).result_rows
    edges = [
        {"source": r[0], "target": r[1], "weight": r[2]}
        for r in edge_rows
    ]

    return {"nodes": nodes, "edges": edges}


@app.get("/api/entity-ego/{entity_name}")
def get_entity_ego(
    entity_name: str,
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    entity_type: str | None = Query(None),
    limit: int = Query(50),
):
    """Return ego graph: a named entity + all co-occurring entities + edges."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)
    params["pv"] = _PROMPT_VERSION
    params["ename"] = entity_name.lower()
    params["limit"] = limit

    where_parts = [
        "e.prompt_version = %(pv)s",
        "has(arrayMap(x -> lower(x), e.`entities.name`), %(ename)s)",
    ]
    if time_clause:
        where_parts.append(time_clause)
    where = " AND ".join(where_parts)

    # Get all entities co-occurring with the target entity
    type_filter = ""
    if entity_type:
        type_filter = "AND ent_type = %(etype)s"
        params["etype"] = entity_type

    nodes_query = f"""
        SELECT
            lower(ent_name) AS entity_name,
            ent_type AS entity_type,
            count(DISTINCT e.article_id) AS article_count
        FROM {_DB}.article_enrichment e FINAL
        ARRAY JOIN e.`entities.name` AS ent_name, e.`entities.type` AS ent_type
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        WHERE {where} {type_filter}
        GROUP BY entity_name, entity_type
        ORDER BY article_count DESC
        LIMIT %(limit)s
    """
    node_rows = ch.query(nodes_query, parameters=params).result_rows
    nodes = [
        {"id": r[0], "name": r[0], "type": r[1], "article_count": r[2]}
        for r in node_rows
    ]
    node_names = {r[0] for r in node_rows}

    if len(node_names) < 2:
        return {"nodes": nodes, "edges": []}

    # Edges between these entities (within articles mentioning the target)
    edges_query = f"""
        SELECT
            e1_name, e2_name, count(DISTINCT aid) AS weight
        FROM (
            SELECT
                e.article_id AS aid,
                lower(n1) AS e1_name,
                lower(n2) AS e2_name
            FROM {_DB}.article_enrichment e FINAL
            ARRAY JOIN e.`entities.name` AS n1
            INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
            ARRAY JOIN e.`entities.name` AS n2
            WHERE {where}
              AND e1_name < e2_name
        )
        WHERE e1_name IN %(names)s AND e2_name IN %(names)s
        GROUP BY e1_name, e2_name
        HAVING weight >= 1
        ORDER BY weight DESC
        LIMIT 300
    """
    params["names"] = list(node_names)
    edge_rows = ch.query(edges_query, parameters=params).result_rows
    edges = [
        {"source": r[0], "target": r[1], "weight": r[2]}
        for r in edge_rows
    ]

    return {"nodes": nodes, "edges": edges}


@app.get("/api/entity/{entity_name}/articles")
def get_entity_articles(
    entity_name: str,
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """Articles mentioning a specific entity (substring match)."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [
        f"e.prompt_version = %(pv)s",
        "arrayExists(x -> positionCaseInsensitive(x, %(ename)s) > 0, e.`entities.name`)",
    ]
    params["pv"] = _PROMPT_VERSION
    params["ename"] = entity_name.lower()
    if time_clause:
        where_parts.append(time_clause)

    where = " AND ".join(where_parts)
    params["limit"] = limit
    params["offset"] = offset

    query = f"""
        SELECT
            a.article_id, a.title, a.headline, a.standfirst,
            a.source, a.section_name, a.published_at, a.url,
            e.sentiment, e.content_type, e.summary
        FROM {_DB}.article_enrichment e FINAL
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        WHERE {where}
        ORDER BY a.published_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = ch.query(query, parameters=params).result_rows
    return [
        {
            "article_id": r[0], "title": r[1], "headline": r[2],
            "standfirst": r[3], "source": r[4], "section": r[5],
            "published_at": r[6].isoformat() if r[6] else None, "url": r[7],
            "sentiment": r[8], "content_type": r[9], "summary": r[10],
        }
        for r in rows
    ]


@app.get("/api/cooccurrence/{entity_a}/{entity_b}/articles")
def get_cooccurrence_articles(
    entity_a: str,
    entity_b: str,
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    limit: int = Query(50),
):
    """Articles where both entities co-occur (substring match)."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [
        f"e.prompt_version = %(pv)s",
        "arrayExists(x -> positionCaseInsensitive(x, %(ea)s) > 0, e.`entities.name`)",
        "arrayExists(x -> positionCaseInsensitive(x, %(eb)s) > 0, e.`entities.name`)",
    ]
    params["pv"] = _PROMPT_VERSION
    params["ea"] = entity_a.lower()
    params["eb"] = entity_b.lower()
    if time_clause:
        where_parts.append(time_clause)

    where = " AND ".join(where_parts)
    params["limit"] = limit

    query = f"""
        SELECT
            a.article_id, a.title, a.headline, a.standfirst,
            a.source, a.section_name, a.published_at, a.url,
            e.sentiment, e.content_type, e.summary
        FROM {_DB}.article_enrichment e FINAL
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        WHERE {where}
        ORDER BY a.published_at DESC
        LIMIT %(limit)s
    """
    rows = ch.query(query, parameters=params).result_rows
    return [
        {
            "article_id": r[0], "title": r[1], "headline": r[2],
            "standfirst": r[3], "source": r[4], "section": r[5],
            "published_at": r[6].isoformat() if r[6] else None, "url": r[7],
            "sentiment": r[8], "content_type": r[9], "summary": r[10],
        }
        for r in rows
    ]


@app.get("/api/topic-river")
def get_topic_river(
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    region: str | None = Query(None),
    bucket: str = Query("day"),
):
    """Time-series data for the topic streamgraph."""
    ch = _get_ch()
    params = {"pv": _PROMPT_VERSION}

    time_parts = []
    if time_from:
        time_parts.append("a.published_at >= %(time_from)s")
        params["time_from"] = time_from
    if time_to:
        time_parts.append("a.published_at <= %(time_to)s")
        params["time_to"] = time_to

    time_where = (" AND " + " AND ".join(time_parts)) if time_parts else ""

    region_join = ""
    if region:
        region_join = f"INNER JOIN {_DB}.article_regions r ON r.article_id = a.article_id AND r.region = %(region)s"
        params["region"] = region

    if bucket == "hour":
        trunc = "toStartOfHour(a.published_at)"
    else:
        trunc = "toDate(a.published_at)"

    query = f"""
        SELECT
            {trunc} AS ts,
            t.topic,
            count(DISTINCT a.article_id) AS cnt
        FROM {_DB}.article_topics t
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = t.article_id
        INNER JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id
        {region_join}
        WHERE e.prompt_version = %(pv)s {time_where}
        GROUP BY ts, t.topic
        ORDER BY ts, t.topic
    """
    rows = ch.query(query, parameters=params).result_rows

    # Pivot into {timestamps, series} format
    from collections import defaultdict
    ts_set = set()
    by_topic = defaultdict(dict)
    for ts, topic, cnt in rows:
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        ts_set.add(ts_str)
        by_topic[topic][ts_str] = cnt

    timestamps = sorted(ts_set)
    series = []
    for topic, values_map in sorted(by_topic.items()):
        series.append({
            "topic": topic,
            "values": [values_map.get(ts, 0) for ts in timestamps],
        })

    return {"timestamps": timestamps, "series": series}


@app.get("/api/articles")
def get_articles(
    topic: str | None = Query(None),
    region: str | None = Query(None),
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """Paginated article list with optional filters."""
    ch = _get_ch()
    params = {"pv": _PROMPT_VERSION, "limit": limit, "offset": offset}

    where_parts = ["e.prompt_version = %(pv)s"]
    joins = ""

    time_clause, time_params = _time_filter("a", time_from, time_to)
    params.update(time_params)
    if time_clause:
        where_parts.append(time_clause)

    if topic:
        joins += f" INNER JOIN {_DB}.article_topics t ON t.article_id = a.article_id AND t.topic = %(topic)s"
        params["topic"] = topic

    if region:
        joins += f" INNER JOIN {_DB}.article_regions r ON r.article_id = a.article_id AND r.region = %(region)s"
        params["region"] = region

    where = " AND ".join(where_parts)

    query = f"""
        SELECT
            a.article_id, a.title, a.headline, a.standfirst,
            a.source, a.section_name, a.published_at, a.url,
            e.sentiment, e.content_type, e.summary
        FROM {_DB}.article_enrichment e FINAL
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        {joins}
        WHERE {where}
        ORDER BY a.published_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = ch.query(query, parameters=params).result_rows
    return [
        {
            "article_id": r[0], "title": r[1], "headline": r[2],
            "standfirst": r[3], "source": r[4], "section": r[5],
            "published_at": r[6].isoformat() if r[6] else None, "url": r[7],
            "sentiment": r[8], "content_type": r[9], "summary": r[10],
        }
        for r in rows
    ]


@app.get("/api/text-search")
def text_search(
    q: str = Query(..., min_length=2),
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """Full-text substring search across article body, title, and headline.

    Each word is matched independently (AND logic) — all words must appear
    somewhere in the article's title, headline, or body text.
    """
    ch = _get_ch()
    # Split on commas first, then split each part on whitespace
    words: list[str] = []
    for part in q.split(","):
        words.extend(w for w in part.strip().split() if w)
    if not words:
        return []

    params: dict = {"limit": limit, "offset": offset}
    where_parts: list[str] = []

    for i, word in enumerate(words):
        key = f"q{i}"
        params[key] = word
        where_parts.append(
            f"(positionCaseInsensitive(a.body_text, %({key})s) > 0"
            f" OR positionCaseInsensitive(a.title, %({key})s) > 0"
            f" OR positionCaseInsensitive(a.headline, %({key})s) > 0)"
        )

    time_clause, time_params = _time_filter("a", time_from, time_to)
    params.update(time_params)
    if time_clause:
        where_parts.append(time_clause)

    where = " AND ".join(where_parts)

    query = f"""
        SELECT
            a.article_id, a.title, a.headline, a.standfirst,
            a.source, a.section_name, a.published_at, a.url, a.word_count
        FROM {_DB}.articles a FINAL
        WHERE {where}
        ORDER BY a.published_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = ch.query(query, parameters=params).result_rows
    return [
        {
            "article_id": r[0], "title": r[1], "headline": r[2],
            "standfirst": r[3], "source": r[4], "section": r[5],
            "published_at": r[6].isoformat() if r[6] else None, "url": r[7],
            "word_count": r[8],
        }
        for r in rows
    ]


@app.get("/api/sentiment-heatmap")
def get_sentiment_heatmap(
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    region: str | None = Query(None),
    bucket: str = Query("week"),
):
    """Topic × time grid colored by average sentiment score."""
    from newschat.enrich.schema import TOPIC_VALUES
    import typing
    valid_topics = list(typing.get_args(TOPIC_VALUES))

    ch = _get_ch()
    params: dict = {"pv": _PROMPT_VERSION, "valid_topics": valid_topics}

    time_parts = []
    if time_from:
        time_parts.append("a.published_at >= %(time_from)s")
        params["time_from"] = time_from
    if time_to:
        time_parts.append("a.published_at <= %(time_to)s")
        params["time_to"] = time_to
    time_where = (" AND " + " AND ".join(time_parts)) if time_parts else ""

    region_join = ""
    if region:
        region_join = f"INNER JOIN {_DB}.article_regions r ON r.article_id = a.article_id AND r.region = %(region)s"
        params["region"] = region

    trunc = "toMonday(a.published_at)" if bucket == "week" else "toDate(a.published_at)"

    query = f"""
        SELECT
            {trunc} AS ts,
            t.topic,
            avg(e.sentiment_score) AS avg_sentiment,
            count(DISTINCT a.article_id) AS cnt
        FROM {_DB}.article_topics t
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = t.article_id
        INNER JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id
        {region_join}
        WHERE e.prompt_version = %(pv)s AND e.sentiment_score != 0
          AND t.topic IN %(valid_topics)s
          {time_where}
        GROUP BY ts, t.topic
        ORDER BY ts, t.topic
    """
    rows = ch.query(query, parameters=params).result_rows

    ts_set: set[str] = set()
    topic_set: set[str] = set()
    cells = []
    for ts, topic, avg_s, cnt in rows:
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        ts_set.add(ts_str)
        topic_set.add(topic)
        cells.append({"ts": ts_str, "topic": topic, "avg_sentiment": round(float(avg_s), 3), "count": cnt})

    return {
        "timestamps": sorted(ts_set),
        "topics": sorted(topic_set),
        "cells": cells,
    }


@app.get("/api/entity-timeline")
def get_entity_timeline(
    entities: str = Query(..., min_length=1),
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    bucket: str = Query("week"),
):
    """Time-series of entity mention counts (substring match on names)."""
    ch = _get_ch()
    search_terms = [e.strip().lower() for e in entities.split(",") if e.strip()]
    if not search_terms:
        return {"timestamps": [], "series": []}

    params: dict = {"pv": _PROMPT_VERSION}

    # Step 1: Resolve each search term to the best-matching entity name.
    # Prefer exact matches, then matches where the term is a whole word,
    # then shortest names (most specific), breaking ties by article count.
    resolved: list[str] = []
    for i, term in enumerate(search_terms):
        key = f"term{i}"
        params[key] = term
        resolve_query = f"""
            SELECT lower(ent_name) AS n, count(DISTINCT e.article_id) AS c
            FROM {_DB}.article_enrichment e FINAL
            ARRAY JOIN e.`entities.name` AS ent_name
            WHERE e.prompt_version = %(pv)s
              AND positionCaseInsensitive(ent_name, %({key})s) > 0
            GROUP BY n
            HAVING c >= 2
            ORDER BY
              (n = %({key})s OR n LIKE concat(%({key})s, ' %%') OR n LIKE concat('%% ', %({key})s)) DESC,
              c DESC
            LIMIT 1
        """
        rows = ch.query(resolve_query, parameters=params).result_rows
        if rows:
            resolved.append(rows[0][0])

    if not resolved:
        return {"timestamps": [], "series": []}

    params["resolved"] = resolved

    time_parts = []
    if time_from:
        time_parts.append("a.published_at >= %(time_from)s")
        params["time_from"] = time_from
    if time_to:
        time_parts.append("a.published_at <= %(time_to)s")
        params["time_to"] = time_to
    time_where = (" AND " + " AND ".join(time_parts)) if time_parts else ""

    trunc = "toMonday(a.published_at)" if bucket == "week" else "toDate(a.published_at)"

    query = f"""
        SELECT
            {trunc} AS ts,
            lower(ent_name) AS entity_name,
            count(DISTINCT e.article_id) AS cnt
        FROM {_DB}.article_enrichment e FINAL
        ARRAY JOIN e.`entities.name` AS ent_name
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
        WHERE e.prompt_version = %(pv)s
          AND lower(ent_name) IN %(resolved)s
          {time_where}
        GROUP BY ts, entity_name
        ORDER BY ts, entity_name
    """
    rows = ch.query(query, parameters=params).result_rows

    from collections import defaultdict
    ts_set: set[str] = set()
    by_entity: dict[str, dict[str, int]] = defaultdict(dict)
    for ts, ename, cnt in rows:
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        ts_set.add(ts_str)
        by_entity[ename][ts_str] = cnt

    timestamps = sorted(ts_set)
    series = [
        {"entity": ename, "values": [vals.get(ts, 0) for ts in timestamps]}
        for ename, vals in sorted(by_entity.items())
    ]
    return {"timestamps": timestamps, "series": series}


_VALID_REGIONS = [
    "north_america", "latin_america_caribbean", "europe",
    "middle_east", "asia_pacific", "oceania", "africa", "global",
]


@app.get("/api/region-overview")
def get_region_overview(
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    topic: str | None = Query(None),
):
    """Per-region article count, avg sentiment, and top entities."""
    ch = _get_ch()
    params: dict = {"pv": _PROMPT_VERSION, "valid_regions": _VALID_REGIONS}

    time_parts = []
    if time_from:
        time_parts.append("a.published_at >= %(time_from)s")
        params["time_from"] = time_from
    if time_to:
        time_parts.append("a.published_at <= %(time_to)s")
        params["time_to"] = time_to
    time_where = (" AND " + " AND ".join(time_parts)) if time_parts else ""

    topic_join = ""
    if topic:
        topic_join = f"INNER JOIN {_DB}.article_topics t ON t.article_id = a.article_id AND t.topic = %(topic)s"
        params["topic"] = topic

    # Counts + avg sentiment per region
    summary_query = f"""
        SELECT
            r.region,
            count(DISTINCT a.article_id) AS article_count,
            avg(e.sentiment_score) AS avg_sentiment
        FROM {_DB}.article_regions r
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = r.article_id
        INNER JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id
        {topic_join}
        WHERE e.prompt_version = %(pv)s
          AND r.region IN %(valid_regions)s
          {time_where}
        GROUP BY r.region
        ORDER BY article_count DESC
    """
    summary_rows = ch.query(summary_query, parameters=params).result_rows

    # Top 5 entities per region
    entities_query = f"""
        SELECT
            r.region,
            lower(ent_name) AS entity_name,
            count(DISTINCT a.article_id) AS cnt
        FROM {_DB}.article_regions r
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = r.article_id
        INNER JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id
        ARRAY JOIN e.`entities.name` AS ent_name
        {topic_join}
        WHERE e.prompt_version = %(pv)s
          AND r.region IN %(valid_regions)s
          {time_where}
        GROUP BY r.region, entity_name
        ORDER BY r.region, cnt DESC
    """
    ent_rows = ch.query(entities_query, parameters=params).result_rows

    # Group top 5 entities per region
    from collections import defaultdict
    region_entities: dict[str, list[dict]] = defaultdict(list)
    for region, ename, cnt in ent_rows:
        if len(region_entities[region]) < 5:
            region_entities[region].append({"name": ename, "count": cnt})

    regions = []
    for region, count, avg_s in summary_rows:
        regions.append({
            "region": region,
            "article_count": count,
            "avg_sentiment": round(float(avg_s), 3),
            "top_entities": region_entities.get(region, []),
        })

    return {"regions": regions}


@app.get("/api/topic-trends")
def get_topic_trends(
    weeks: int = Query(4),
    region: str | None = Query(None),
):
    """Emerging/declining topics: current N weeks vs previous N weeks."""
    ch = _get_ch()
    import typing
    from newschat.enrich.schema import TOPIC_VALUES
    valid_topics = list(typing.get_args(TOPIC_VALUES))
    params: dict = {"pv": _PROMPT_VERSION, "weeks": weeks, "valid_topics": valid_topics}

    region_join = ""
    if region:
        region_join = f"INNER JOIN {_DB}.article_regions r ON r.article_id = a.article_id AND r.region = %(region)s"
        params["region"] = region

    query = f"""
        WITH latest AS (
            SELECT max(a.published_at) AS mx
            FROM {_DB}.article_enrichment e FINAL
            INNER JOIN {_DB}.articles a FINAL ON a.article_id = e.article_id
            WHERE e.prompt_version = %(pv)s
        )
        SELECT
            t.topic,
            countIf(a.published_at >= (SELECT mx FROM latest) - %(weeks)s * 7) AS current_count,
            countIf(a.published_at < (SELECT mx FROM latest) - %(weeks)s * 7
                AND a.published_at >= (SELECT mx FROM latest) - %(weeks)s * 14) AS previous_count
        FROM {_DB}.article_topics t
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = t.article_id
        INNER JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id
        {region_join}
        WHERE e.prompt_version = %(pv)s
          AND a.published_at >= (SELECT mx FROM latest) - %(weeks)s * 14
          AND t.topic IN %(valid_topics)s
        GROUP BY t.topic
        HAVING current_count + previous_count > 0
        ORDER BY current_count DESC
    """
    rows = ch.query(query, parameters=params).result_rows
    trends = []
    for topic, current, previous in rows:
        if previous > 0:
            pct_change = round(((current - previous) / previous) * 100, 1)
        elif current > 0:
            pct_change = 100.0
        else:
            pct_change = 0.0
        trends.append({
            "topic": topic,
            "current_count": current,
            "previous_count": previous,
            "pct_change": pct_change,
        })
    return {"trends": trends}


@app.get("/api/stats")
def get_stats():
    """Quick stats for the dashboard."""
    ch = _get_ch()
    params = {"pv": _PROMPT_VERSION}
    row = ch.query(f"""
        SELECT
            count() AS total_articles,
            countIf(e.article_id != '') AS enriched_articles
        FROM {_DB}.articles a FINAL
        LEFT JOIN {_DB}.article_enrichment e FINAL ON e.article_id = a.article_id AND e.prompt_version = %(pv)s
    """, parameters=params).result_rows[0]
    return {"total_articles": row[0], "enriched_articles": row[1]}


# ---------------------------------------------------------------------------
# Alerts & Saved Searches
# ---------------------------------------------------------------------------

@app.get("/api/alerts")
def get_alerts(
    alert_type: str | None = Query(None),
    severity: str | None = Query(None),
    acknowledged: int | None = Query(None),
    limit: int = Query(50),
):
    """List system alerts."""
    ch = _get_ch()
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if alert_type:
        conditions.append("alert_type = %(alert_type)s")
        params["alert_type"] = alert_type
    if severity:
        conditions.append("severity = %(severity)s")
        params["severity"] = severity
    if acknowledged is not None:
        conditions.append("acknowledged = %(ack)s")
        params["ack"] = acknowledged

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = ch.query(f"""
        SELECT alert_id, alert_type, severity, message, context,
               created_at, acknowledged
        FROM {_DB}.alerts
        {where}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """, parameters=params).result_rows
    return [
        {
            "alert_id": str(r[0]), "alert_type": r[1], "severity": r[2],
            "message": r[3], "context": r[4],
            "created_at": r[5].isoformat() if r[5] else None,
            "acknowledged": r[6],
        }
        for r in rows
    ]


@app.post("/api/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str):
    """Mark an alert as acknowledged."""
    ch = _get_ch()
    ch.command(
        f"ALTER TABLE {_DB}.alerts UPDATE acknowledged = 1 "
        f"WHERE alert_id = %(alert_id)s",
        parameters={"alert_id": alert_id},
    )
    return {"status": "acknowledged", "alert_id": alert_id}


class SavedSearchCreate(BaseModel):
    label: str
    query: str
    email: str = ""


@app.get("/api/saved-searches")
def get_saved_searches():
    """List all active saved searches."""
    ch = _get_ch()
    rows = ch.query(f"""
        SELECT search_id, label, query, email, active, created_at
        FROM {_DB}.saved_searches FINAL
        WHERE active = 1
        ORDER BY created_at DESC
    """).result_rows
    return [
        {
            "search_id": str(r[0]), "label": r[1], "query": r[2],
            "email": r[3], "active": r[4],
            "created_at": r[5].isoformat() if r[5] else None,
        }
        for r in rows
    ]


@app.post("/api/saved-searches")
def create_saved_search(body: SavedSearchCreate):
    """Create a new saved search."""
    ch = _get_ch()
    search_id = str(uuid.uuid4())
    ch.command(
        f"INSERT INTO {_DB}.saved_searches (search_id, label, query, email) "
        f"VALUES (%(id)s, %(label)s, %(query)s, %(email)s)",
        parameters={"id": search_id, "label": body.label, "query": body.query, "email": body.email},
    )
    return {"search_id": search_id, "label": body.label, "query": body.query}


@app.delete("/api/saved-searches/{search_id}")
def delete_saved_search(search_id: str):
    """Soft-delete a saved search."""
    ch = _get_ch()
    ch.command(
        f"ALTER TABLE {_DB}.saved_searches UPDATE active = 0 "
        f"WHERE search_id = %(id)s",
        parameters={"id": search_id},
    )
    return {"status": "deleted", "search_id": search_id}


# ---------------------------------------------------------------------------
# Enrichment Exceptions
# ---------------------------------------------------------------------------

@app.get("/api/enrichment-exceptions")
def get_enrichment_exceptions(
    status: str | None = Query(None),
    limit: int = Query(50),
):
    """List articles that repeatedly fail enrichment."""
    ch = _get_ch()
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if status:
        conditions.append("ex.status = %(status)s")
        params["status"] = status

    where = f"AND {' AND '.join(conditions)}" if conditions else ""
    rows = ch.query(f"""
        SELECT ex.article_id, ex.reason, ex.fail_count, ex.status, ex.updated_at,
               a.title, a.published_at
        FROM {_DB}.enrichment_exceptions AS ex FINAL
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = ex.article_id
        WHERE 1=1 {where}
        ORDER BY ex.fail_count DESC, ex.updated_at DESC
        LIMIT %(limit)s
    """, parameters=params).result_rows
    return [
        {
            "article_id": r[0], "reason": r[1], "fail_count": r[2],
            "status": r[3],
            "updated_at": r[4].isoformat() if r[4] else None,
            "title": r[5],
            "published_at": r[6].isoformat() if r[6] else None,
        }
        for r in rows
    ]


class ExceptionUpdate(BaseModel):
    status: str  # 'skip' or 'retry'


@app.post("/api/enrichment-exceptions/{article_id}")
def update_enrichment_exception(article_id: str, body: ExceptionUpdate):
    """Update status of an enrichment exception (skip or retry)."""
    if body.status not in ("skip", "retry"):
        from fastapi import HTTPException
        raise HTTPException(400, "status must be 'skip' or 'retry'")

    ch = _get_ch()
    if body.status == "retry":
        ch.command(
            f"ALTER TABLE {_DB}.enrichment_exceptions DELETE "
            f"WHERE article_id = %(aid)s",
            parameters={"aid": article_id},
        )
        return {"status": "retry", "article_id": article_id}
    else:
        ch.command(
            f"ALTER TABLE {_DB}.enrichment_exceptions UPDATE status = 'skip' "
            f"WHERE article_id = %(aid)s",
            parameters={"aid": article_id},
        )
        return {"status": "skip", "article_id": article_id}


# ---------------------------------------------------------------------------
# Liveblog Blocks
# ---------------------------------------------------------------------------

@app.get("/api/liveblog-blocks/{article_id}")
def get_liveblog_blocks(
    article_id: str,
    limit: int = Query(200),
):
    """Get liveblog blocks for an article, newest first."""
    ch = _get_ch()
    rows = ch.query(f"""
        SELECT block_id, title, body_text, published_at
        FROM {_DB}.liveblog_blocks FINAL
        WHERE article_id = %(aid)s
        ORDER BY published_at DESC
        LIMIT %(limit)s
    """, parameters={"aid": article_id, "limit": limit}).result_rows
    return [
        {
            "article_id": article_id,
            "block_id": r[0],
            "title": r[1],
            "body_text": r[2],
            "published_at": r[3].isoformat() if r[3] else None,
        }
        for r in rows
    ]


@app.get("/api/liveblog-search")
def search_liveblog_blocks(
    q: str = Query(..., min_length=2),
    limit: int = Query(50),
):
    """Search across liveblog block text."""
    ch = _get_ch()
    rows = ch.query(f"""
        SELECT lb.article_id, lb.block_id, lb.title, lb.body_text, lb.published_at,
               a.title AS article_title, a.url
        FROM {_DB}.liveblog_blocks lb FINAL
        INNER JOIN {_DB}.articles a FINAL ON a.article_id = lb.article_id
        WHERE positionCaseInsensitive(lb.body_text, %(q)s) > 0
           OR positionCaseInsensitive(lb.title, %(q)s) > 0
        ORDER BY lb.published_at DESC
        LIMIT %(limit)s
    """, parameters={"q": q, "limit": limit}).result_rows
    return [
        {
            "article_id": r[0],
            "block_id": r[1],
            "title": r[2],
            "body_text": r[3][:500],  # Truncate for listing
            "published_at": r[4].isoformat() if r[4] else None,
            "article_title": r[5],
            "article_url": r[6],
        }
        for r in rows
    ]


@app.get("/api/search-matches")
def get_search_matches(
    search_id: str | None = Query(None),
    limit: int = Query(50),
):
    """List matched articles for saved searches."""
    ch = _get_ch()
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if search_id:
        conditions.append("m.search_id = %(sid)s")
        params["sid"] = search_id

    where = f"AND {' AND '.join(conditions)}" if conditions else ""
    rows = ch.query(f"""
        SELECT m.match_id, m.search_id, m.article_id, m.matched_at,
               s.label, s.query,
               a.title, a.published_at, a.url
        FROM {_DB}.search_matches AS m
        LEFT JOIN {_DB}.saved_searches AS s FINAL ON s.search_id = m.search_id
        LEFT JOIN {_DB}.articles AS a FINAL ON a.article_id = m.article_id
        WHERE 1=1 {where}
        ORDER BY m.matched_at DESC
        LIMIT %(limit)s
    """, parameters=params).result_rows
    return [
        {
            "match_id": str(r[0]), "search_id": str(r[1]), "article_id": r[2],
            "matched_at": r[3].isoformat() if r[3] else None,
            "search_label": r[4], "search_query": r[5],
            "title": r[6],
            "published_at": r[7].isoformat() if r[7] else None,
            "url": r[8],
        }
        for r in rows
    ]
