"""NowTheNews API — FastAPI backend for the interactive GUI."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import clickhouse_connect
import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from newschat.config import CLICKHOUSE_DATABASE, CLICKHOUSE_DSN, GROQ_API_KEY

log = logging.getLogger(__name__)
_DB = CLICKHOUSE_DATABASE

# ---------------------------------------------------------------------------
# ClickHouse connection pool (module-level singleton)
# ---------------------------------------------------------------------------
_ch = None


def _get_ch():
    global _ch
    if _ch is None:
        _ch = clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)
    return _ch


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    global _ch
    if _ch is not None:
        _ch.close()
        _ch = None


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


@app.get("/api/entity/{entity_name}/articles")
def get_entity_articles(
    entity_name: str,
    time_from: str | None = Query(None),
    time_to: str | None = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """Articles mentioning a specific entity."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [
        f"e.prompt_version = %(pv)s",
        "has(arrayMap(x -> lower(x), e.`entities.name`), %(ename)s)",
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
            a.source, a.section_name, a.published_at, a.url
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
    """Articles where both entities co-occur."""
    ch = _get_ch()
    time_clause, params = _time_filter("a", time_from, time_to)

    where_parts = [
        f"e.prompt_version = %(pv)s",
        "has(arrayMap(x -> lower(x), e.`entities.name`), %(ea)s)",
        "has(arrayMap(x -> lower(x), e.`entities.name`), %(eb)s)",
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
            a.source, a.section_name, a.published_at, a.url
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

    Multiple terms separated by commas — ALL must match (AND logic).
    """
    ch = _get_ch()
    terms = [t.strip() for t in q.split(",") if t.strip()]
    if not terms:
        return []

    params: dict = {"limit": limit, "offset": offset}
    where_parts: list[str] = []

    for i, term in enumerate(terms):
        key = f"q{i}"
        params[key] = term
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
