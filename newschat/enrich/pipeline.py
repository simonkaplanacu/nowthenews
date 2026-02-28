"""Enrichment pipeline — fetch unenriched articles, call LLM, store results."""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path

from pydantic import ValidationError

from newschat.config import CLICKHOUSE_DATABASE, ENRICH_BATCH_SIZE, ENRICH_MODEL
from newschat.db import get_client as get_ch_client
from newschat.enrich.llm import GroqClient, OllamaClient
from newschat.enrich.prompt import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt
from newschat.enrich.schema import (
    CONTENT_TYPES,
    ENTITY_TYPES,
    REGION_CODES,
    SENTIMENT_VALUES,
    TOPIC_VALUES,
    EnrichmentResult,
    LenientEnrichmentResult,
)

log = logging.getLogger(__name__)

_DB = CLICKHOUSE_DATABASE
_INVALID_LABELS_LOG = Path("logs/invalid_labels.jsonl")


_KNOWN_ENTITY_TYPES = set(ENTITY_TYPES.__args__)
_KNOWN_REGIONS = set(REGION_CODES.__args__)
_KNOWN_TOPICS = set(TOPIC_VALUES.__args__)
_KNOWN_CONTENT_TYPES = set(CONTENT_TYPES.__args__)
_KNOWN_SENTIMENTS = set(SENTIMENT_VALUES.__args__)


def _log_nonstandard_labels(article_id: str, result: LenientEnrichmentResult) -> None:
    """Log any labels outside the known vocabulary from a lenient result."""
    entries = []
    ts = datetime.now(timezone.utc).isoformat()
    for i, e in enumerate(result.entities):
        if e.type not in _KNOWN_ENTITY_TYPES:
            entries.append({"article_id": article_id, "field": f"entities.{i}.type", "invalid_value": e.type, "ts": ts})
    for i, g in enumerate(result.geographic_relevance):
        if g.region not in _KNOWN_REGIONS:
            entries.append({"article_id": article_id, "field": f"geographic_relevance.{i}.region", "invalid_value": g.region, "ts": ts})
    for i, t in enumerate(result.topics):
        if t not in _KNOWN_TOPICS:
            entries.append({"article_id": article_id, "field": f"topics.{i}", "invalid_value": t, "ts": ts})
    if result.content_type not in _KNOWN_CONTENT_TYPES:
        entries.append({"article_id": article_id, "field": "content_type", "invalid_value": result.content_type, "ts": ts})
    if result.sentiment not in _KNOWN_SENTIMENTS:
        entries.append({"article_id": article_id, "field": "sentiment", "invalid_value": result.sentiment, "ts": ts})

    if entries:
        _INVALID_LABELS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_INVALID_LABELS_LOG, "a") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")


def _log_invalid_labels(article_id: str, exc: Exception) -> None:
    """Extract invalid enum values from pydantic ValidationErrors and log to JSONL."""
    cause = exc
    while cause is not None:
        if isinstance(cause, ValidationError):
            break
        cause = cause.__cause__
    if not isinstance(cause, ValidationError):
        return

    entries = []
    for err in cause.errors():
        if err["type"] == "literal_error":
            field = ".".join(str(loc) for loc in err["loc"])
            entries.append({
                "article_id": article_id,
                "field": field,
                "invalid_value": err["input"],
                "ts": datetime.now(timezone.utc).isoformat(),
            })

    if entries:
        _INVALID_LABELS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_INVALID_LABELS_LOG, "a") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")


def _unenriched_ids(ch_client, limit: int, model: str) -> list[tuple[str, str, str, str, str, str]]:
    """Return (article_id, title, headline, byline, published_at, body_text) for
    articles that have not yet been enriched by this model+prompt combo.
    Excludes articles with status='skip' in enrichment_exceptions."""
    query = f"""
        SELECT a.article_id, a.title, a.headline, a.byline,
               toString(a.published_at), a.body_text
        FROM {_DB}.articles a
        LEFT ANTI JOIN (
            SELECT article_id FROM {_DB}.article_enrichment
            WHERE model_used = %(model)s AND prompt_version = %(pv)s
        ) e ON a.article_id = e.article_id
        LEFT JOIN {_DB}.enrichment_exceptions ex FINAL
            ON ex.article_id = a.article_id
        WHERE ex.status IS NULL OR ex.status != 'skip'
        ORDER BY a.published_at, a.article_id
        LIMIT %(limit)s
    """
    result = ch_client.query(
        query, parameters={"model": model, "pv": PROMPT_VERSION, "limit": limit}
    )
    return result.result_rows


def _store_enrichment(
    ch_client,
    article_id: str,
    result: EnrichmentResult | LenientEnrichmentResult,
    model: str,
) -> None:
    """Write one enrichment row to ClickHouse."""
    event_date = None
    if result.event_date:
        try:
            event_date = date.fromisoformat(result.event_date)
            # ClickHouse Date type only supports 1970-01-01 onwards
            if event_date < date(1970, 1, 1):
                log.warning("event_date %s before epoch for %s, storing as null", event_date, article_id)
                event_date = None
        except ValueError:
            log.warning("Invalid event_date '%s' for %s", result.event_date, article_id)

    row = [[
        article_id,
        datetime.now(timezone.utc),
        # entities nested
        [e.name for e in result.entities],
        [e.type for e in result.entities],
        # policy nested
        [p.domain for p in result.policy_domains],
        [p.score for p in result.policy_domains],
        # sentiment
        result.sentiment,
        result.sentiment_score,
        # framing
        result.framing_notes,
        # smoke_terms nested
        [s.term for s in result.smoke_terms],
        [s.context for s in result.smoke_terms],
        [s.rationale for s in result.smoke_terms],
        # quotes nested
        [q.quote for q in result.quotes],
        [q.speaker for q in result.quotes],
        [q.context for q in result.quotes],
        # event
        result.event_signature,
        event_date,
        # summary
        result.summary,
        # content type
        result.content_type,
        # metadata
        model,
        PROMPT_VERSION,
    ]]

    ch_client.insert(
        f"{_DB}.article_enrichment",
        row,
        column_names=[
            "article_id", "enriched_at",
            "entities.name", "entities.type",
            "policy.domain", "policy.score",
            "sentiment", "sentiment_score",
            "framing_notes",
            "smoke_terms.term", "smoke_terms.context", "smoke_terms.rationale",
            "quotes.quote", "quotes.speaker", "quotes.context",
            "event_signature", "event_date",
            "summary",
            "content_type",
            "model_used", "prompt_version",
        ],
    )

    # Insert geographic relevance rows
    if result.geographic_relevance:
        region_rows = [
            [article_id, g.region, g.score]
            for g in result.geographic_relevance
        ]
        ch_client.insert(
            f"{_DB}.article_regions",
            region_rows,
            column_names=["article_id", "region", "score"],
        )

    # Insert topic rows
    if result.topics:
        topic_rows = [[article_id, t] for t in result.topics]
        ch_client.insert(
            f"{_DB}.article_topics",
            topic_rows,
            column_names=["article_id", "topic"],
        )


def _record_exception(
    ch_client,
    ch_lock: threading.Lock,
    article_id: str,
    reason: str,
) -> None:
    """Upsert a failure into enrichment_exceptions (ReplacingMergeTree merges on article_id)."""
    try:
        truncated = reason[:500]
        with ch_lock:
            # Read current fail_count (may not exist yet)
            result = ch_client.query(
                f"SELECT fail_count FROM {_DB}.enrichment_exceptions FINAL "
                f"WHERE article_id = %(aid)s",
                parameters={"aid": article_id},
            )
            current = result.result_rows[0][0] if result.result_rows else 0
            ch_client.insert(
                f"{_DB}.enrichment_exceptions",
                [[article_id, truncated, current + 1, "pending", datetime.now(timezone.utc)]],
                column_names=["article_id", "reason", "fail_count", "status", "updated_at"],
            )
    except Exception:
        log.exception("Failed to record enrichment exception for %s", article_id)


def _enrich_one(
    llm,
    ch_client,
    ch_lock: threading.Lock,
    model: str,
    article_id: str,
    title: str,
    headline: str,
    byline: str,
    published_at: str,
    body_text: str,
) -> bool:
    """Enrich a single article. Returns True on success, False on failure."""
    try:
        user_prompt = build_user_prompt(
            title=title,
            headline=headline,
            byline=byline,
            published_at=published_at,
            body_text=body_text,
        )
        result = llm.enrich(system=SYSTEM_PROMPT, user=user_prompt, article_id=article_id)
        if isinstance(result, LenientEnrichmentResult):
            _log_nonstandard_labels(article_id, result)
        with ch_lock:
            _store_enrichment(ch_client, article_id, result, model)
        log.info(
            "Enriched %s — %d entities, %d smoke terms, %d quotes",
            article_id,
            len(result.entities),
            len(result.smoke_terms),
            len(result.quotes),
        )
        return True
    except Exception as exc:
        _log_invalid_labels(article_id, exc)
        log.exception("Failed to enrich %s", article_id)
        _record_exception(ch_client, ch_lock, article_id, str(exc))
        return False


def enrich(
    model: str | None = None,
    limit: int | None = None,
    workers: int | None = None,
) -> dict:
    """Run the enrichment pipeline.

    Args:
        model: Model name. Prefix with ``groq:`` for Groq cloud inference.
        limit: Max articles to process this run. Defaults to config ENRICH_BATCH_SIZE.
        workers: Concurrent threads. Defaults to 8 for Groq, 1 for Ollama.

    Returns:
        Summary dict with counts.
    """
    model = model if model is not None else ENRICH_MODEL
    limit = limit if limit is not None else ENRICH_BATCH_SIZE

    is_groq = model.startswith("groq:")
    if workers is None:
        workers = 8 if is_groq else 1

    ch = None
    llm = None

    enriched = 0
    failed = 0

    try:
        ch = get_ch_client()
        ch_lock = threading.Lock()

        if is_groq:
            groq_model = model[len("groq:"):]
            llm = GroqClient(model=groq_model)
        else:
            llm = OllamaClient(model=model)

        if not llm.check_health():
            backend = "Groq" if is_groq else "Ollama"
            raise RuntimeError(f"{backend} not reachable or model '{model}' not available")

        rows = _unenriched_ids(ch, limit, model)
        total = len(rows)
        log.info(
            "Found %d unenriched articles (model=%s, limit=%d, workers=%d)",
            total, model, limit, workers,
        )

        if workers <= 1:
            for row in rows:
                ok = _enrich_one(llm, ch, ch_lock, model, *row)
                enriched += ok
                failed += not ok
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_enrich_one, llm, ch, ch_lock, model, *row): row[0]
                    for row in rows
                }
                for future in as_completed(futures):
                    ok = future.result()
                    enriched += ok
                    failed += not ok
                    if (enriched + failed) % 50 == 0:
                        log.info("Progress: %d/%d enriched, %d failed", enriched, total, failed)

    except Exception:
        log.exception("Enrichment run failed")
        _log_enrichment(ch, model, enriched + failed, enriched, failed, "error")
        raise
    else:
        _log_enrichment(ch, model, enriched + failed, enriched, failed, "ok")
    finally:
        if llm is not None:
            llm.close()
        if ch is not None:
            ch.close()

    summary = {
        "model": model,
        "prompt_version": PROMPT_VERSION,
        "enriched": enriched,
        "failed": failed,
        "total_attempted": enriched + failed,
    }
    log.info("Enrichment complete: %s", summary)
    return summary


def _log_enrichment(
    ch_client,
    model: str,
    attempted: int,
    enriched: int,
    failed: int,
    status: str,
) -> None:
    """Write an audit row to enrichment_log. Swallows exceptions."""
    try:
        ch_client.insert(
            f"{_DB}.enrichment_log",
            [[model, PROMPT_VERSION, datetime.now(timezone.utc),
              attempted, enriched, failed, status]],
            column_names=[
                "model", "prompt_version", "run_at",
                "articles_attempted", "articles_enriched",
                "articles_failed", "status",
            ],
        )
    except Exception:
        log.exception("Failed to write enrichment log entry")
