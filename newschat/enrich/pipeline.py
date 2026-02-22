"""Enrichment pipeline — fetch unenriched articles, call LLM, store results."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from newschat.config import CLICKHOUSE_DATABASE, ENRICH_BATCH_SIZE, ENRICH_MODEL
from newschat.db import get_client as get_ch_client
from newschat.enrich.llm import OllamaClient
from newschat.enrich.prompt import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt
from newschat.enrich.schema import EnrichmentResult

log = logging.getLogger(__name__)

_DB = CLICKHOUSE_DATABASE


def _unenriched_ids(ch_client, limit: int, model: str) -> list[tuple[str, str, str, str, str, str]]:
    """Return (article_id, title, headline, byline, published_at, body_text) for
    articles that have not yet been enriched by this model+prompt combo."""
    query = f"""
        SELECT a.article_id, a.title, a.headline, a.byline,
               toString(a.published_at), a.body_text
        FROM {_DB}.articles a
        LEFT ANTI JOIN (
            SELECT article_id FROM {_DB}.article_enrichment
            WHERE model_used = %(model)s AND prompt_version = %(pv)s
        ) e ON a.article_id = e.article_id
        ORDER BY a.published_at
        LIMIT %(limit)s
    """
    result = ch_client.query(
        query, parameters={"model": model, "pv": PROMPT_VERSION, "limit": limit}
    )
    return result.result_rows


def _store_enrichment(
    ch_client,
    article_id: str,
    result: EnrichmentResult,
    model: str,
) -> None:
    """Write one enrichment row to ClickHouse."""
    event_date = None
    if result.event_date:
        try:
            event_date = date.fromisoformat(result.event_date)
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
            "model_used", "prompt_version",
        ],
    )


def enrich(
    model: str | None = None,
    limit: int | None = None,
) -> dict:
    """Run the enrichment pipeline.

    Args:
        model: Ollama model name. Defaults to config ENRICH_MODEL.
        limit: Max articles to process this run. Defaults to config ENRICH_BATCH_SIZE.

    Returns:
        Summary dict with counts.
    """
    model = model or ENRICH_MODEL
    limit = limit or ENRICH_BATCH_SIZE

    ch = get_ch_client()
    llm = OllamaClient(model=model)

    enriched = 0
    failed = 0

    try:
        if not llm.check_health():
            raise RuntimeError(
                f"Ollama not reachable or model '{model}' not available at {llm.host}"
            )

        rows = _unenriched_ids(ch, limit, model)
        log.info("Found %d unenriched articles (model=%s, limit=%d)", len(rows), model, limit)

        for article_id, title, headline, byline, published_at, body_text in rows:
            try:
                user_prompt = build_user_prompt(
                    title=title,
                    headline=headline,
                    byline=byline,
                    published_at=published_at,
                    body_text=body_text,
                )
                result = llm.enrich(system=SYSTEM_PROMPT, user=user_prompt)
                _store_enrichment(ch, article_id, result, model)
                enriched += 1
                log.info(
                    "Enriched %s (%d/%d) — %d entities, %d smoke terms, %d quotes",
                    article_id,
                    enriched,
                    len(rows),
                    len(result.entities),
                    len(result.smoke_terms),
                    len(result.quotes),
                )
            except Exception:
                failed += 1
                log.exception("Failed to enrich %s", article_id)

    except Exception:
        log.exception("Enrichment run failed")
        _log_enrichment(ch, model, enriched + failed, enriched, failed, "error")
        raise
    else:
        _log_enrichment(ch, model, enriched + failed, enriched, failed, "ok")
    finally:
        llm.close()
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
