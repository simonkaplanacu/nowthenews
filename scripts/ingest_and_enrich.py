#!/usr/bin/env python
"""Ingest recent Guardian articles and enrich any unenriched backlog.

Designed to run unattended via launchctl every 5 minutes.
"""
import json
import logging
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def _write_alert(alert_type: str, severity: str, message: str, context: str = ""):
    """Write an alert to ClickHouse and send email (best-effort, never raises)."""
    try:
        from newschat.db import write_alert
        write_alert(alert_type, severity, message, context)
        log.info("Alert written: [%s] %s", alert_type, message)
    except Exception:
        log.exception("Failed to write alert")

    # Send email for warning/critical system alerts
    if severity in ("warning", "critical"):
        try:
            from newschat.email import send_alert_email
            send_alert_email(
                f"{severity.upper()}: {alert_type}",
                f"<h3>{alert_type}</h3><p>{message}</p>"
                f"{'<pre>' + context + '</pre>' if context else ''}",
            )
        except Exception:
            log.exception("Failed to send alert email")


def _check_stale_db():
    """Write a stale_db alert if latest article is >24h old."""
    try:
        from newschat.db import get_client
        from newschat.config import CLICKHOUSE_DATABASE
        client = get_client()
        try:
            latest = client.command(
                f"SELECT max(published_at) FROM {CLICKHOUSE_DATABASE}.articles FINAL"
            )
        finally:
            client.close()

        if latest:
            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - latest
            if age > timedelta(hours=24):
                _write_alert(
                    "stale_db", "warning",
                    f"No new articles in {age.total_seconds() / 3600:.0f}h (latest: {latest.isoformat()})",
                    json.dumps({"latest_article": latest.isoformat(), "age_hours": round(age.total_seconds() / 3600, 1)}),
                )
    except Exception:
        log.exception("Stale DB check failed")


def _run_saved_search_matching():
    """Match active saved searches against articles and liveblog blocks from the last 24h."""
    try:
        from newschat.db import get_client
        from newschat.config import CLICKHOUSE_DATABASE
        _DB = CLICKHOUSE_DATABASE
        client = get_client()
        try:
            # Get active saved searches
            result = client.query(
                f"SELECT search_id, label, query FROM {_DB}.saved_searches FINAL WHERE active = 1"
            )
            searches = [(str(r[0]), r[1], r[2]) for r in result.result_rows]
            if not searches:
                return

            for search_id, label, query in searches:
                # Find articles from last 24h matching the search query
                # Also match against liveblog block text
                match_result = client.query(
                    f"""SELECT DISTINCT a.article_id
                        FROM {_DB}.articles a FINAL
                        LEFT JOIN {_DB}.search_matches m
                          ON m.article_id = a.article_id AND m.search_id = %(sid)s
                        LEFT JOIN {_DB}.liveblog_blocks lb FINAL
                          ON lb.article_id = a.article_id
                        WHERE (positionCaseInsensitive(a.title, %(q)s) > 0
                            OR positionCaseInsensitive(a.body_text, %(q)s) > 0
                            OR positionCaseInsensitive(lb.body_text, %(q)s) > 0
                            OR positionCaseInsensitive(lb.title, %(q)s) > 0)
                          AND a.published_at >= now() - INTERVAL 24 HOUR
                          AND m.match_id IS NULL""",
                    parameters={"q": query, "sid": search_id},
                )
                new_matches = [r[0] for r in match_result.result_rows]
                if not new_matches:
                    continue

                # Insert matches
                for article_id in new_matches:
                    client.command(
                        f"INSERT INTO {_DB}.search_matches (search_id, article_id) "
                        f"VALUES (%(sid)s, %(aid)s)",
                        parameters={"sid": search_id, "aid": article_id},
                    )

                _write_alert(
                    "search_match", "info",
                    f"Saved search '{label}' matched {len(new_matches)} new article(s)",
                    json.dumps({"search_id": search_id, "label": label, "match_count": len(new_matches)}),
                )
                log.info("Search '%s' matched %d new articles", label, len(new_matches))

                # Email notification for search matches
                try:
                    # Fetch article titles for the email
                    title_result = client.query(
                        f"SELECT title, url FROM {_DB}.articles FINAL "
                        f"WHERE article_id IN %(aids)s",
                        parameters={"aids": new_matches},
                    )
                    articles_html = "".join(
                        f'<li><a href="{r[1]}">{r[0]}</a></li>'
                        for r in title_result.result_rows
                    )
                    from newschat.email import send_alert_email
                    send_alert_email(
                        f"Saved search: {label} ({len(new_matches)} new)",
                        f"<h3>Saved search &ldquo;{label}&rdquo; matched {len(new_matches)} new article(s)</h3>"
                        f"<ul>{articles_html}</ul>",
                    )
                except Exception:
                    log.exception("Failed to send search match email")
        finally:
            client.close()
    except Exception:
        log.exception("Saved search matching failed")


def main():
    # --- Ingest today + yesterday (ClickHouse dedupes via ReplacingMergeTree) ---
    # Guardian API uses date granularity, so 1-day lookback is the minimum
    # useful window. 2 days covers the midnight boundary safely.
    # At 288 runs/day × ~1 page each = ~288 requests, well under 500/day limit.
    log.info("Starting ingestion...")
    try:
        from newschat.ingest.loader import ingest

        to_date = date.today()
        from_date = to_date - timedelta(days=1)
        result = ingest(from_date=from_date, to_date=to_date)
        log.info("Ingestion complete: %s", result)

        # Alert on liveblog updates
        if isinstance(result, dict) and result.get("new_blocks", 0) > 0:
            for aid, block_info in result.get("new_blocks_by_article", {}).items():
                titles = [t for t, _ in block_info if t]
                summary_text = "; ".join(titles[:5]) if titles else f"{len(block_info)} new updates"
                _write_alert(
                    "liveblog_update", "info",
                    f"Live blog updated: {len(block_info)} new block(s) — {summary_text}",
                    json.dumps({"article_id": aid, "new_block_count": len(block_info),
                                "block_titles": titles[:10]}),
                )

        # Check for zero articles
        if isinstance(result, dict) and result.get("articles_new") == 0:
            _write_alert(
                "ingestion_failure", "warning",
                "Ingestion returned 0 new articles",
                json.dumps({"from_date": str(from_date), "to_date": str(to_date)}),
            )
    except Exception as exc:
        log.exception("Ingestion failed")
        _write_alert(
            "ingestion_failure", "critical",
            f"Ingestion crashed: {exc}",
        )

    # Check for stale database
    _check_stale_db()

    # --- Enrich unenriched articles ---
    log.info("Starting enrichment coordinator...")
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "scripts/enrich_coordinator.py",
                "--model", "groq:qwen/qwen3-32b",
                "--batch", "50",
                "--workers", "1",
            ],
            cwd="/Users/simon/GitHub/nowthenews",
            timeout=4 * 60,  # 4 minute timeout (leaves 1min buffer before next run)
            capture_output=False,
        )
        log.info("Enrichment coordinator exited with code %d", proc.returncode)
        if proc.returncode != 0:
            _write_alert(
                "enrichment_crash", "critical",
                f"Enrichment coordinator exited with code {proc.returncode}",
                json.dumps({"returncode": proc.returncode}),
            )
    except subprocess.TimeoutExpired:
        log.warning("Enrichment coordinator timed out after 4 minutes")
        _write_alert(
            "enrichment_crash", "info",
            "Enrichment coordinator timed out after 4 minutes (will continue next run)",
        )
    except Exception as exc:
        log.exception("Enrichment coordinator failed")
        _write_alert(
            "enrichment_crash", "critical",
            f"Enrichment coordinator exception: {exc}",
        )

    # --- Match saved searches against newly ingested articles ---
    log.info("Running saved search matching...")
    _run_saved_search_matching()

    log.info("Done.")


if __name__ == "__main__":
    main()
