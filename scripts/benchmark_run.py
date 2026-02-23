#!/usr/bin/env python3
"""Run enrichment benchmark — enrich a fixed 50-article sample with multiple models.

Usage:
    python scripts/benchmark_run.py                     # run all candidates
    python scripts/benchmark_run.py --model qwen2.5:7b  # run one model
    python scripts/benchmark_run.py --list               # show status
"""

import argparse
import logging
import shutil
import subprocess
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from newschat.config import (
    CLICKHOUSE_DATABASE,
    LOG_BACKUP_COUNT,
    LOG_FILE,
    LOG_LEVEL,
    LOG_MAX_BYTES,
)
from newschat.db import get_client
from newschat.enrich.llm import OllamaClient
from newschat.enrich.pipeline import _store_enrichment
from newschat.enrich.prompt import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt

_DB = CLICKHOUSE_DATABASE
log = logging.getLogger(__name__)

# The 50 benchmark article IDs — deterministic sample from the 351 qwen3 enrichments
BENCHMARK_IDS_SQL = f"""
    SELECT article_id FROM {_DB}.benchmark_reference
    ORDER BY cityHash64(article_id)
    LIMIT 50
"""

def ensure_ollama():
    """Check if Ollama is running; start it if not."""
    ollama = shutil.which("ollama")
    if not ollama:
        raise RuntimeError("ollama not found on PATH")
    try:
        subprocess.run([ollama, "list"], capture_output=True, timeout=5, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        print("Ollama not running — starting it...")
        subprocess.Popen([ollama, "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        subprocess.run([ollama, "list"], capture_output=True, timeout=10, check=True)
        print("Ollama started.")


CANDIDATE_MODELS = [
    "qwen2.5:7b",
    "llama3.1:8b",
    "phi4:latest",
    "gemma3:27b",
    "mistral:7b",
]


def setup_logging():
    log_path = Path(LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(fmt)
    root.addHandler(stderr_handler)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


def get_benchmark_articles(ch):
    """Fetch the 50 benchmark articles with their text."""
    ids = [r[0] for r in ch.query(BENCHMARK_IDS_SQL).result_rows]
    placeholders = ", ".join(f"'{aid}'" for aid in ids)
    rows = ch.query(f"""
        SELECT article_id, title, headline, byline,
               toString(published_at), body_text
        FROM {_DB}.articles FINAL
        WHERE article_id IN ({placeholders})
    """).result_rows
    return rows


def already_enriched(ch, model):
    """Return set of article_ids already enriched by this model+prompt."""
    ids = [r[0] for r in ch.query(BENCHMARK_IDS_SQL).result_rows]
    placeholders = ", ".join(f"'{aid}'" for aid in ids)
    rows = ch.query(f"""
        SELECT article_id FROM {_DB}.article_enrichment FINAL
        WHERE model_used = %(model)s AND prompt_version = %(pv)s
        AND article_id IN ({placeholders})
    """, parameters={"model": model, "pv": PROMPT_VERSION}).result_rows
    return {r[0] for r in rows}


def run_model(model: str):
    """Enrich 50 benchmark articles with the given model. Returns summary dict."""
    ch = get_client()
    llm = OllamaClient(model=model)

    if not llm.check_health():
        llm.close()
        ch.close()
        raise RuntimeError(f"Model '{model}' not available")

    articles = get_benchmark_articles(ch)
    done = already_enriched(ch, model)
    todo = [a for a in articles if a[0] not in done]

    print(f"\n{'='*60}")
    print(f"Model: {model}")
    print(f"Articles: {len(articles)} total, {len(done)} already done, {len(todo)} remaining")
    print(f"{'='*60}")

    if not todo:
        print("All 50 already enriched — skipping.")
        llm.close()
        ch.close()
        return {"model": model, "enriched": 0, "failed": 0, "skipped": len(done),
                "total_seconds": 0, "avg_seconds": 0}

    enriched = 0
    failed = 0
    start = time.time()

    for i, (article_id, title, headline, byline, published_at, body_text) in enumerate(todo):
        t0 = time.time()
        try:
            user_prompt = build_user_prompt(
                title=title, headline=headline, byline=byline,
                published_at=published_at, body_text=body_text,
            )
            result = llm.enrich(system=SYSTEM_PROMPT, user=user_prompt)
            _store_enrichment(ch, article_id, result, model)
            enriched += 1
            elapsed = time.time() - t0
            print(f"  [{enriched+failed}/{len(todo)}] {article_id[:50]}... {elapsed:.1f}s OK")
        except Exception as e:
            failed += 1
            elapsed = time.time() - t0
            print(f"  [{enriched+failed}/{len(todo)}] {article_id[:50]}... {elapsed:.1f}s FAIL: {e}")

    total = time.time() - start
    avg = total / max(enriched + failed, 1)

    # Log to enrichment_log
    try:
        ch.insert(
            f"{_DB}.enrichment_log",
            [[model, PROMPT_VERSION, datetime.now(timezone.utc),
              enriched + failed, enriched, failed, "benchmark"]],
            column_names=["model", "prompt_version", "run_at",
                          "articles_attempted", "articles_enriched",
                          "articles_failed", "status"],
        )
    except Exception:
        pass

    llm.close()
    ch.close()

    summary = {
        "model": model,
        "enriched": enriched,
        "failed": failed,
        "skipped": len(done),
        "total_seconds": round(total, 1),
        "avg_seconds": round(avg, 1),
    }
    print(f"\nResult: {summary}")
    return summary


def show_status():
    """Show which models have benchmark enrichments."""
    ch = get_client()
    ids = [r[0] for r in ch.query(BENCHMARK_IDS_SQL).result_rows]
    placeholders = ", ".join(f"'{aid}'" for aid in ids)
    rows = ch.query(f"""
        SELECT model_used, count() as cnt
        FROM {_DB}.article_enrichment FINAL
        WHERE article_id IN ({placeholders})
        AND prompt_version = %(pv)s
        GROUP BY model_used
        ORDER BY model_used
    """, parameters={"pv": PROMPT_VERSION}).result_rows
    ch.close()

    print("\nBenchmark enrichment status (50 articles):")
    print(f"{'Model':<25} {'Count':>5}")
    print("-" * 32)
    for model, cnt in rows:
        print(f"{model:<25} {cnt:>5}")


def main():
    parser = argparse.ArgumentParser(description="Run enrichment benchmark")
    parser.add_argument("--model", help="Run a single model (default: all candidates)")
    parser.add_argument("--list", action="store_true", help="Show current status")
    args = parser.parse_args()

    setup_logging()
    ensure_ollama()

    if args.list:
        show_status()
        return

    models = [args.model] if args.model else CANDIDATE_MODELS
    results = []

    for model in models:
        try:
            r = run_model(model)
            results.append(r)
        except Exception as e:
            print(f"FAILED: {model} — {e}")
            results.append({"model": model, "error": str(e)})

    print(f"\n{'='*60}")
    print("BENCHMARK COMPLETE")
    print(f"{'='*60}")
    print(f"{'Model':<25} {'OK':>4} {'Fail':>4} {'Skip':>4} {'Total(s)':>9} {'Avg(s)':>7}")
    print("-" * 60)
    for r in results:
        if "error" in r:
            print(f"{r['model']:<25} ERROR: {r['error']}")
        else:
            print(f"{r['model']:<25} {r['enriched']:>4} {r['failed']:>4} "
                  f"{r['skipped']:>4} {r['total_seconds']:>9.1f} {r['avg_seconds']:>7.1f}")


if __name__ == "__main__":
    main()
