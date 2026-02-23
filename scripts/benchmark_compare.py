#!/usr/bin/env python3
"""Compare enrichment quality across models against the qwen3:30b-a3b baseline.

Usage:
    python scripts/benchmark_compare.py
    python scripts/benchmark_compare.py --json
"""

import argparse
import json
import sys

from newschat.config import CLICKHOUSE_DATABASE
from newschat.db import get_client
from newschat.enrich.prompt import PROMPT_VERSION

_DB = CLICKHOUSE_DATABASE
BASELINE_MODEL = "qwen3:30b-a3b"

BENCHMARK_IDS_SQL = f"""
    SELECT article_id FROM {_DB}.benchmark_reference
    ORDER BY cityHash64(article_id)
    LIMIT 50
"""


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def get_enrichments(ch, model, article_ids):
    """Fetch enrichment data for a model on the benchmark articles."""
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    rows = ch.query(f"""
        SELECT article_id, sentiment, sentiment_score, content_type, summary,
               entities.name, entities.type,
               smoke_terms.term
        FROM {_DB}.article_enrichment FINAL
        WHERE model_used = %(model)s AND prompt_version = %(pv)s
        AND article_id IN ({placeholders})
    """, parameters={"model": model, "pv": PROMPT_VERSION})

    cols = rows.column_names
    result = {}
    for row in rows.result_rows:
        d = dict(zip(cols, row))
        result[d["article_id"]] = d
    return result


def get_regions(ch, article_ids):
    """Fetch all regions for benchmark articles (all models share same table)."""
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    rows = ch.query(f"""
        SELECT article_id, region, score
        FROM {_DB}.article_regions FINAL
        WHERE article_id IN ({placeholders})
    """).result_rows
    result = {}
    for aid, region, score in rows:
        result.setdefault(aid, []).append((region, score))
    return result


def get_topics(ch, article_ids):
    """Fetch all topics for benchmark articles."""
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    rows = ch.query(f"""
        SELECT article_id, topic
        FROM {_DB}.article_topics FINAL
        WHERE article_id IN ({placeholders})
    """).result_rows
    result = {}
    for aid, topic in rows:
        result.setdefault(aid, []).append(topic)
    return result


def get_benchmark_regions(ch, article_ids):
    """Fetch baseline regions from the snapshot table."""
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    rows = ch.query(f"""
        SELECT article_id, region, score
        FROM {_DB}.benchmark_regions_ref
        WHERE article_id IN ({placeholders})
    """).result_rows
    result = {}
    for aid, region, score in rows:
        result.setdefault(aid, []).append((region, score))
    return result


def get_benchmark_topics(ch, article_ids):
    """Fetch baseline topics from the snapshot table."""
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    rows = ch.query(f"""
        SELECT article_id, topic
        FROM {_DB}.benchmark_topics_ref
        WHERE article_id IN ({placeholders})
    """).result_rows
    result = {}
    for aid, topic in rows:
        result.setdefault(aid, []).append(topic)
    return result


def get_speed(ch, model):
    """Get timing data from enrichment_log for benchmark runs."""
    rows = ch.query(f"""
        SELECT articles_attempted, articles_enriched, articles_failed
        FROM {_DB}.enrichment_log
        WHERE model = %(model)s AND status = 'benchmark'
        ORDER BY run_at DESC LIMIT 1
    """, parameters={"model": model}).result_rows
    if rows:
        return {"attempted": rows[0][0], "enriched": rows[0][1], "failed": rows[0][2]}
    return None


def compare_model(ch, model, baseline_data, baseline_regions, baseline_topics, article_ids):
    """Compare one model's output against the baseline."""
    model_data = get_enrichments(ch, model, article_ids)

    if not model_data:
        return None

    # Current regions/topics tables contain latest data for all models
    # For non-baseline models, regions/topics get overwritten since the tables
    # don't partition by model. So we compare what we can from enrichment table.
    model_regions = get_regions(ch, article_ids)
    model_topics = get_topics(ch, article_ids)

    common_ids = set(baseline_data.keys()) & set(model_data.keys())
    if not common_ids:
        return None

    # Metrics
    entity_overlaps = []
    sentiment_matches = 0
    content_type_matches = 0
    summary_length_ratios = []
    smoke_term_overlaps = []

    for aid in common_ids:
        base = baseline_data[aid]
        cand = model_data[aid]

        # Entity name overlap (case-insensitive)
        base_ents = {n.lower() for n in base["entities.name"]}
        cand_ents = {n.lower() for n in cand["entities.name"]}
        entity_overlaps.append(jaccard(base_ents, cand_ents))

        # Sentiment label match
        if base["sentiment"] == cand["sentiment"]:
            sentiment_matches += 1

        # Content type match
        if base["content_type"] == cand["content_type"]:
            content_type_matches += 1

        # Summary length ratio
        base_len = len(base["summary"]) if base["summary"] else 0
        cand_len = len(cand["summary"]) if cand["summary"] else 0
        if base_len > 0:
            summary_length_ratios.append(cand_len / base_len)

        # Smoke term overlap
        base_smoke = {t.lower() for t in base["smoke_terms.term"]}
        cand_smoke = {t.lower() for t in cand["smoke_terms.term"]}
        smoke_term_overlaps.append(jaccard(base_smoke, cand_smoke))

    # Topic overlap (from separate tables — compare against baseline snapshot)
    topic_overlaps = []
    for aid in common_ids:
        base_t = set(baseline_topics.get(aid, []))
        cand_t = set(model_topics.get(aid, []))
        topic_overlaps.append(jaccard(base_t, cand_t))

    # Region overlap (compare against baseline snapshot)
    region_overlaps = []
    for aid in common_ids:
        base_r = {r for r, s in baseline_regions.get(aid, []) if s >= 0.3}
        cand_r = {r for r, s in model_regions.get(aid, []) if s >= 0.3}
        region_overlaps.append(jaccard(base_r, cand_r))

    n = len(common_ids)
    return {
        "model": model,
        "articles_compared": n,
        "articles_enriched": len(model_data),
        "parse_success_pct": round(100 * len(model_data) / 50, 1),
        "entity_overlap": round(sum(entity_overlaps) / n, 3) if entity_overlaps else 0,
        "sentiment_agreement_pct": round(100 * sentiment_matches / n, 1),
        "content_type_agreement_pct": round(100 * content_type_matches / n, 1),
        "topic_overlap": round(sum(topic_overlaps) / n, 3) if topic_overlaps else 0,
        "region_overlap": round(sum(region_overlaps) / n, 3) if region_overlaps else 0,
        "smoke_term_overlap": round(sum(smoke_term_overlaps) / n, 3) if smoke_term_overlaps else 0,
        "avg_summary_length_ratio": round(
            sum(summary_length_ratios) / len(summary_length_ratios), 2
        ) if summary_length_ratios else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Compare benchmark enrichment quality")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    ch = get_client()

    # Get benchmark article IDs
    article_ids = [r[0] for r in ch.query(BENCHMARK_IDS_SQL).result_rows]

    # Get baseline data
    baseline_data = get_enrichments(ch, BASELINE_MODEL, article_ids)
    baseline_regions = get_benchmark_regions(ch, article_ids)
    baseline_topics = get_benchmark_topics(ch, article_ids)

    if not baseline_data:
        print("ERROR: No baseline enrichments found for", BASELINE_MODEL)
        sys.exit(1)

    # Find all models that have enrichment data for these articles
    placeholders = ", ".join(f"'{aid}'" for aid in article_ids)
    model_rows = ch.query(f"""
        SELECT model_used, count() as cnt
        FROM {_DB}.article_enrichment FINAL
        WHERE article_id IN ({placeholders}) AND prompt_version = %(pv)s
        GROUP BY model_used ORDER BY model_used
    """, parameters={"pv": PROMPT_VERSION}).result_rows

    models = [r[0] for r in model_rows if r[0] != BASELINE_MODEL]

    results = []
    for model in models:
        r = compare_model(ch, model, baseline_data, baseline_regions, baseline_topics, article_ids)
        if r:
            results.append(r)

    ch.close()

    if args.json:
        print(json.dumps(results, indent=2))
        return

    # Pretty-print markdown table
    print(f"\n## Benchmark Comparison (vs {BASELINE_MODEL} baseline, {len(article_ids)} articles)\n")
    print(f"Baseline: {len(baseline_data)} articles enriched\n")

    if not results:
        print("No candidate model data found yet. Run benchmark_run.py first.")
        return

    headers = ["Model", "N", "Parse%", "Entity", "Sent%", "Type%",
               "Topic", "Region", "Smoke", "SumLen"]
    widths = [25, 4, 6, 7, 6, 6, 7, 7, 7, 7]

    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |"
    sep_line = "|" + "|".join("-" * (w + 2) for w in widths) + "|"

    print(header_line)
    print(sep_line)

    for r in results:
        vals = [
            r["model"][:25],
            str(r["articles_compared"]),
            f"{r['parse_success_pct']:.0f}",
            f"{r['entity_overlap']:.3f}",
            f"{r['sentiment_agreement_pct']:.0f}",
            f"{r['content_type_agreement_pct']:.0f}",
            f"{r['topic_overlap']:.3f}",
            f"{r['region_overlap']:.3f}",
            f"{r['smoke_term_overlap']:.3f}",
            f"{r['avg_summary_length_ratio']:.2f}",
        ]
        print("| " + " | ".join(v.ljust(w) for v, w in zip(vals, widths)) + " |")

    print()
    print("Legend:")
    print("  N = articles compared, Parse% = successful parse rate")
    print("  Entity/Topic/Region/Smoke = Jaccard similarity vs baseline (1.0 = identical)")
    print("  Sent% = sentiment label agreement, Type% = content type agreement")
    print("  SumLen = avg summary length ratio vs baseline (1.0 = same length)")


if __name__ == "__main__":
    main()
