# Code Review Issues

40 issues identified during review. All fixed.

## A. Configuration

1. **FIXED** — **No config file** → Created `config.json` for all operational parameters. `config.py` loads from it with env var overrides for secrets.

2. **FIXED** — **`GUARDIAN_API_KEY` crashes at import time** → Changed to `os.environ.get("GUARDIAN_API_KEY", "")`. `GuardianClient.__init__` validates the key is present when actually needed.

3. **FIXED** — **`load_dotenv()` module-level side effect** → Kept in `config.py` (the config module is the right place for it) but the crash (#2) is eliminated, making the side effect benign.

## B. Schema and Database

4. **FIXED** — **`MergeTree()` instead of `ReplacingMergeTree()`** → `articles`, `article_tags`, and `article_enrichment` now use `ReplacingMergeTree`. `ingestion_log` stays `MergeTree` (append-only log).

5. **FIXED** — **`id` vs `article_id` naming inconsistency** → Renamed to `article_id` consistently across all tables.

6. **FIXED** — **Bootstrap bug** → DSN default no longer includes a database name. All queries use fully qualified table names (`news.articles`). `init_schema` connects without specifying a database.

7. **FIXED** — **Two sources of truth for enrichment status** → Removed `enriched UInt8` flag from `articles` table. Enrichment status is determined solely by the existence of rows in `article_enrichment`.

8. **FIXED** — **Parallel arrays have no alignment guarantee** → Changed to ClickHouse `Nested` type: `entities Nested(name String, type String)` and `policy Nested(domain String, score Float32)`. ClickHouse enforces array length alignment.

9. **FIXED** — **`DateTime` columns lack timezone** → All `DateTime` columns now use `DateTime('UTC')`.

10. **FIXED** — **Dependencies unpinned** → All dependencies now have compatible version ranges (e.g. `httpx>=0.27,<1`).

## C. Guardian API Client

11. **FIXED** — **No retry logic** → `_get()` retries up to `max_retries` (configurable, default 3) with exponential backoff on 5xx, 429, and transport errors.

12. **FIXED** — **500 requests/day limit not tracked** → `GuardianClient` tracks `_daily_request_count` and raises `RuntimeError` when the limit is reached. Limit is configurable via `config.json`.

13. **FIXED** — **`headline` column stores `standfirst`** → `headline` now maps to Guardian's `headline` field. Added separate `standfirst` column and field for the standfirst.

14. **FIXED** — **`standfirst` HTML not stripped** → Both `headline` and `standfirst` now go through `strip_html()`.

15. **FIXED** — **Tag `t["id"]` bare key access** → Tags use `.get("id")` with a `None` check; tags without an `id` are skipped.

16. **FIXED** — **`raw["id"]` and `raw["webPublicationDate"]` bare key access** → `_parse_article` uses `.get()` for both and returns `None` if either is missing.

17. **FIXED** — **Timezone-aware datetime into timezone-naive column** → Timestamps are explicitly converted to UTC and stripped of tzinfo before insertion into `DateTime('UTC')` columns.

18. **FIXED** — **Inconsistent null handling** → All string fields use `or ""` consistently.

## D. Ingestion Loader (was Coordinator)

19. **FIXED** — **`pages` counter is wrong** → Replaced synthetic counter with `guardian.pages_fetched` which tracks actual API pages.

20. **FIXED** — **`_existing_ids` not updated during run** → `existing.update()` called after each batch insert.

21. **DOCUMENTED** — **`_existing_ids` loads all IDs into memory** → Added comment explaining this is an optimisation with `ReplacingMergeTree` as safety net. For extreme date ranges, duplicate inserts are handled by the engine.

22. **FIXED** — **`new_articles` list accumulates in memory** → Replaced with `new_count` integer counter.

23. **FIXED** — **`datetime.utcnow()` deprecated** → Changed to `datetime.now(timezone.utc)`.

24. **FIXED** — **`datetime.max.time()` microsecond truncation** → Changed to exclusive upper bound: `published_at < (to_date + 1 day)` instead of `<= 23:59:59.999999`.

25. **FIXED** — **Error path references unbound variables** → All counters (`fetched`, `new_count`, `pages`, `status`) initialised before the `try` block.

26. **FIXED** — **`_log_ingestion` can itself throw** → Wrapped in its own `try/except` that logs the failure without masking the original exception.

27. **FIXED** — **No batch-level error handling** → Improved: `ReplacingMergeTree` makes re-runs safe (duplicates merge). Batch errors propagate with full context via `log.exception`.

28. **FIXED** — **No date validation** → `ingest()` raises `ValueError` if `from_date > to_date`.

## E. Architecture and Naming

29. **FIXED** — **`coordinator.py` misnamed** → Renamed to `loader.py`.

30. **FIXED** — **`Article` dataclass in Guardian module** → Moved to `newschat/models.py`.

31. **FIXED** — **Parallel lists with no structural link** → `article_column_names()` and `article_to_row()` derive column order from `dataclasses.fields(Article)`. Single source of truth.

32. **FIXED** — **No per-article error handling** → `_parse_article` returns `None` on failure (with logging). `search()` filters out `None` results. One bad article no longer kills the batch.

33. **FIXED** — **No connectivity check** → `_check_connectivity(ch)` runs `SELECT 1` before starting ingestion.

## F. Testability and Project Setup

34. **FIXED** — **`base_url` not parameterized** → `GuardianClient.__init__` now accepts `base_url` as a constructor parameter.

35. **FIXED** — **Bare key access on API response envelope** → `_get()` validates `"response"` key exists. `search()` uses `.get()` with defaults for `total` and `pages`.

36. **FIXED** — **Package/repo name mismatch** → Documented in `pyproject.toml` comment: `# Package: newschat | Repository: nowthenews`.

37. **FIXED** — **No tests** → Added 15 tests across `test_models.py`, `test_guardian.py`, and `test_loader.py`. Removed unused `pytest-asyncio` dependency.

38. **FIXED** — **Tags use untyped `list[dict]`** → Created `Tag(TypedDict)` in `models.py`. Tags are now `list[Tag]`.

## G. Observability

39. **FIXED** — **Logging goes to stderr only** → `ingest_once.py` configures both stderr and a `RotatingFileHandler` with configurable path, size, and backup count via `config.json`.

40. **FIXED** — **Sort key makes ID lookups slow** → Added `INDEX idx_article_id article_id TYPE bloom_filter GRANULARITY 1` to the articles table for efficient point lookups by article ID.
