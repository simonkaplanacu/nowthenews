# Code Review Issues

38 distinct issues identified during review.

## A. Configuration

1. **No config file** — operational parameters are hardcoded across four modules (`config.py`, `guardian.py`, `coordinator.py`, `db.py`). Rate limits, batch sizes, page sizes, timeouts, API field lists, the database name `news`, and the source string `"guardian"` are all string/number literals scattered through the code with no single source of truth.

2. **`GUARDIAN_API_KEY = os.environ["GUARDIAN_API_KEY"]` crashes at import time** (`config.py:6`) — uses bare `os.environ[]` instead of `os.getenv()`. Since `db.py` imports `config.py`, even `setup_db.py` (which doesn't need the Guardian key) crashes without it.

3. **`load_dotenv()` runs as a module-level side effect** (`config.py:4`) — fires on any import of any module in the package, makes behavior hard to control in tests.

## B. Schema and Database

4. **`MergeTree()` instead of `ReplacingMergeTree()`** (`db.py:29,37,53,65`) — all four tables use plain `MergeTree`, providing no storage-level dedup. If application-level dedup fails, duplicates accumulate permanently.

5. **`id` vs `article_id` naming inconsistency** — the `articles` table has column `id`, but `article_tags` and `article_enrichment` reference it as `article_id`. No enforced relationship between them.

6. **Bootstrap bug — `setup_db.py` can't create the database it needs to connect to** (`db.py:72`, `config.py:9`) — the DSN defaults to `clickhouse://localhost:9000/news`, but `init_schema()` needs to run `CREATE DATABASE IF NOT EXISTS news` first. On a fresh install, the connection to the non-existent `news` database may fail before the DDL can execute.

7. **Two sources of truth for enrichment status** (`db.py:28,40-54`) — the `enriched UInt8` flag on `articles` and the existence of rows in `article_enrichment` can diverge. ClickHouse has no cross-table transactions.

8. **`article_enrichment` parallel arrays have no alignment guarantee** (`db.py:43-46`) — `entities[i]` must correspond to `entity_types[i]`, `policy_domains[i]` to `policy_scores[i]`. Nothing enforces this.

9. **`DateTime` columns lack timezone specification** (`db.py:22,27,42`) — `published_at`, `ingested_at`, `enriched_at` are all bare `DateTime`, interpreted in server-local timezone.

10. **Dependencies unpinned** (`pyproject.toml:6-11`) — no version constraints on any dependency. A breaking upstream release silently breaks the project.

## C. Guardian API Client

11. **No retry logic for transient HTTP errors** (`guardian.py:115-116`) — a single 429, 500, or 503 kills the entire run. For a backfill taking hours, one transient failure at the end means starting over.

12. **500 requests/day limit not tracked** (`guardian.py:12`) — the comment documents the limit but nothing enforces it. A large backfill easily exceeds it.

13. **`headline` column stores `standfirst`, not headlines** (`guardian.py:75`) — `headline=fields.get("standfirst", "")`. The actual Guardian `headline` field is requested in `_SHOW_FIELDS` (line 16) but never read. The column named "headline" contains subtitle/summary text.

14. **`standfirst` HTML not stripped** (`guardian.py:75`) — `body_text` goes through `strip_html()`, but standfirst (stored as `headline`) is stored with raw HTML tags.

15. **Tag `t["id"]` bare key access** (`guardian.py:63`) — `t["id"]` will `KeyError` if any Guardian tag lacks an `id` field. Other tag fields use `.get()` with defaults.

16. **`raw["id"]` and `raw["webPublicationDate"]` bare key access** (`guardian.py:71,82`) — one malformed article missing either field crashes the entire page parse. No per-article error handling in the list comprehension at line 154.

17. **`published_at` timezone-aware datetime inserted into timezone-naive column** (`guardian.py:81-83`) — `datetime.fromisoformat()` produces a timezone-aware datetime, but ClickHouse `DateTime` is naive. Behaviour depends on driver interpretation.

18. **Inconsistent null handling** (`guardian.py:73-87`) — some fields use `or ""` guard (lines 75,77,86,87), others don't (lines 73,78,79). If the Guardian API returns explicit `null` for `webUrl` or `sectionId`, they'd be stored as `None`.

## D. Ingestion Coordinator

19. **`pages` counter is wrong** (`coordinator.py:123-124,130`) — `if fetched % 200 == 0: pages += 1` has nothing to do with actual API pages. It's an inaccurate synthetic counter.

20. **`_existing_ids` not updated during the run** (`coordinator.py:93,113`) — the dedup set is built once at the start. Articles inserted during the current run aren't added to it, so if the Guardian API returns the same article twice (e.g., across page boundaries), it gets inserted twice.

21. **`_existing_ids` loads all IDs into memory** (`coordinator.py:12-23`) — for a large date range, this could be hundreds of thousands of IDs in a Python set.

22. **`new_articles` list accumulates all articles in memory** (`coordinator.py:100,115`) — keeps full `Article` objects (with body text) forever, only used for `len()`. An integer counter would suffice.

23. **`datetime.utcnow()` is deprecated** (`coordinator.py:70`) — deprecated since Python 3.12. Returns a naive datetime. Should use `datetime.now(timezone.utc)`.

24. **`datetime.max.time()` microsecond truncation** (`coordinator.py:20`) — produces `23:59:59.999999`, but ClickHouse `DateTime` truncates to second precision. Articles published at exactly `23:59:59.999999` on the boundary date could be missed.

25. **Error path references potentially unbound variables** (`coordinator.py:133-136`) — if `_existing_ids` raises before `fetched`, `new_articles`, or `pages` are assigned, the `_log_ingestion` call in `except` raises `NameError`, masking the original error.

26. **`_log_ingestion` in the error path can itself throw** (`coordinator.py:136`) — if ClickHouse is unreachable, logging the error fails, potentially masking the original exception.

27. **No batch-level error handling or resume capability** (`coordinator.py:118-121`) — if one batch insert fails, everything already inserted stays but there's no record of where to resume from.

28. **No date validation** (`ingest_once.py:26-27`) — reversed dates (`from > to`) cause `_existing_ids` to return an empty set, silently breaking dedup and inserting duplicates.

## E. Architecture and Naming

29. **`coordinator.py` is misnamed** — it performs a linear single-source ingest. No coordination, no orchestration, no multi-source or enrichment wiring. Should be `loader.py` or `ingest.py`.

30. **`Article` dataclass lives in the Guardian-specific module** (`guardian.py:20-38`) — it's the general data model, imported by the coordinator. When a second source is added, importing from `guardian.py` is wrong.

31. **Article rows and column names are parallel lists with no structural link** (`coordinator.py:32-50`) — two independent 15-element lists that must stay in sync. Same lack of discipline that produced issue #5.

32. **No per-article error handling** (`guardian.py:154`) — one malformed article in a page of 200 crashes the list comprehension, losing the entire page and killing the run.

33. **No connectivity check before starting** — a long backfill can fetch hundreds of API pages before discovering ClickHouse is unreachable at the first insert attempt.

## F. Testability and Project Setup

34. **`GuardianClient.base_url` is not parameterized** (`guardian.py:97`) — `api_key` is a constructor parameter with a default, so it can be overridden. `base_url` is not — it's always `GUARDIAN_BASE_URL`. Can't point the client at a mock server for testing without monkeypatching the module constant.

35. **Bare key access on API response envelope** (`guardian.py:152,155`) — `data["response"]`, `response["total"]`, `response["pages"]` all `KeyError` if the Guardian API returns an error body with a different structure. Distinct from #16 (article-level fields) — this is the response envelope itself.

36. **Package name doesn't match repo name** (`pyproject.toml:2`) — the installable package is `newschat`, the repository is `nowthenews`. Anyone looking at one won't find the other.

37. **No tests exist** (`pyproject.toml:15`) — dev dependencies include `pytest` and `pytest-asyncio`, but there are zero test files in the project. `pytest-asyncio` is doubly dead since there's no async code either.

38. **Tags use untyped `list[dict]`** (`guardian.py:38`) — the tag structure has keys `tag_id`, `tag_title`, `tag_type` but no TypedDict or dataclass. Misspelling a key produces no static warning; it fails at runtime during ClickHouse insert.
