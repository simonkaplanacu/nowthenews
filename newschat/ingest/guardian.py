"""Guardian API client — fetch and normalise articles."""

import logging
import time
from datetime import date, datetime, timezone
from typing import Generator

import httpx
from bs4 import BeautifulSoup

from newschat.config import (
    GUARDIAN_API_KEY,
    GUARDIAN_BASE_URL,
    GUARDIAN_DAILY_LIMIT,
    GUARDIAN_MAX_RETRIES,
    GUARDIAN_PAGE_SIZE,
    GUARDIAN_RATE_LIMIT_INTERVAL,
    GUARDIAN_SHOW_FIELDS,
    GUARDIAN_SHOW_TAGS,
    GUARDIAN_TIMEOUT,
)
from newschat.models import Article, Tag

log = logging.getLogger(__name__)


def strip_html(html: str | None) -> str:
    """Strip HTML tags, return plain text."""
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)


def _parse_article(raw: dict) -> Article | None:
    """Convert a raw Guardian API result into a normalised Article.

    Returns None if the article cannot be parsed (logs a warning).
    """
    try:
        article_id = raw.get("id")
        if not article_id:
            log.warning("Article missing id, skipping")
            return None

        pub_date_str = raw.get("webPublicationDate")
        if not pub_date_str:
            log.warning("Article missing webPublicationDate, skipping: %s", article_id)
            return None

        fields = raw.get("fields") or {}
        pillar_id = raw.get("pillarId") or ""
        pillar = pillar_id.split("/")[-1] if pillar_id else ""

        word_count_str = fields.get("wordcount") or "0"
        try:
            word_count = int(word_count_str)
        except (ValueError, TypeError):
            word_count = 0

        tags: list[Tag] = []
        for t in raw.get("tags") or []:
            tag_id = t.get("id")
            if not tag_id:
                continue
            tags.append(
                Tag(
                    tag_id=tag_id,
                    tag_title=t.get("webTitle") or "",
                    tag_type=t.get("type") or "keyword",
                )
            )

        # Parse timestamp, convert to naive UTC for ClickHouse DateTime('UTC')
        published_at = (
            datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .replace(tzinfo=None)
        )

        return Article(
            article_id=article_id,
            source="guardian",
            url=raw.get("webUrl") or "",
            title=raw.get("webTitle") or "",
            headline=strip_html(fields.get("headline") or ""),
            standfirst=strip_html(fields.get("standfirst") or ""),
            body_text=strip_html(fields.get("body") or ""),
            byline=fields.get("byline") or "",
            section_id=raw.get("sectionId") or "",
            section_name=raw.get("sectionName") or "",
            pillar=pillar,
            published_at=published_at,
            word_count=word_count,
            lang=fields.get("lang") or "en",
            short_url=fields.get("shortUrl") or "",
            thumbnail_url=fields.get("thumbnail") or "",
            guardian_type=raw.get("type") or "",
            production_office=fields.get("productionOffice") or "",
            tags=tags,
        )
    except Exception:
        log.exception("Failed to parse article: %s", raw.get("id", "unknown"))
        return None


class GuardianClient:
    """Client for the Guardian Content API with rate limiting, retry, and
    daily budget tracking."""

    def __init__(
        self,
        api_key: str = GUARDIAN_API_KEY,
        base_url: str = GUARDIAN_BASE_URL,
    ):
        if not api_key:
            raise ValueError(
                "Guardian API key is required. "
                "Set GUARDIAN_API_KEY in .env or environment."
            )
        self.api_key = api_key
        self.base_url = base_url
        self._last_request_time: float = 0
        self._daily_request_count: int = 0
        self._client = httpx.Client(timeout=GUARDIAN_TIMEOUT)
        self.pages_fetched: int = 0

    def _rate_limit(self):
        """Enforce per-second and daily rate limits."""
        if self._daily_request_count >= GUARDIAN_DAILY_LIMIT:
            raise RuntimeError(
                f"Daily Guardian API limit reached ({GUARDIAN_DAILY_LIMIT} requests). "
                "Resume tomorrow or use a higher-tier API key."
            )
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < GUARDIAN_RATE_LIMIT_INTERVAL:
            time.sleep(GUARDIAN_RATE_LIMIT_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make a rate-limited GET request with retry on transient errors."""
        self._rate_limit()
        params = dict(params) if params else {}
        params["api-key"] = self.api_key
        url = f"{self.base_url}{endpoint}"

        last_exc: Exception | None = None
        for attempt in range(1, GUARDIAN_MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                resp.raise_for_status()
                self._daily_request_count += 1
                data = resp.json()
                if "response" not in data:
                    raise ValueError(
                        "Guardian API returned unexpected structure (no 'response' key)"
                    )
                return data
            except (httpx.HTTPStatusError, httpx.TransportError, ValueError) as exc:
                last_exc = exc
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and exc.response.status_code < 500
                    and exc.response.status_code != 429
                ):
                    raise
                if attempt < GUARDIAN_MAX_RETRIES:
                    wait = 2**attempt
                    log.warning(
                        "Guardian API request failed (attempt %d/%d), "
                        "retrying in %ds: %s",
                        attempt,
                        GUARDIAN_MAX_RETRIES,
                        wait,
                        exc,
                    )
                    time.sleep(wait)

        raise RuntimeError(
            f"Guardian API request failed after {GUARDIAN_MAX_RETRIES} attempts"
        ) from last_exc

    def search(
        self,
        query: str | None = None,
        section: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        page: int = 1,
        page_size: int = GUARDIAN_PAGE_SIZE,
        order_by: str = "newest",
    ) -> tuple[list[Article], int, int]:
        """Search for articles.

        Returns:
            (articles, total_results, total_pages)
        """
        params: dict = {
            "show-fields": GUARDIAN_SHOW_FIELDS,
            "show-tags": GUARDIAN_SHOW_TAGS,
            "order-by": order_by,
            "page": page,
            "page-size": page_size,
        }
        if query:
            params["q"] = query
        if section:
            params["section"] = section
        if from_date:
            params["from-date"] = from_date.isoformat()
        if to_date:
            params["to-date"] = to_date.isoformat()

        data = self._get("/search", params)
        response = data["response"]

        articles = []
        for r in response.get("results") or []:
            article = _parse_article(r)
            if article is not None:
                articles.append(article)

        total = response.get("total", 0)
        pages = response.get("pages", 0)
        return articles, total, pages

    def get_article(self, article_id: str) -> Article | None:
        """Fetch a single article by its ID."""
        params = {
            "show-fields": GUARDIAN_SHOW_FIELDS,
            "show-tags": GUARDIAN_SHOW_TAGS,
        }
        data = self._get(f"/{article_id}", params)
        content = data["response"].get("content")
        if not content:
            return None
        return _parse_article(content)

    def fetch_all(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
        section: str | None = None,
        query: str | None = None,
        order_by: str = "oldest",
    ) -> Generator[Article, None, None]:
        """Generator that yields all articles matching the query, handling pagination.

        Uses oldest-first ordering by default for backfill.
        Sets self.pages_fetched as a side effect.
        """
        self.pages_fetched = 0
        page = 1
        while True:
            articles, total, total_pages = self.search(
                query=query,
                section=section,
                from_date=from_date,
                to_date=to_date,
                page=page,
                order_by=order_by,
            )
            self.pages_fetched = page
            for article in articles:
                yield article

            if page >= total_pages:
                break
            page += 1

    def sections(self) -> list[dict]:
        """Fetch all Guardian sections."""
        data = self._get("/sections")
        return data["response"].get("results") or []

    @property
    def daily_requests_remaining(self) -> int:
        return max(0, GUARDIAN_DAILY_LIMIT - self._daily_request_count)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
