"""Guardian API client — fetch and normalise articles."""

import time
from dataclasses import dataclass, field
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from newschat.config import GUARDIAN_API_KEY, GUARDIAN_BASE_URL

# Rate limit: 1 request/second, 500 requests/day
_MIN_REQUEST_INTERVAL = 1.0

# Fields we request from the API
_SHOW_FIELDS = "headline,standfirst,body,byline,wordcount,thumbnail,shortUrl,lang"
_SHOW_TAGS = "keyword"


@dataclass
class Article:
    """Normalised article ready for ClickHouse insertion."""
    id: str
    source: str
    url: str
    title: str
    headline: str
    body_text: str
    byline: str
    section_id: str
    section_name: str
    pillar: str
    published_at: datetime
    word_count: int
    lang: str
    short_url: str
    thumbnail_url: str
    tags: list[dict] = field(default_factory=list)


def strip_html(html: str) -> str:
    """Strip HTML tags, return plain text."""
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)


def _parse_article(raw: dict) -> Article:
    """Convert a raw Guardian API result into a normalised Article."""
    fields = raw.get("fields", {})
    pillar_id = raw.get("pillarId", "")
    # pillarId is like "pillar/news" — extract the short name
    pillar = pillar_id.split("/")[-1] if pillar_id else ""

    word_count_str = fields.get("wordcount", "0")
    try:
        word_count = int(word_count_str)
    except (ValueError, TypeError):
        word_count = 0

    tags = [
        {
            "tag_id": t["id"],
            "tag_title": t.get("webTitle", ""),
            "tag_type": t.get("type", "keyword"),
        }
        for t in raw.get("tags", [])
    ]

    return Article(
        id=raw["id"],
        source="guardian",
        url=raw.get("webUrl", ""),
        title=raw.get("webTitle", ""),
        headline=fields.get("standfirst", "") or "",
        body_text=strip_html(fields.get("body", "")),
        byline=fields.get("byline", "") or "",
        section_id=raw.get("sectionId", ""),
        section_name=raw.get("sectionName", ""),
        pillar=pillar,
        published_at=datetime.fromisoformat(
            raw["webPublicationDate"].replace("Z", "+00:00")
        ),
        word_count=word_count,
        lang=fields.get("lang", "en"),
        short_url=fields.get("shortUrl", "") or "",
        thumbnail_url=fields.get("thumbnail", "") or "",
        tags=tags,
    )


class GuardianClient:
    """Client for the Guardian Content API with rate limiting."""

    def __init__(self, api_key: str = GUARDIAN_API_KEY):
        self.api_key = api_key
        self.base_url = GUARDIAN_BASE_URL
        self._last_request_time: float = 0
        self._client = httpx.Client(timeout=30)

    def _rate_limit(self):
        """Enforce 1 request/second rate limit."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make a rate-limited GET request to the Guardian API."""
        self._rate_limit()
        params = params or {}
        params["api-key"] = self.api_key
        url = f"{self.base_url}{endpoint}"
        resp = self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def search(
        self,
        query: str | None = None,
        section: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        page: int = 1,
        page_size: int = 200,
        order_by: str = "newest",
    ) -> tuple[list[Article], int, int]:
        """
        Search for articles.

        Returns:
            (articles, total_results, total_pages)
        """
        params: dict = {
            "show-fields": _SHOW_FIELDS,
            "show-tags": _SHOW_TAGS,
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

        articles = [_parse_article(r) for r in response.get("results", [])]
        return articles, response["total"], response["pages"]

    def get_article(self, article_id: str) -> Article:
        """Fetch a single article by its ID."""
        params = {
            "show-fields": _SHOW_FIELDS,
            "show-tags": _SHOW_TAGS,
        }
        data = self._get(f"/{article_id}", params)
        return _parse_article(data["response"]["content"])

    def fetch_all(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
        section: str | None = None,
        query: str | None = None,
        order_by: str = "oldest",
    ):
        """
        Generator that yields all articles matching the query, handling pagination.

        Yields Article objects one at a time. Uses oldest-first ordering by default
        for backfill so we can resume from where we left off.
        """
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
            for article in articles:
                yield article

            if page >= total_pages:
                break
            page += 1

    def sections(self) -> list[dict]:
        """Fetch all Guardian sections."""
        data = self._get("/sections")
        return data["response"]["results"]

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
