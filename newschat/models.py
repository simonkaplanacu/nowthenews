"""Shared data models for articles and tags."""

from dataclasses import dataclass, field, fields as dc_fields
from datetime import datetime
from typing import TypedDict


class Tag(TypedDict):
    """A keyword tag associated with an article."""

    tag_id: str
    tag_title: str
    tag_type: str


@dataclass
class LiveBlock:
    """A single block from a Guardian live blog."""

    article_id: str
    block_id: str
    title: str
    body_text: str
    published_at: datetime


@dataclass
class Article:
    """Normalised article ready for storage."""

    article_id: str
    source: str
    url: str
    title: str
    headline: str
    standfirst: str
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
    guardian_type: str = ""
    production_office: str = ""
    tags: list[Tag] = field(default_factory=list)


# --- Insertion helpers (single source of truth for column order) ---

_ARTICLE_NON_COLUMNS = frozenset({"tags"})


def article_column_names() -> list[str]:
    """Column names for the ClickHouse articles table, derived from Article fields."""
    return [f.name for f in dc_fields(Article) if f.name not in _ARTICLE_NON_COLUMNS]


def article_to_row(article: Article) -> list:
    """Convert an Article to a row of values matching article_column_names() order."""
    return [getattr(article, name) for name in article_column_names()]
