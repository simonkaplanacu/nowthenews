"""Tests for shared data models."""

from datetime import datetime, timezone

from newschat.models import Article, Tag, article_column_names, article_to_row


def _make_article(**overrides) -> Article:
    defaults = dict(
        article_id="test/2024/article",
        source="guardian",
        url="https://example.com/article",
        title="Test Title",
        headline="Test Headline",
        standfirst="Test standfirst",
        body_text="Article body text",
        byline="Test Author",
        section_id="test-section",
        section_name="Test Section",
        pillar="news",
        published_at=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
        word_count=100,
        lang="en",
        short_url="https://short.url/abc",
        thumbnail_url="https://example.com/thumb.jpg",
        tags=[Tag(tag_id="keyword/test", tag_title="test", tag_type="keyword")],
    )
    defaults.update(overrides)
    return Article(**defaults)


def test_article_to_row_length_matches_columns():
    article = _make_article()
    assert len(article_to_row(article)) == len(article_column_names())


def test_article_to_row_values_match_columns():
    article = _make_article()
    row = article_to_row(article)
    col_map = dict(zip(article_column_names(), row))
    assert col_map["article_id"] == "test/2024/article"
    assert col_map["source"] == "guardian"
    assert col_map["title"] == "Test Title"
    assert col_map["headline"] == "Test Headline"
    assert col_map["standfirst"] == "Test standfirst"


def test_article_columns_excludes_tags():
    assert "tags" not in article_column_names()


def test_tag_typed_dict():
    tag = Tag(tag_id="keyword/test", tag_title="test", tag_type="keyword")
    assert tag["tag_id"] == "keyword/test"
    assert tag["tag_title"] == "test"
    assert tag["tag_type"] == "keyword"
