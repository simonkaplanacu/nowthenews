"""Tests for Guardian API client parsing."""

from newschat.ingest.guardian import _parse_article, strip_html


def test_strip_html_removes_tags():
    assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"


def test_strip_html_handles_none():
    assert strip_html(None) == ""


def test_strip_html_handles_empty():
    assert strip_html("") == ""


def _raw_article(**overrides) -> dict:
    base = {
        "id": "test/2024/jan/15/article",
        "webUrl": "https://www.theguardian.com/test",
        "webTitle": "Test Article",
        "webPublicationDate": "2024-01-15T10:30:00Z",
        "sectionId": "test",
        "sectionName": "Test",
        "pillarId": "pillar/news",
        "fields": {
            "headline": "The Headline",
            "standfirst": "<p>The summary</p>",
            "body": "<p>Article body</p>",
            "byline": "Author Name",
            "wordcount": "500",
            "lang": "en",
            "shortUrl": "https://gu.com/abc",
            "thumbnail": "https://example.com/thumb.jpg",
        },
        "tags": [
            {"id": "keyword/test", "webTitle": "test", "type": "keyword"},
        ],
    }
    base.update(overrides)
    return base


def test_parse_article_valid():
    article = _parse_article(_raw_article())
    assert article is not None
    assert article.article_id == "test/2024/jan/15/article"
    assert article.title == "Test Article"
    assert article.headline == "The Headline"
    assert article.standfirst == "The summary"  # HTML stripped
    assert article.body_text == "Article body"  # HTML stripped
    assert article.word_count == 500
    assert len(article.tags) == 1
    assert article.tags[0]["tag_id"] == "keyword/test"


def test_parse_article_missing_id_returns_none():
    raw = _raw_article()
    del raw["id"]
    assert _parse_article(raw) is None


def test_parse_article_missing_date_returns_none():
    raw = _raw_article()
    del raw["webPublicationDate"]
    assert _parse_article(raw) is None


def test_parse_article_tag_missing_id_skipped():
    raw = _raw_article(
        tags=[
            {"webTitle": "no-id-tag", "type": "keyword"},
            {"id": "keyword/valid", "webTitle": "valid", "type": "keyword"},
        ]
    )
    article = _parse_article(raw)
    assert article is not None
    assert len(article.tags) == 1
    assert article.tags[0]["tag_id"] == "keyword/valid"


def test_parse_article_null_fields_do_not_crash():
    raw = _raw_article()
    raw["webUrl"] = None
    raw["webTitle"] = None
    raw["sectionId"] = None
    raw["sectionName"] = None
    raw["pillarId"] = None
    raw["fields"] = {
        "headline": None,
        "standfirst": None,
        "body": None,
        "byline": None,
        "wordcount": None,
        "lang": None,
        "shortUrl": None,
        "thumbnail": None,
    }
    article = _parse_article(raw)
    assert article is not None
    assert article.url == ""
    assert article.title == ""
    assert article.headline == ""
    assert article.standfirst == ""
    assert article.body_text == ""


def test_parse_article_standfirst_html_stripped():
    raw = _raw_article()
    raw["fields"]["standfirst"] = "<p>Some <strong>bold</strong> text</p>"
    article = _parse_article(raw)
    assert article is not None
    assert article.standfirst == "Some bold text"


def test_parse_article_headline_html_stripped():
    raw = _raw_article()
    raw["fields"]["headline"] = "<em>Breaking</em>: News"
    article = _parse_article(raw)
    assert article is not None
    assert article.headline == "Breaking : News"
