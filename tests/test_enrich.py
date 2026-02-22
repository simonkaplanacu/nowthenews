"""Tests for the enrichment module — schema validation, prompt building, LLM client."""

import json

from newschat.enrich.prompt import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt
from newschat.enrich.schema import EnrichmentResult, Entity, PolicyDomain, Quote, SmokeTerm


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

def _sample_result() -> dict:
    """Minimal valid enrichment payload."""
    return {
        "entities": [
            {"name": "Keir Starmer", "type": "person"},
            {"name": "NHS", "type": "organisation"},
        ],
        "policy_domains": [
            {"domain": "healthcare", "score": 0.9},
        ],
        "sentiment": "neutral",
        "sentiment_score": 0.1,
        "framing_notes": "Government-centric framing with emphasis on spending figures.",
        "smoke_terms": [
            {
                "term": "crisis",
                "context": "The NHS faces a deepening crisis.",
                "rationale": "Loaded term implying urgency without specifying measurable criteria.",
            }
        ],
        "quotes": [
            {
                "quote": "We will fix the NHS.",
                "speaker": "Keir Starmer",
                "context": "Speech at Labour conference on health policy.",
            }
        ],
        "event_signature": "UK Labour leader pledges NHS reform at party conference",
        "event_date": "2024-09-22",
        "summary": "Labour leader Keir Starmer announced NHS reform plans at the party conference.",
    }


def test_enrichment_result_parses_valid_payload():
    data = _sample_result()
    result = EnrichmentResult.model_validate(data)
    assert len(result.entities) == 2
    assert result.entities[0].name == "Keir Starmer"
    assert result.sentiment == "neutral"


def test_enrichment_result_roundtrip_json():
    data = _sample_result()
    result = EnrichmentResult.model_validate(data)
    dumped = json.loads(result.model_dump_json())
    restored = EnrichmentResult.model_validate(dumped)
    assert restored.event_signature == result.event_signature
    assert len(restored.smoke_terms) == 1


def test_smoke_terms_are_emergent():
    """Smoke terms come from the LLM response — they are not a fixed list."""
    data = _sample_result()
    data["smoke_terms"] = [
        {"term": "flood", "context": "A flood of migrants.", "rationale": "Dehumanising metaphor."},
        {"term": "burden", "context": "The burden on public services.", "rationale": "Frames people as cost."},
    ]
    result = EnrichmentResult.model_validate(data)
    terms = [s.term for s in result.smoke_terms]
    assert "flood" in terms
    assert "burden" in terms
    assert len(result.smoke_terms) == 2


def test_empty_smoke_terms_allowed():
    data = _sample_result()
    data["smoke_terms"] = []
    result = EnrichmentResult.model_validate(data)
    assert result.smoke_terms == []


def test_quotes_parsed():
    data = _sample_result()
    result = EnrichmentResult.model_validate(data)
    assert len(result.quotes) == 1
    assert result.quotes[0].speaker == "Keir Starmer"


def test_event_date_nullable():
    data = _sample_result()
    data["event_date"] = None
    result = EnrichmentResult.model_validate(data)
    assert result.event_date is None


def test_json_schema_generation():
    """The schema must be serialisable for Ollama's format parameter."""
    schema = EnrichmentResult.model_json_schema()
    assert "properties" in schema
    assert "smoke_terms" in schema["properties"]
    assert "quotes" in schema["properties"]
    # Must be JSON-serialisable
    json.dumps(schema)


# ---------------------------------------------------------------------------
# Prompt tests
# ---------------------------------------------------------------------------

def test_prompt_version_set():
    assert PROMPT_VERSION == "v1"


def test_system_prompt_mentions_smoke_terms():
    assert "smoke term" in SYSTEM_PROMPT.lower()
    assert "pre-set list" in SYSTEM_PROMPT.lower() or "pre-set" in SYSTEM_PROMPT.lower()


def test_build_user_prompt_contains_fields():
    prompt = build_user_prompt(
        title="Test Title",
        headline="Test Headline",
        byline="Test Author",
        published_at="2024-01-15T10:30:00Z",
        body_text="Article body text here.",
    )
    assert "TITLE: Test Title" in prompt
    assert "HEADLINE: Test Headline" in prompt
    assert "BYLINE: Test Author" in prompt
    assert "Article body text here." in prompt


# ---------------------------------------------------------------------------
# LLM client tests (unit — no Ollama needed)
# ---------------------------------------------------------------------------

def test_ollama_client_build_payload():
    from newschat.enrich.llm import OllamaClient

    client = OllamaClient(model="test-model", host="http://localhost:11434")
    payload = client._build_payload("sys", "usr")
    assert payload["model"] == "test-model"
    assert payload["stream"] is False
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][1]["content"] == "usr"
    assert "format" in payload
    assert payload["options"]["temperature"] == 0
    client.close()
