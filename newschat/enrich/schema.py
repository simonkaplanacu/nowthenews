"""Pydantic models defining the structured output the LLM must produce."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ENTITY_TYPES = Literal[
    "person", "organisation", "place", "event", "legislation", "statistic"
]
SENTIMENT_VALUES = Literal["positive", "negative", "neutral", "mixed"]

REGION_CODES = Literal[
    "australia", "united_kingdom", "united_states", "europe",
    "middle_east", "asia_pacific", "latin_america", "africa", "global"
]

TOPIC_VALUES = Literal[
    "domestic_politics", "international_relations", "trade", "defence_security",
    "economy", "business", "immigration", "law_justice", "health", "education",
    "environment", "technology", "culture_arts", "sport", "social_issues",
    "media", "religion", "science", "human_interest", "conflict_crisis"
]

CONTENT_TYPES = Literal[
    "news_report", "analysis", "opinion", "live_blog", "review", "feature",
    "letter", "obituary", "roundup", "data_visual", "transcript",
    "social_media_post", "press_release", "speech", "parliamentary_record"
]


class Entity(BaseModel):
    name: str = Field(description="Entity name as it appears (or normalised)")
    type: ENTITY_TYPES = Field(description="Entity type")


class PolicyDomain(BaseModel):
    domain: str = Field(description="Policy area, e.g. 'healthcare', 'immigration', 'defence'")
    score: float = Field(ge=0.0, le=1.0, description="Relevance score 0-1")


class SmokeTerm(BaseModel):
    term: str = Field(description="The loaded or framing word/phrase as it appears in the text")
    context: str = Field(description="The sentence or clause containing the term")
    rationale: str = Field(description="Why this term carries implicit bias, framing, or connotation")


class Quote(BaseModel):
    quote: str = Field(description="The verbatim quote from the article")
    speaker: str = Field(description="Who said it (name or attribution)")
    context: str = Field(description="Brief context — what the quote is about")


class GeographicRelevance(BaseModel):
    region: REGION_CODES = Field(description="Region code")
    score: float = Field(ge=0.0, le=1.0, description="Relevance score 0-1")


class EnrichmentResult(BaseModel):
    """Full enrichment payload returned by the LLM for a single article."""

    entities: list[Entity] = Field(default_factory=list)
    policy_domains: list[PolicyDomain] = Field(default_factory=list)
    sentiment: SENTIMENT_VALUES = Field(description="Overall tone")
    sentiment_score: float = Field(
        ge=-1.0, le=1.0, description="Sentiment score from -1 (negative) to 1 (positive)"
    )
    framing_notes: str = Field(
        description="How the article frames its subject — narrative devices, angle, omissions"
    )
    smoke_terms: list[SmokeTerm] = Field(default_factory=list)
    quotes: list[Quote] = Field(default_factory=list)
    event_signature: str = Field(
        description=(
            "Canonical one-line description of the core event, "
            "e.g. 'UK government announces NHS funding increase of £2bn'"
        )
    )
    event_date: str | None = Field(
        default=None,
        description="ISO date (YYYY-MM-DD) of the event if identifiable, else null",
    )
    summary: str = Field(description="2-3 sentence neutral summary of the article")
    geographic_relevance: list[GeographicRelevance] = Field(default_factory=list)
    topics: list[TOPIC_VALUES] = Field(
        min_length=1, max_length=4,
        description="1-4 topic labels from the controlled vocabulary",
    )
    content_type: CONTENT_TYPES = Field(description="Content form classification")
