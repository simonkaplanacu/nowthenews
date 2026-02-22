"""Pydantic models defining the structured output the LLM must produce."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Entity(BaseModel):
    name: str = Field(description="Entity name as it appears (or normalised)")
    type: str = Field(
        description="Entity type: person, organisation, place, event, legislation, statistic"
    )


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


class EnrichmentResult(BaseModel):
    """Full enrichment payload returned by the LLM for a single article."""

    entities: list[Entity] = Field(default_factory=list)
    policy_domains: list[PolicyDomain] = Field(default_factory=list)
    sentiment: str = Field(description="One of: positive, negative, neutral, mixed")
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
