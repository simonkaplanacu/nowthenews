"""LLM clients for structured article enrichment (Ollama + Groq)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import httpx

from newschat.config import GROQ_API_KEY, OLLAMA_HOST, OLLAMA_NUM_CTX
from newschat.enrich.schema import EnrichmentResult

log = logging.getLogger(__name__)

_GROQ_BASE = "https://api.groq.com/openai/v1"


class OllamaClient:
    """Thin wrapper around the Ollama /api/chat endpoint.

    The model is injected at construction time so callers can swap it
    for experiments without touching this module.
    """

    def __init__(self, model: str, host: str | None = None, num_ctx: int | None = None):
        self.model = model
        self.host = (host or OLLAMA_HOST).rstrip("/")
        self.num_ctx = num_ctx or OLLAMA_NUM_CTX
        self._http = httpx.Client(
            timeout=httpx.Timeout(connect=10, read=300, write=30, pool=10)
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich(self, system: str, user: str) -> EnrichmentResult:
        """Send a chat completion request and parse the structured response.

        Uses Ollama's native ``format`` parameter to constrain the output
        to the ``EnrichmentResult`` JSON schema.
        """
        payload = self._build_payload(system, user)
        raw = self._call(payload)
        return EnrichmentResult.model_validate_json(raw)

    def check_health(self) -> bool:
        """Return True if Ollama is reachable and the model is available."""
        try:
            resp = self._http.get(f"{self.host}/api/tags")
            resp.raise_for_status()
            available = set()
            for m in resp.json().get("models", []):
                name = m["name"]
                available.add(name)
                # Strip :latest so "qwen3:30b-a3b:latest" also matches "qwen3:30b-a3b"
                if name.endswith(":latest"):
                    available.add(name[: -len(":latest")])
            return self.model in available
        except Exception:
            return False

    def close(self) -> None:
        self._http.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_payload(self, system: str, user: str) -> dict[str, Any]:
        return {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "format": EnrichmentResult.model_json_schema(),
            "options": {
                "temperature": 0,
                "num_ctx": self.num_ctx,
            },
        }

    def _call(self, payload: dict) -> str:
        """POST to Ollama and return the raw assistant message content."""
        url = f"{self.host}/api/chat"
        log.debug("Ollama request to %s model=%s", url, self.model)

        resp = self._http.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()

        content = body.get("message", {}).get("content", "")
        if not content:
            raise RuntimeError(f"Empty response from Ollama: {json.dumps(body)[:500]}")

        log.debug(
            "Ollama response: %d chars, eval_duration=%s",
            len(content),
            body.get("eval_duration"),
        )
        return content


class GroqClient:
    """Groq cloud inference client with the same public API as OllamaClient."""

    MAX_RETRIES = 2

    def __init__(self, model: str, api_key: str | None = None):
        self.model = model
        self._api_key = api_key or GROQ_API_KEY
        if not self._api_key:
            raise ValueError("GROQ_API_KEY not set — add it to .env")
        self._http = httpx.Client(
            base_url=_GROQ_BASE,
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=httpx.Timeout(connect=10, read=120, write=30, pool=10),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich(self, system: str, user: str) -> EnrichmentResult:
        schema_json = json.dumps(EnrichmentResult.model_json_schema(), indent=2)
        system_with_schema = (
            f"{system}\n\n"
            f"You MUST return JSON that conforms EXACTLY to this JSON Schema:\n"
            f"```json\n{schema_json}\n```\n"
            f"Return a single JSON object (not an array). "
            f"Use the exact field names and types shown in the schema."
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_with_schema},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "reasoning_effort": "none",
            "response_format": {"type": "json_object"},
        }

        last_err: Exception | None = None
        for attempt in range(1 + self.MAX_RETRIES):
            raw = self._call(payload)
            try:
                return EnrichmentResult.model_validate_json(raw)
            except Exception as e:
                last_err = e
                log.warning(
                    "Groq JSON parse failed (attempt %d/%d): %s — raw[:200]: %s",
                    attempt + 1, 1 + self.MAX_RETRIES, e, raw[:200],
                )
        raise RuntimeError(f"Groq returned invalid JSON after {1 + self.MAX_RETRIES} attempts") from last_err

    def check_health(self) -> bool:
        try:
            resp = self._http.get("/models")
            resp.raise_for_status()
            models = {m["id"] for m in resp.json().get("data", [])}
            return self.model in models
        except Exception:
            return False

    def close(self) -> None:
        self._http.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _call(self, payload: dict) -> str:
        log.debug("Groq request model=%s", self.model)
        for attempt in range(3):
            resp = self._http.post("/chat/completions", json=payload)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("retry-after", 5))
                log.warning("Groq 429 — waiting %.1fs (attempt %d/3)", retry_after, attempt + 1)
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            break
        else:
            resp.raise_for_status()  # raise the last 429
        body = resp.json()

        content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not content:
            raise RuntimeError(f"Empty response from Groq: {json.dumps(body)[:500]}")

        usage = body.get("usage", {})
        log.debug(
            "Groq response: %d chars, prompt_tokens=%s, completion_tokens=%s",
            len(content), usage.get("prompt_tokens"), usage.get("completion_tokens"),
        )
        return content
