"""Ollama LLM client for structured article enrichment."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from newschat.config import OLLAMA_HOST, OLLAMA_NUM_CTX
from newschat.enrich.schema import EnrichmentResult

log = logging.getLogger(__name__)


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
