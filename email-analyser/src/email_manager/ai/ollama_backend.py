from __future__ import annotations

import json

import httpx

from email_manager.ai.base import TokenTracker, TokenUsage


class OllamaBackend:
    def __init__(
        self, model: str = "llama3.1:8b", base_url: str = "http://localhost:11434"
    ) -> None:
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._tracker = TokenTracker()

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={
                "model": self._model, "system": system, "prompt": user,
                "stream": False, "options": {"temperature": temperature},
            },
            timeout=120.0,
        )
        response.raise_for_status()
        data = response.json()
        self._tracker.record(TokenUsage(
            input_tokens=data.get("prompt_eval_count", 0),
            output_tokens=data.get("eval_count", 0),
        ))
        return data["response"]

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={
                "model": self._model, "system": json_system, "prompt": user,
                "stream": False, "format": "json",
                "options": {"temperature": temperature},
            },
            timeout=120.0,
        )
        response.raise_for_status()
        data = response.json()
        self._tracker.record(TokenUsage(
            input_tokens=data.get("prompt_eval_count", 0),
            output_tokens=data.get("eval_count", 0),
        ))
        return json.loads(data["response"])

    # ── Async methods ─────────────────────────────────────────────────────

    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self._base_url}/api/generate",
                json={
                    "model": self._model, "system": system, "prompt": user,
                    "stream": False, "options": {"temperature": temperature},
                },
            )
        response.raise_for_status()
        data = response.json()
        self._tracker.record(TokenUsage(
            input_tokens=data.get("prompt_eval_count", 0),
            output_tokens=data.get("eval_count", 0),
        ))
        return data["response"]

    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self._base_url}/api/generate",
                json={
                    "model": self._model, "system": json_system, "prompt": user,
                    "stream": False, "format": "json",
                    "options": {"temperature": temperature},
                },
            )
        response.raise_for_status()
        data = response.json()
        self._tracker.record(TokenUsage(
            input_tokens=data.get("prompt_eval_count", 0),
            output_tokens=data.get("eval_count", 0),
        ))
        return json.loads(data["response"])
