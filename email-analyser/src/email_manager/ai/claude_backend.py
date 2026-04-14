from __future__ import annotations

import json

import anthropic

from email_manager.ai.base import TokenTracker, TokenUsage


class ClaudeBackend:
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6") -> None:
        self._client = anthropic.Anthropic(api_key=api_key, timeout=120.0)
        self._async_client = anthropic.AsyncAnthropic(api_key=api_key, timeout=120.0)
        self._model = model
        self._tracker = TokenTracker()

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        return response.content[0].text

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=[
                {"role": "user", "content": user},
                {"role": "assistant", "content": "{"},
            ],
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = "{" + response.content[0].text
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from Claude API. Response ({len(raw)} chars): {raw[:300]}") from e

    # ── Async methods ─────────────────────────────────────────────────────

    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = await self._async_client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        return response.content[0].text

    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        response = await self._async_client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=[
                {"role": "user", "content": user},
                {"role": "assistant", "content": "{"},
            ],
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = "{" + response.content[0].text
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from Claude API. Response ({len(raw)} chars): {raw[:300]}") from e
