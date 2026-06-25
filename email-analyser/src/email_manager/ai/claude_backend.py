from __future__ import annotations

import json

import anthropic

from email_manager.ai.base import TokenTracker, TokenUsage


class ClaudeBackend:
    def __init__(self, api_key: str = "", auth_token: str = "", model: str = "claude-sonnet-4-6") -> None:
        if auth_token:
            self._client = anthropic.Anthropic(auth_token=auth_token, timeout=120.0)
            self._async_client = anthropic.AsyncAnthropic(auth_token=auth_token, timeout=120.0)
            self._prefill = False  # OAuth route doesn't support assistant prefill
        else:
            self._client = anthropic.Anthropic(api_key=api_key, timeout=120.0)
            self._async_client = anthropic.AsyncAnthropic(api_key=api_key, timeout=120.0)
            self._prefill = True
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
        messages: list = [{"role": "user", "content": user}]
        if self._prefill:
            messages.append({"role": "assistant", "content": "{"})
        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=messages,
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = ("{" + response.content[0].text) if self._prefill else response.content[0].text
        # Strip markdown fences if present
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            cleaned = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
        if not cleaned.startswith("{"):
            start = cleaned.find("{")
            if start != -1:
                cleaned = cleaned[start: cleaned.rfind("}") + 1]
        try:
            return json.loads(cleaned)
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
        messages: list = [{"role": "user", "content": user}]
        if self._prefill:
            messages.append({"role": "assistant", "content": "{"})
        response = await self._async_client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=messages,
        )
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = ("{" + response.content[0].text) if self._prefill else response.content[0].text
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            cleaned = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
        if not cleaned.startswith("{"):
            start = cleaned.find("{")
            if start != -1:
                cleaned = cleaned[start: cleaned.rfind("}") + 1]
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from Claude API. Response ({len(raw)} chars): {raw[:300]}") from e
