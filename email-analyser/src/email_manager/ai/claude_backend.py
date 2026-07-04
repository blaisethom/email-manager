from __future__ import annotations

import json
import logging
import time

import anthropic

from email_manager.ai.base import TokenTracker, TokenUsage

logger = logging.getLogger(__name__)

_RETRY_DELAYS = [30, 60, 120, 180, 240]  # seconds between retries on 429


def _prefect_log_warning(msg: str) -> None:
    """Log via Prefect's run logger when inside a flow, otherwise fall back to stdlib logging."""
    try:
        from prefect.logging import get_run_logger
        get_run_logger().warning(msg)
    except Exception:
        logger.warning(msg)


def _with_retry(fn):
    """Call fn(), retrying up to len(_RETRY_DELAYS) times on RateLimitError."""
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return fn()
        except anthropic.RateLimitError:
            _prefect_log_warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt}/{len(_RETRY_DELAYS)})")
            time.sleep(delay)
    return fn()  # final attempt — let it raise


async def _with_retry_async(fn):
    """Async version of _with_retry."""
    import asyncio
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return await fn()
        except anthropic.RateLimitError:
            _prefect_log_warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt}/{len(_RETRY_DELAYS)})")
            await asyncio.sleep(delay)
    return await fn()


class ClaudeBackend:
    def __init__(self, api_key: str = "", auth_token: str = "", model: str = "claude-sonnet-4-6") -> None:
        if auth_token:
            self._client = anthropic.Anthropic(auth_token=auth_token, timeout=120.0, max_retries=0)
            self._async_client = anthropic.AsyncAnthropic(auth_token=auth_token, timeout=120.0, max_retries=0)
            self._prefill = False  # OAuth route doesn't support assistant prefill
        else:
            self._client = anthropic.Anthropic(api_key=api_key, timeout=120.0, max_retries=0)
            self._async_client = anthropic.AsyncAnthropic(api_key=api_key, timeout=120.0, max_retries=0)
            self._prefill = True
        self._model = model
        self._tracker = TokenTracker()
        self._last_raw_response: str = ""

    @property
    def last_raw_response(self) -> str:
        """Raw text from the most recent complete_json call (before JSON parsing)."""
        return self._last_raw_response

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = _with_retry(lambda: self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        ))
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
        response = _with_retry(lambda: self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=messages,
        ))
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = ("{" + response.content[0].text) if self._prefill else response.content[0].text
        self._last_raw_response = raw
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
        response = await _with_retry_async(lambda: self._async_client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        ))
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
        response = await _with_retry_async(lambda: self._async_client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=messages,
        ))
        self._tracker.record(TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        ))
        raw = ("{" + response.content[0].text) if self._prefill else response.content[0].text
        self._last_raw_response = raw
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
