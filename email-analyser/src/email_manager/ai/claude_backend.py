from __future__ import annotations

import asyncio
import json
import logging
import time

import anthropic

from email_manager.ai.base import TokenTracker, TokenUsage

logger = logging.getLogger(__name__)

_RETRY_DELAYS = [5, 15, 30, 60]  # seconds between retries on 429
_MIN_CALL_INTERVAL = 2.0  # minimum seconds between consecutive API calls


def _prefect_log_warning(msg: str) -> None:
    """Log via Prefect's run logger when inside a flow, otherwise fall back to stdlib logging."""
    try:
        from prefect.logging import get_run_logger
        get_run_logger().warning(msg)
    except Exception:
        logger.warning(msg)


def _rate_limit_detail(exc: anthropic.RateLimitError) -> str:
    """Extract the most useful detail from a RateLimitError for logging."""
    parts: list[str] = []
    try:
        # Try structured body first, then fall back to exc.message / str(exc)
        body: dict = {}
        if hasattr(exc, "body") and isinstance(exc.body, dict):
            body = exc.body
        elif hasattr(exc, "response") and exc.response is not None:
            try:
                body = exc.response.json()
            except Exception:
                pass
        msg = (body.get("error", {}) or {}).get("message", "")
        if not msg:
            msg = getattr(exc, "message", None) or str(exc)
        parts.append(msg)

        hdrs = {}
        if hasattr(exc, "response") and exc.response is not None:
            hdrs = dict(exc.response.headers)
        reset = hdrs.get("x-ratelimit-reset-requests") or hdrs.get("x-ratelimit-reset-tokens") or ""
        limit_req = hdrs.get("x-ratelimit-limit-requests", "")
        limit_tok = hdrs.get("x-ratelimit-limit-tokens", "")
        remaining_req = hdrs.get("x-ratelimit-remaining-requests", "")
        remaining_tok = hdrs.get("x-ratelimit-remaining-tokens", "")
        if limit_req or limit_tok:
            parts.append(f"limits={limit_req}req/{limit_tok}tok remaining={remaining_req}req/{remaining_tok}tok reset={reset}")
        elif hdrs:
            # Dump all x-ratelimit-* headers so we can see what the proxy sends
            rl_hdrs = {k: v for k, v in hdrs.items() if "ratelimit" in k.lower()}
            if rl_hdrs:
                parts.append(str(rl_hdrs))
    except Exception as inner:
        parts.append(f"(detail extraction failed: {inner})")
        parts.append(str(exc))
    return " | ".join(parts)


def _with_retry(fn):
    """Call fn(), retrying up to len(_RETRY_DELAYS) times on RateLimitError."""
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return fn()
        except anthropic.RateLimitError as e:
            _prefect_log_warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt}/{len(_RETRY_DELAYS)}): {_rate_limit_detail(e)}")
            time.sleep(delay)
    return fn()  # final attempt — let it raise


async def _with_retry_async(fn):
    """Async version of _with_retry."""
    import asyncio
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return await fn()
        except anthropic.RateLimitError as e:
            _prefect_log_warning(f"Rate limit hit, retrying in {delay}s (attempt {attempt}/{len(_RETRY_DELAYS)}): {_rate_limit_detail(e)}")
            await asyncio.sleep(delay)
    return await fn()


class ClaudeBackend:
    def __init__(self, api_key: str = "", auth_token: str = "", model: str = "claude-sonnet-4-6") -> None:
        self._api_key = api_key
        self._auth_token = auth_token
        if auth_token:
            self._client = anthropic.Anthropic(auth_token=auth_token, timeout=120.0, max_retries=0)
            self._prefill = False  # OAuth route doesn't support assistant prefill
        else:
            self._client = anthropic.Anthropic(api_key=api_key, timeout=120.0, max_retries=0)
            self._prefill = True
        self._async_client: anthropic.AsyncAnthropic | None = None  # created lazily
        self._model = model
        self._tracker = TokenTracker()
        self._last_raw_response: str = ""
        self._last_call_time: float = 0.0
        self._async_lock = asyncio.Lock()

    def _get_async_client(self) -> anthropic.AsyncAnthropic:
        if self._async_client is None:
            if self._auth_token:
                self._async_client = anthropic.AsyncAnthropic(auth_token=self._auth_token, timeout=120.0, max_retries=0)
            else:
                self._async_client = anthropic.AsyncAnthropic(api_key=self._api_key, timeout=120.0, max_retries=0)
        return self._async_client

    def close(self) -> None:
        """Close underlying HTTP clients to release connections and memory."""
        try:
            self._client.close()
        except Exception:
            pass
        if self._async_client is not None:
            try:
                import asyncio as _asyncio
                loop = _asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._async_client.close())
                else:
                    loop.run_until_complete(self._async_client.close())
            except Exception:
                pass
            self._async_client = None

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

    # ── Throttle helpers ──────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Sleep to maintain at least _MIN_CALL_INTERVAL between API calls."""
        elapsed = time.monotonic() - self._last_call_time
        if elapsed < _MIN_CALL_INTERVAL:
            time.sleep(_MIN_CALL_INTERVAL - elapsed)
        self._last_call_time = time.monotonic()

    async def _athrottle(self) -> None:
        """Async version: serialises calls and enforces _MIN_CALL_INTERVAL."""
        async with self._async_lock:
            elapsed = time.monotonic() - self._last_call_time
            if elapsed < _MIN_CALL_INTERVAL:
                await asyncio.sleep(_MIN_CALL_INTERVAL - elapsed)
            self._last_call_time = time.monotonic()

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        self._throttle()
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
        self._throttle()
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
        await self._athrottle()
        response = await _with_retry_async(lambda: self._get_async_client().messages.create(
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
        await self._athrottle()
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        messages: list = [{"role": "user", "content": user}]
        if self._prefill:
            messages.append({"role": "assistant", "content": "{"})
        response = await _with_retry_async(lambda: self._get_async_client().messages.create(
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
