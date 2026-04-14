from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class TokenUsage:
    """Token usage for a single LLM call."""
    input_tokens: int = 0
    output_tokens: int = 0
    duration_ms: int | None = None

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass
class TokenTracker:
    """Accumulates token usage across multiple LLM calls. Thread-safe."""
    calls: list[TokenUsage] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, usage: TokenUsage) -> None:
        with self._lock:
            self.calls.append(usage)

    @property
    def total_input(self) -> int:
        with self._lock:
            return sum(u.input_tokens for u in self.calls)

    @property
    def total_output(self) -> int:
        with self._lock:
            return sum(u.output_tokens for u in self.calls)

    @property
    def total(self) -> int:
        return self.total_input + self.total_output

    @property
    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)

    @property
    def total_duration_ms(self) -> int:
        """Sum of individual call durations (total LLM compute time, not wall-clock)."""
        with self._lock:
            return sum(u.duration_ms for u in self.calls if u.duration_ms is not None)

    def reset(self) -> None:
        with self._lock:
            self.calls.clear()

    def snapshot(self) -> list[TokenUsage]:
        """Return a copy of calls for safe iteration."""
        with self._lock:
            return list(self.calls)


class LLMBackend(Protocol):
    # Sync methods
    def complete(self, system: str, user: str, temperature: float = 0.3) -> str: ...
    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict: ...

    # Async methods
    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str: ...
    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict: ...

    @property
    def model_name(self) -> str: ...

    @property
    def token_tracker(self) -> TokenTracker: ...
