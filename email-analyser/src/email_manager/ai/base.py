from __future__ import annotations

from typing import Protocol


class LLMBackend(Protocol):
    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        """Send a prompt, get a string response."""
        ...

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        """Send a prompt, get a parsed JSON response."""
        ...

    @property
    def model_name(self) -> str: ...
