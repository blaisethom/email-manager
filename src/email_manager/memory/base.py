from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Protocol

from email_manager.ai.base import LLMBackend


@dataclass
class ContactMemory:
    """Structured AI-generated memory profile for a contact."""

    email: str
    name: str | None = None
    relationship: str = "unknown"
    summary: str = ""
    discussions: list[dict] = field(default_factory=list)
    # Each discussion: {"topic": str, "status": "active|resolved|waiting", "summary": str}
    key_facts: list[str] = field(default_factory=list)
    generated_at: str = ""
    model_used: str = ""
    strategy_used: str = ""
    version: int = 1
    emails_hash: str = ""


class MemoryBackend(Protocol):
    """Storage backend for contact memories."""

    def store(self, memory: ContactMemory) -> None: ...

    def load(self, email: str) -> ContactMemory | None: ...

    def load_all(self) -> list[ContactMemory]: ...

    def delete(self, email: str) -> None: ...


class MemoryStrategy(Protocol):
    """Generation strategy for contact memories."""

    @property
    def name(self) -> str: ...

    def generate(
        self,
        conn: sqlite3.Connection,
        ai_backend: LLMBackend,
        email_address: str,
    ) -> ContactMemory: ...
