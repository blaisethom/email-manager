from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from email_manager.ai.base import TokenTracker
from email_manager.config import Config
from email_manager.db import get_db

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def sample_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "simple.eml").read_bytes()


@pytest.fixture
def reply_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "reply.eml").read_bytes()


@pytest.fixture
def html_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "html_email.eml").read_bytes()


@pytest.fixture
def thread_chain_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "thread_chain.eml").read_bytes()


@pytest.fixture
def test_config(tmp_path: Path) -> Config:
    return Config(
        db_backend="sqlite",
        db_path=tmp_path / "test.db",
        imap_host="imap.test.com",
        imap_user="test@test.com",
        imap_password="test",
    )


@pytest.fixture
def test_db(test_config: Config) -> sqlite3.Connection:
    conn = get_db(test_config)
    yield conn
    conn.close()


# ── Mock LLM backend ───────────────────────────────────────────────────────


class MockLLMBackend:
    """Deterministic LLM backend for testing. Satisfies the LLMBackend protocol."""

    def __init__(self, model: str = "test-model-v1", responses: list[dict] | None = None):
        self._model = model
        self._responses: list[dict] = list(responses or [])
        self._call_index = 0
        self._tracker = TokenTracker()
        self.calls: list[tuple[str, str]] = []

    def _next_response(self) -> dict:
        if self._call_index < len(self._responses):
            resp = self._responses[self._call_index]
            self._call_index += 1
            return resp
        return {}

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        self.calls.append((system, user))
        return json.dumps(self._next_response())

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        self.calls.append((system, user))
        return self._next_response()

    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str:
        self.calls.append((system, user))
        return json.dumps(self._next_response())

    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        self.calls.append((system, user))
        return self._next_response()

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker


@pytest.fixture
def mock_backend():
    """Factory fixture: call with optional model name and responses list."""
    def _make(model="test-model-v1", responses=None):
        return MockLLMBackend(model=model, responses=responses or [])
    return _make


# ── DB seed helpers ─────────────────────────────────────────────────────────


def insert_email(
    conn: sqlite3.Connection,
    message_id: str,
    from_address: str,
    to_addresses: str | list[str],
    date: str,
    subject: str = "Test Subject",
    body_text: str = "Test body",
    thread_id: str | None = None,
    folder: str = "INBOX",
) -> int:
    """Insert a minimal email row. Returns the row id."""
    now = datetime.now(timezone.utc).isoformat()
    if isinstance(to_addresses, str):
        to_addresses = [to_addresses]
    conn.execute(
        """INSERT OR IGNORE INTO emails
           (message_id, thread_id, subject, normalised_subject, from_address, from_name,
            to_addresses, cc_addresses, date, body_text, body_html,
            raw_headers, folder, size_bytes, has_attachments, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (message_id, thread_id, subject, subject.lower(), from_address,
         from_address.split("@")[0], json.dumps(to_addresses), json.dumps([]),
         date, body_text, None, json.dumps({}), folder, 1000, 0, now),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM emails WHERE message_id = ?", (message_id,)).fetchone()
    return row[0] if row else -1


def insert_company(
    conn: sqlite3.Connection,
    domain: str,
    name: str | None = None,
    email_count: int = 5,
) -> int:
    """Insert a company row. Returns the company id."""
    name = name or domain.split(".")[0].capitalize()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO companies (name, domain, email_count, first_seen, last_seen)
           VALUES (?, ?, ?, ?, ?)""",
        (name, domain, email_count, now, now),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
    return row[0] if row else -1


def insert_processing_run(
    conn: sqlite3.Connection,
    company_domain: str,
    stage: str,
    model: str = "test-model-v1",
    prompt_hash: str | None = None,
    email_cutoff_date: str | None = None,
    error: str | None = None,
) -> int:
    """Insert a processing_run record. Returns the run id."""
    now = datetime.now(timezone.utc).isoformat()
    mode = f"staged:{stage}"
    cursor = conn.execute(
        """INSERT INTO processing_runs
           (company_domain, mode, model, started_at, completed_at, email_cutoff_date, prompt_hash, error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (company_domain, mode, model, now, now, email_cutoff_date, prompt_hash, error),
    )
    conn.commit()
    return cursor.lastrowid


def insert_event(
    conn: sqlite3.Connection,
    event_id: str,
    thread_id: str,
    domain: str,
    event_type: str,
    actor: str = "someone@example.com",
    detail: str = "Test event",
    event_date: str = "2025-06-01",
    confidence: float = 0.9,
    discussion_id: int | None = None,
    run_id: int | None = None,
    source_email_id: str | None = None,
) -> None:
    """Insert an event_ledger row."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO event_ledger
           (id, thread_id, source_email_id, domain, type, actor, detail, event_date,
            confidence, source_type, discussion_id, run_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'email', ?, ?, ?)""",
        (event_id, thread_id, source_email_id, domain, event_type, actor, detail,
         event_date, confidence, discussion_id, run_id, now),
    )
    conn.commit()


def insert_discussion(
    conn: sqlite3.Connection,
    company_id: int,
    title: str,
    category: str = "general",
    current_state: str = "active",
    summary: str | None = None,
) -> int:
    """Insert a discussion row. Returns the discussion id."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO discussions
           (title, category, current_state, company_id, summary,
            participants, first_seen, last_seen, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (title, category, current_state, company_id, summary,
         json.dumps([]), now, now, now),
    )
    conn.commit()
    return cursor.lastrowid
