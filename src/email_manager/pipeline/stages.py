from __future__ import annotations

import sqlite3

from email_manager.ai.base import LLMBackend
from email_manager.config import Config


def run_categorise(conn: sqlite3.Connection, backend: LLMBackend, config: Config, on_progress=None) -> int:
    from email_manager.analysis.categoriser import categorise_emails

    return categorise_emails(conn, backend, batch_size=config.ai_batch_size, on_progress=on_progress)


def run_extract_entities(conn: sqlite3.Connection, backend: LLMBackend, config: Config, on_progress=None) -> int:
    from email_manager.analysis.entities import extract_entities

    return extract_entities(conn, backend, batch_size=config.ai_batch_size, on_progress=on_progress)


def run_summarise_threads(conn: sqlite3.Connection, backend: LLMBackend, config: Config, on_progress=None) -> int:
    from email_manager.analysis.summariser import summarise_threads

    return summarise_threads(conn, backend, on_progress=on_progress)


def run_build_crm(conn: sqlite3.Connection, backend: LLMBackend, config: Config, on_progress=None) -> int:
    from email_manager.analysis.crm import build_crm

    return build_crm(conn)


ALL_STAGES = {
    "categorise": run_categorise,
    "extract_entities": run_extract_entities,
    "summarise_threads": run_summarise_threads,
    "build_crm": run_build_crm,
}
