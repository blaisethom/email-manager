from __future__ import annotations

import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.config import Config


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    )


def run_extract_base(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None) -> int:
    from email_manager.analysis.base_extract import extract_base

    return extract_base(conn, console=console or Console(), limit=limit)


def run_extract_entities(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None) -> int:
    from email_manager.analysis.entities import extract_entities

    console = console or Console()
    with _make_progress(console) as progress:
        task = progress.add_task("extract_entities", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)

        return extract_entities(conn, backend, batch_size=config.ai_batch_size, on_progress=on_progress, limit=limit)


def run_categorise(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None) -> int:
    from email_manager.analysis.categoriser import categorise_emails

    console = console or Console()
    with _make_progress(console) as progress:
        task = progress.add_task("categorise", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)

        return categorise_emails(conn, backend, batch_size=config.ai_batch_size, on_progress=on_progress, limit=limit)


def run_summarise_threads(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None) -> int:
    from email_manager.analysis.summariser import summarise_threads

    console = console or Console()
    with _make_progress(console) as progress:
        task = progress.add_task("summarise_threads", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)

        return summarise_threads(conn, backend, on_progress=on_progress, limit=limit)


def run_contact_memory(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None) -> int:
    from email_manager.analysis.contact_memory import build_contact_memories
    from email_manager.memory.factory import get_memory_backends, get_memory_strategy

    console = console or Console()
    memory_backends = get_memory_backends(config, conn)
    strategy = get_memory_strategy(config)
    return build_contact_memories(conn, backend, memory_backends, strategy, console=console, limit=limit)


ALL_STAGES = {
    "extract_base": run_extract_base,
    "contact_memory": run_contact_memory,
    "extract_entities": run_extract_entities,
    "categorise": run_categorise,
    "summarise_threads": run_summarise_threads,
}
