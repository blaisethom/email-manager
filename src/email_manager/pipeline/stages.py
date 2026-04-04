from __future__ import annotations

import logging
import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.config import Config

logger = logging.getLogger("email_manager.pipeline.stages")


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    )


def run_extract_base(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.base_extract import extract_base

    return extract_base(conn, console=console or Console(), limit=limit)



def run_categorise(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.categoriser import categorise_emails

    console = console or Console()
    with _make_progress(console) as progress:
        task = progress.add_task("categorise", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)
            logger.info("categorise: %d/%d", done, total)

        return categorise_emails(conn, backend, batch_size=config.ai_batch_size, on_progress=on_progress, limit=limit)


def run_summarise_threads(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.summariser import summarise_threads

    console = console or Console()
    with _make_progress(console) as progress:
        task = progress.add_task("summarise_threads", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)
            logger.info("summarise_threads: %d/%d", done, total)

        return summarise_threads(conn, backend, on_progress=on_progress, limit=limit)


def run_fetch_homepages(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.homepage import fetch_homepages

    return fetch_homepages(conn, console=console or Console(), limit=limit, max_workers=config.homepage_max_workers)


def run_label_companies(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.company_labels import label_companies, load_label_config

    console = console or Console()
    labels_config = load_label_config(getattr(config, "company_labels_path", None))

    with _make_progress(console) as progress:
        task = progress.add_task("label_companies", total=None)

        def on_progress(done: int, total: int, name: str = "") -> None:
            desc = f"label_companies ({name})" if name and done < total else "label_companies"
            progress.update(task, completed=done, total=total or 0, description=desc)
            logger.info("label_companies: %d/%d — %s", done, total, name)

        return label_companies(conn, backend, labels_config=labels_config, on_progress=on_progress, limit=limit, force=force)


def run_contact_memory(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.contact_memory import build_contact_memories
    from email_manager.memory.factory import get_memory_backends, get_memory_strategy

    console = console or Console()
    memory_backends = get_memory_backends(config, conn)
    strategy = get_memory_strategy(config)
    return build_contact_memories(conn, backend, memory_backends, strategy, console=console, limit=limit)


def run_discussions(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, company: str | None = None, label: str | None = None, exclude: list[str] | None = None, contact: str | None = None) -> int:
    from email_manager.analysis.discussions import extract_discussions, load_category_config

    console = console or Console()
    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        company_task = progress.add_task("Companies", total=None)
        batch_task = progress.add_task("  Batches", total=None)

        def on_company_progress(done: int, total: int, name: str) -> None:
            progress.update(company_task, completed=done, total=total or 0,
                            description=f"Companies ({name})" if done < total else "Companies")
            # Reset batch bar for new company
            if done < total:
                progress.update(batch_task, completed=0, total=None, description="  Batches")
            logger.info("discussions: company %d/%d — %s", done, total, name)

        def on_batch_progress(done: int, total: int) -> None:
            progress.update(batch_task, completed=done, total=total or 0)

        def on_step(description: str) -> None:
            progress.update(batch_task, description=f"  {description}")
            logger.info("discussions: %s", description)

        return extract_discussions(
            conn, backend, categories_config=categories_config,
            on_company_progress=on_company_progress,
            on_batch_progress=on_batch_progress, on_step=on_step,
            limit=limit, force=force,
            company_domain=company, company_label=label, exclude_companies=exclude,
            contact_email=contact,
        )


ALL_STAGES = {
    "extract_base": run_extract_base,
    "fetch_homepages": run_fetch_homepages,
    "contact_memory": run_contact_memory,
    "categorise": run_categorise,
    "summarise_threads": run_summarise_threads,
    "label_companies": run_label_companies,
    "discussions": run_discussions,
}
