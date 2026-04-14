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
        TextColumn("{task.completed}/{task.total} {task.fields[unit]}"),
        console=console,
    )


def run_extract_base(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.analysis.base_extract import extract_base

    return extract_base(conn, console=console or Console(), limit=limit, force=force)




def run_fetch_homepages(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, company: str | None = None, label: str | None = None) -> int:
    from email_manager.analysis.homepage import fetch_homepages

    if label and not company:
        # Fetch homepages for all companies with this label
        from email_manager.db import fetchall
        domains = [r[0] for r in fetchall(
            conn,
            "SELECT c.domain FROM companies c JOIN company_labels cl ON c.id = cl.company_id WHERE cl.label = ?",
            (label,),
        )]
        total = 0
        for domain in domains:
            total += fetch_homepages(conn, console=console or Console(), limit=limit, company_domain=domain, max_workers=config.homepage_max_workers)
        return total

    return fetch_homepages(conn, console=console or Console(), limit=limit, company_domain=company, max_workers=config.homepage_max_workers)


def run_label_companies(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, company: str | None = None, label: str | None = None) -> int:
    from email_manager.analysis.company_labels import label_companies, load_label_config

    console = console or Console()
    labels_config = load_label_config(getattr(config, "company_labels_path", None))

    with _make_progress(console) as progress:
        task = progress.add_task("label_companies", total=None, unit="companies")

        def on_progress(done: int, total: int, name: str = "") -> None:
            desc = f"label_companies ({name})" if name and done < total else "label_companies"
            progress.update(task, completed=done, total=total or 0, description=desc)
            logger.info("label_companies: %d/%d — %s", done, total, name)

        count = label_companies(conn, backend, labels_config=labels_config, on_progress=on_progress, limit=limit, force=force, company_domain=company)
    console.print(f"  [green]label_companies: labelled {count} companies[/green]")
    return count


def run_contact_memory(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, company: str | None = None, label: str | None = None) -> int:
    from email_manager.analysis.contact_memory import build_contact_memories
    from email_manager.memory.factory import get_memory_backends, get_memory_strategy

    console = console or Console()
    memory_backends = get_memory_backends(config, conn)
    strategy = get_memory_strategy(config)

    if label and not company:
        # Process contacts for all companies with this label
        from email_manager.db import fetchall
        domains = [r[0] for r in fetchall(
            conn,
            "SELECT c.domain FROM companies c JOIN company_labels cl ON c.id = cl.company_id WHERE cl.label = ?",
            (label,),
        )]
        total = 0
        for domain in domains:
            total += build_contact_memories(conn, backend, memory_backends, strategy, company_domain=domain, console=console, limit=limit)
        return total

    return build_contact_memories(conn, backend, memory_backends, strategy, company_domain=company, console=console, limit=limit)



def run_sync_calendar(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    from email_manager.ingestion.calendar_client import sync_calendar_events

    console = console or Console()
    total = 0
    for acct in config.get_accounts():
        if acct.backend == "gmail":
            label = acct.name or "gmail"
            console.print(f"  Syncing calendar for: {label}")
            total += sync_calendar_events(conn, acct, console=console)
    return total



def run_extract_events(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, clean: bool = False, company: str | None = None, label: str | None = None, concurrency: int = 1) -> int:
    from email_manager.analysis.events import extract_events, load_category_config

    console = console or Console()
    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))

    # Use a cheaper model for event extraction if configured
    stage_backend = backend
    extract_model = getattr(config, "extract_events_model", "")
    if extract_model and backend is not None:
        from email_manager.ai.factory import get_backend as _get_backend
        from email_manager.config import Config as _Config

        override_config = _Config(
            ai_backend=config.ai_backend,
            anthropic_api_key=config.anthropic_api_key,
            claude_model=extract_model,
            ollama_model=config.ollama_model,
            ollama_url=config.ollama_url,
        )
        try:
            stage_backend = _get_backend(override_config)
            console.print(f"  [dim]extract_events using model: {stage_backend.model_name}[/dim]")
        except Exception:
            pass  # fall back to default backend

    with _make_progress(console) as progress:
        task = progress.add_task("extract_events", total=None, unit="threads")

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total or 0)
            logger.info("extract_events: %d/%d threads", done, total)

        count = extract_events(
            conn, stage_backend, categories_config=categories_config,
            limit=limit, force=force, clean=clean,
            company_domain=company, company_label=label,
            on_progress=on_progress, concurrency=concurrency,
        )
    console.print(f"  [green]extract_events: generated {count} events[/green]")
    return count


def run_discover_discussions(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, clean: bool = False, company: str | None = None, label: str | None = None) -> int:
    from email_manager.analysis.discover_discussions import discover_discussions
    from email_manager.analysis.events import load_category_config

    console = console or Console()
    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))

    with _make_progress(console) as progress:
        task = progress.add_task("discover_discussions", total=None, unit="companies")

        def on_progress(done: int, total: int, name: str = "") -> None:
            desc = f"discover_discussions ({name})" if name and done < total else "discover_discussions"
            progress.update(task, completed=done, total=total or 0, description=desc)
            logger.info("discover_discussions: %d/%d — %s", done, total, name)

        count = discover_discussions(
            conn, backend, limit=limit, force=force, clean=clean,
            company_domain=company, company_label=label,
            on_progress=on_progress,
            categories_config=categories_config,
        )
    console.print(f"  [green]discover_discussions: created/updated {count} discussions[/green]")
    return count


def run_analyse_discussions(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, clean: bool = False, company: str | None = None, label: str | None = None, concurrency: int = 1) -> int:
    from email_manager.analysis.analyse_discussions import analyse_discussions, load_category_config

    console = console or Console()
    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))

    with _make_progress(console) as progress:
        task = progress.add_task("analyse_discussions", total=None, unit="discussions")

        def on_progress(done: int, total: int, name: str = "") -> None:
            desc = f"analyse_discussions ({name})" if name and done < total else "analyse_discussions"
            progress.update(task, completed=done, total=total or 0, description=desc)
            logger.info("analyse_discussions: %d/%d — %s", done, total, name)

        count = analyse_discussions(
            conn, backend, categories_config=categories_config,
            limit=limit, force=force, clean=clean, company_domain=company,
            company_label=label, on_progress=on_progress, concurrency=concurrency,
        )
    console.print(f"  [green]analyse_discussions: analysed {count} discussions[/green]")
    return count


def run_propose_actions(conn: sqlite3.Connection, backend: LLMBackend, config: Config, console: Console = None, limit: int | None = None, force: bool = False, clean: bool = False, company: str | None = None, label: str | None = None, concurrency: int = 1) -> int:
    from email_manager.analysis.propose_actions import propose_actions, load_category_config

    console = console or Console()
    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))

    with _make_progress(console) as progress:
        task = progress.add_task("propose_actions", total=None, unit="discussions")

        def on_progress(done: int, total: int, name: str = "") -> None:
            desc = f"propose_actions ({name})" if name and done < total else "propose_actions"
            progress.update(task, completed=done, total=total or 0, description=desc)
            logger.info("propose_actions: %d/%d — %s", done, total, name)

        count = propose_actions(
            conn, backend, categories_config=categories_config,
            limit=limit, force=force, clean=clean, company_domain=company,
            company_label=label, on_progress=on_progress, concurrency=concurrency,
        )
    console.print(f"  [green]propose_actions: proposed actions for {count} discussions[/green]")
    return count


ALL_STAGES = {
    # Phase 1: Base extraction
    "extract_base": run_extract_base,
    "fetch_homepages": run_fetch_homepages,
    "label_companies": run_label_companies,
    # Phase 2: Event-driven discussion pipeline
    "extract_events": run_extract_events,
    "discover_discussions": run_discover_discussions,
    "analyse_discussions": run_analyse_discussions,
    # Phase 3: Proposed actions & contact enrichment
    "propose_actions": run_propose_actions,
    "contact_memory": run_contact_memory,
}
