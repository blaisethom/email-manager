from __future__ import annotations

import inspect
import logging
import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.ai.factory import get_backend
from email_manager.config import Config
from email_manager.db import get_db
from email_manager.pipeline.stages import ALL_STAGES

logger = logging.getLogger("email_manager.pipeline")


def _setup_file_logging(config: Config) -> None:
    """Configure file logging for pipeline runs."""
    log_path = getattr(config, "log_file", None) or "email_manager.log"
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root = logging.getLogger("email_manager")
    # Avoid duplicate handlers on repeated calls
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == handler.baseFilename
               for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(logging.INFO)


def _run_stage(
    stage_name: str,
    conn: sqlite3.Connection,
    backend: LLMBackend | None,
    config: Config,
    console: Console,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company: str | None = None,
    label: str | None = None,
    exclude: list[str] | None = None,
    contact: str | None = None,
) -> int:
    """Run a single pipeline stage. Returns item count or -1 on error."""
    if stage_name not in ALL_STAGES:
        console.print(f"[red]Unknown stage: {stage_name}[/red]")
        return -1

    stage_fn = ALL_STAGES[stage_name]
    console.print(f"\n[bold]Running stage: {stage_name}[/bold]")
    logger.info("Starting stage: %s", stage_name)

    try:
        kwargs = dict(console=console, limit=limit, force=force)
        sig = inspect.signature(stage_fn)
        if clean and "clean" in sig.parameters:
            kwargs["clean"] = True
        for opt_name, opt_val in [("company", company), ("label", label), ("exclude", exclude), ("contact", contact)]:
            if opt_name in sig.parameters and opt_val:
                kwargs[opt_name] = opt_val
        count = stage_fn(conn, backend, config, **kwargs)
        console.print(f"  [green]{stage_name}: processed {count} items[/green]")
        logger.info("Finished stage: %s — processed %d items", stage_name, count)
        return count
    except Exception as e:
        console.print(f"  [red]{stage_name} failed: {e}[/red]")
        logger.exception("Stage %s failed", stage_name)
        return -1


# Stages that don't support --company filtering and should run once globally
GLOBAL_STAGES = {"sync_calendar", "extract_base"}


def run_pipeline(
    config: Config,
    stages: list[str] | None = None,
    console: Console | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company: str | None = None,
    label: str | None = None,
    exclude: list[str] | None = None,
    contact: str | None = None,
    per_company: bool = False,
) -> dict[str, int]:
    if console is None:
        console = Console()

    _setup_file_logging(config)

    conn = get_db(config)

    stage_names = stages or list(ALL_STAGES.keys())
    results: dict[str, int] = {}

    # Only initialise AI backend if we need it
    NO_AI_STAGES = {"extract_base", "fetch_homepages", "sync_calendar"}
    needs_ai = any(s not in NO_AI_STAGES for s in stage_names)
    backend = None
    if needs_ai:
        backend = get_backend(config)
        console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")
        logger.info("Using AI backend: %s", backend.model_name)
    else:
        console.print("Running non-AI stages only")
        logger.info("Running non-AI stages only")

    logger.info("Pipeline started — stages: %s", ", ".join(stage_names))

    if per_company and label and not company:
        # Company-first mode: run all stages for each company before moving on
        from email_manager.db import fetchall

        # Run global stages once first
        global_stages = [s for s in stage_names if s in GLOBAL_STAGES]
        per_co_stages = [s for s in stage_names if s not in GLOBAL_STAGES]

        for stage_name in global_stages:
            count = _run_stage(stage_name, conn, backend, config, console,
                               limit=limit, force=force, clean=clean, label=label)
            results[stage_name] = count

        # Get companies for this label
        domains = [r[0] for r in fetchall(
            conn,
            """SELECT DISTINCT c.domain FROM companies c
               JOIN company_labels cl ON c.id = cl.company_id
               WHERE cl.label = ? ORDER BY c.email_count DESC""",
            (label,),
        )]
        if limit:
            domains = domains[:limit]

        console.print(f"\n[bold]Processing {len(domains)} companies (per-company mode)[/bold]")

        for i, domain in enumerate(domains):
            console.print(f"\n{'='*60}")
            console.print(f"  [bold cyan]Company {i+1}/{len(domains)}: {domain}[/bold cyan]")
            console.print(f"{'='*60}")

            for stage_name in per_co_stages:
                count = _run_stage(stage_name, conn, backend, config, console,
                                   limit=None, force=force, clean=clean, company=domain)
                key = f"{stage_name}"
                results[key] = results.get(key, 0) + max(count, 0)
    else:
        # Stage-first mode (default): run all companies per stage
        for stage_name in stage_names:
            count = _run_stage(stage_name, conn, backend, config, console,
                               limit=limit, force=force, clean=clean,
                               company=company, label=label, exclude=exclude, contact=contact)
            results[stage_name] = count

    logger.info("Pipeline finished — results: %s", results)
    conn.close()
    return results
