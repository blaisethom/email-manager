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
    stale_before: str | None = None,
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

    def _resolve_company_domains() -> list[str] | None:
        """Resolve the list of company domains to process, or None for default filtering."""
        from email_manager.db import fetchall

        conditions = []
        params: list[str] = []

        if label:
            conditions.append("c.id IN (SELECT company_id FROM company_labels WHERE label = ?)")
            params.append(label)

        if stale_before:
            # Companies whose latest discussion updated_at is before the cutoff,
            # OR companies with no discussions yet
            conditions.append("""(
                NOT EXISTS (SELECT 1 FROM discussions d WHERE d.company_id = c.id)
                OR c.id IN (
                    SELECT d.company_id FROM discussions d
                    GROUP BY d.company_id
                    HAVING MAX(d.updated_at) < ?
                )
            )""")
            params.append(stale_before)

        if not conditions:
            return None

        where = " AND ".join(conditions)
        rows = fetchall(
            conn,
            f"SELECT DISTINCT c.domain FROM companies c WHERE {where} ORDER BY c.email_count DESC",
            tuple(params),
        )
        domains = [r[0] for r in rows]
        if limit:
            domains = domains[:limit]
        return domains

    # Resolve companies if filtering by label or stale_before
    target_domains = None
    if (label or stale_before) and not company:
        target_domains = _resolve_company_domains()
        if target_domains is not None:
            console.print(f"[bold]Targeting {len(target_domains)} companies[/bold]")

    if per_company and target_domains is not None:
        # Company-first mode: run all stages for each company before moving on

        # Run global stages once first
        global_stages = [s for s in stage_names if s in GLOBAL_STAGES]
        per_co_stages = [s for s in stage_names if s not in GLOBAL_STAGES]

        for stage_name in global_stages:
            count = _run_stage(stage_name, conn, backend, config, console,
                               limit=limit, force=force, clean=clean)
            results[stage_name] = count

        for i, domain in enumerate(target_domains):
            console.print(f"\n{'='*60}")
            console.print(f"  [bold cyan]Company {i+1}/{len(target_domains)}: {domain}[/bold cyan]")
            console.print(f"{'='*60}")

            for stage_name in per_co_stages:
                count = _run_stage(stage_name, conn, backend, config, console,
                                   limit=None, force=force, clean=clean, company=domain)
                results[stage_name] = results.get(stage_name, 0) + max(count, 0)
    elif target_domains is not None and not per_company:
        # Stage-first mode with resolved company list
        for stage_name in stage_names:
            if stage_name in GLOBAL_STAGES:
                count = _run_stage(stage_name, conn, backend, config, console,
                                   limit=limit, force=force, clean=clean)
            else:
                count = 0
                for domain in target_domains:
                    c = _run_stage(stage_name, conn, backend, config, console,
                                   limit=None, force=force, clean=clean, company=domain)
                    count += max(c, 0)
            results[stage_name] = count
    else:
        # Single company or no filtering — original behavior
        for stage_name in stage_names:
            count = _run_stage(stage_name, conn, backend, config, console,
                               limit=limit, force=force, clean=clean,
                               company=company, label=label, exclude=exclude, contact=contact)
            results[stage_name] = count

    logger.info("Pipeline finished — results: %s", results)
    conn.close()
    return results
