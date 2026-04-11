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
        logger.info("Finished stage: %s — processed %d items", stage_name, count)
        return count
    except Exception as e:
        console.print(f"  [red]{stage_name} failed: {e}[/red]")
        logger.exception("Stage %s failed", stage_name)
        return -1


# Stages that should run once globally in per-company mode, not per company
GLOBAL_STAGES = {"extract_base", "fetch_homepages", "label_companies"}


def run_pipeline(
    config: Config,
    stages: list[str] | None = None,
    console: Console | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company: str | None = None,
    company_list: list[str] | None = None,
    label: str | None = None,
    exclude: list[str] | None = None,
    contact: str | None = None,
    per_company: bool = False,
    stale_before: str | None = None,
    last_seen_after: str | None = None,
    last_seen_before: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    if console is None:
        console = Console()

    _setup_file_logging(config)

    conn = get_db(config)

    stage_names = stages or list(ALL_STAGES.keys())
    results: dict[str, int] = {}

    # Only initialise AI backend if we need it
    NO_AI_STAGES = {"extract_base", "fetch_homepages"}
    needs_ai = any(s not in NO_AI_STAGES for s in stage_names)
    backend = None
    if needs_ai:
        backend = get_backend(config)
        console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")
        logger.info("Using AI backend: %s", backend.model_name)
    else:
        console.print("Running non-AI stages only")
        logger.info("Running non-AI stages only")

    # Show what's being processed
    if company:
        console.print(f"Scoped to company: [bold]{company}[/bold]")
    if company_list:
        console.print(f"Company list: [bold]{len(company_list)} companies from file[/bold]")
    if label:
        console.print(f"Scoped to label: [bold]{label}[/bold]")
    if stale_before:
        console.print(f"Stale before: [bold]{stale_before}[/bold]")
    if last_seen_after:
        console.print(f"Last seen after: [bold]{last_seen_after}[/bold]")
    if last_seen_before:
        console.print(f"Last seen before: [bold]{last_seen_before}[/bold]")
    if clean:
        console.print("[yellow]Clean mode: previous output will be deleted before reprocessing[/yellow]")

    logger.info("Pipeline started — stages: %s", ", ".join(stage_names))

    def _resolve_company_domains() -> list[str] | None:
        """Resolve the list of company domains to process, or None for default filtering."""
        from email_manager.db import fetchall

        conditions = []
        params: list[str] = []

        if company_list:
            lowered = [v.lower() for v in company_list]
            placeholders = ", ".join("?" for _ in lowered)
            conditions.append(f"(LOWER(c.domain) IN ({placeholders}) OR LOWER(c.name) IN ({placeholders}))")
            params.extend(lowered)
            params.extend(lowered)

        if label:
            conditions.append("c.id IN (SELECT company_id FROM company_labels WHERE label = ?)")
            params.append(label)

        if stale_before:
            # Companies whose latest milestone evaluation is before the cutoff,
            # OR companies with no analysed discussions yet
            conditions.append("""(
                NOT EXISTS (
                    SELECT 1 FROM discussions d
                    JOIN milestones m ON m.discussion_id = d.id
                    WHERE d.company_id = c.id
                )
                OR c.id IN (
                    SELECT d.company_id FROM discussions d
                    LEFT JOIN milestones m ON m.discussion_id = d.id
                    GROUP BY d.company_id
                    HAVING MAX(m.last_evaluated_at) < ? OR MAX(m.last_evaluated_at) IS NULL
                )
            )""")
            params.append(stale_before)

        if last_seen_after:
            conditions.append("c.last_seen >= ?")
            params.append(last_seen_after)

        if last_seen_before:
            conditions.append("(c.last_seen < ? OR c.last_seen IS NULL)")
            params.append(last_seen_before)

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

    # Resolve companies if filtering by label, stale_before, last_seen, or company_list
    target_domains = None
    if (label or stale_before or last_seen_after or last_seen_before or company_list) and not company:
        target_domains = _resolve_company_domains()
        if target_domains is not None:
            console.print(f"[bold]Targeting {len(target_domains)} companies[/bold]")

    if dry_run:
        if company:
            console.print(f"\n[bold]Dry run — would process 1 company:[/bold]")
            console.print(f"  {company}")
        elif target_domains is not None:
            console.print(f"\n[bold]Dry run — would process {len(target_domains)} companies:[/bold]")
            from email_manager.db import fetchall as _fa
            for domain in target_domains:
                row = _fa(conn, "SELECT name, email_count FROM companies WHERE domain = ? COLLATE NOCASE", (domain,))
                name = row[0]["name"] if row else "?"
                emails = row[0]["email_count"] if row else 0
                # Get last analysis date
                last = _fa(conn,
                    "SELECT MAX(d.updated_at) as last_update FROM discussions d JOIN companies c ON d.company_id = c.id WHERE c.domain = ? COLLATE NOCASE",
                    (domain,))
                last_date = (last[0]["last_update"] or "never")[:10] if last else "never"
                console.print(f"  {domain:<35s} {name:<25s} {emails:>6} emails  last analysed: {last_date}")
        else:
            console.print("\n[bold]Dry run — no company filter, would process all companies[/bold]")
        console.print(f"\nStages: {', '.join(stage_names)}")
        console.print(f"Flags: force={force}, clean={clean}, per_company={per_company}")
        conn.close()
        return {}

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
