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
    concurrency: int = 1,
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
        if "concurrency" in sig.parameters and concurrency > 1:
            kwargs["concurrency"] = concurrency
        count = stage_fn(conn, backend, config, **kwargs)
        logger.info("Finished stage: %s — processed %d items", stage_name, count)
        return count
    except Exception as e:
        console.print(f"  [red]{stage_name} failed: {e}[/red]")
        logger.exception("Stage %s failed", stage_name)
        # Record the error in processing_runs so we can see it later
        if company:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            mode = f"staged:{stage_name}"
            model_name = backend.model_name if backend else "unknown"
            try:
                conn.execute(
                    """INSERT INTO processing_runs (company_domain, mode, model, started_at, completed_at, error)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (company, mode, model_name, now, now, str(e)[:500]),
                )
                conn.commit()
            except Exception:
                pass  # Don't fail on error recording
        return -1


# Stages that should run once globally in per-company mode, not per company
GLOBAL_STAGES = {"extract_base"}


def _is_stage_stale(
    conn: Any, domain: str, stage: str, backend: Any,
    check_model: bool, check_prompt: bool,
) -> bool:
    """Check if a specific stage needs forcing for a specific company."""
    if not check_model and not check_prompt:
        return False

    from email_manager.db import fetchone
    mode = f"staged:{stage}"
    run = fetchone(
        conn,
        "SELECT model, prompt_hash FROM processing_runs WHERE company_domain = ? AND mode = ? ORDER BY id DESC LIMIT 1",
        (domain, mode),
    )
    if not run:
        return True  # never run = stale

    if check_model and backend:
        if run.get("model") != backend.model_name:
            return True

    if check_prompt and run.get("prompt_hash"):
        from email_manager.analysis.feedback import compute_prompt_hash, format_rules_block
        current_hash = None
        if stage == "extract_events":
            from email_manager.ai.prompts import EXTRACT_EVENTS_SYSTEM
            current_hash = compute_prompt_hash(EXTRACT_EVENTS_SYSTEM + format_rules_block(conn, "events"))
        elif stage == "analyse_discussions":
            from email_manager.analysis.analyse_discussions import ANALYSE_SYSTEM
            current_hash = compute_prompt_hash(ANALYSE_SYSTEM + format_rules_block(conn, "discussion_updates"))
        elif stage == "propose_actions":
            from email_manager.analysis.propose_actions import PROPOSE_SYSTEM
            current_hash = compute_prompt_hash(PROPOSE_SYSTEM + format_rules_block(conn, "actions"))
        elif stage == "label_companies":
            from email_manager.analysis.company_labels import _build_system_prompt, load_label_config
            labels_config = load_label_config()
            current_hash = compute_prompt_hash(_build_system_prompt(labels_config) + format_rules_block(conn, "labels"))
        elif stage == "discover_discussions":
            from email_manager.analysis.discover_discussions import DISCOVER_SYSTEM
            current_hash = compute_prompt_hash(DISCOVER_SYSTEM)
        if current_hash and current_hash != run["prompt_hash"]:
            return True

    return False


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
    concurrency: int = 1,
    only_new_emails: bool = False,
    only_stale_prompt: bool = False,
    only_stale_model: bool = False,
    only_unprocessed: bool = False,
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
        if "label_companies" in stage_names:
            console.print("[yellow]  Note: --label filter uses existing labels to select companies. "
                          "label_companies will re-label those companies but won't discover new ones.[/yellow]")
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
            f"SELECT c.domain FROM companies c WHERE {where} GROUP BY c.domain ORDER BY MAX(c.email_count) DESC",
            tuple(params),
        )
        resolved = {r[0].lower(): r[0] for r in rows}

        # If a company file was provided, preserve its order
        if company_list:
            ordered = []
            for entry in company_list:
                low = entry.lower()
                if low in resolved:
                    ordered.append(resolved.pop(low))
            # Append any extras matched by other filters but not in the file
            ordered.extend(resolved.values())
            domains = ordered
        else:
            domains = list(resolved.values())

        if limit:
            domains = domains[:limit]
        return domains

    def _filter_by_staleness(domains: list[str]) -> list[str]:
        """Post-filter domains by staleness criteria (new emails, prompt change, model change, unprocessed)."""
        from email_manager.db import fetchall, fetchone
        from email_manager.analysis.feedback import compute_prompt_hash, format_rules_block

        # Determine which modes to check based on requested stages
        modes_to_check = []
        for s in stage_names:
            if s not in GLOBAL_STAGES:
                modes_to_check.append(f"staged:{s}")

        # Compute current prompt hashes for comparison
        current_hashes: dict[str, str] = {}
        if only_stale_prompt and backend:
            for s in stage_names:
                if s == "extract_events":
                    from email_manager.ai.prompts import EXTRACT_EVENTS_SYSTEM
                    current_hashes[f"staged:{s}"] = compute_prompt_hash(
                        EXTRACT_EVENTS_SYSTEM + format_rules_block(conn, "events"))
                elif s == "analyse_discussions":
                    from email_manager.analysis.analyse_discussions import ANALYSE_SYSTEM
                    current_hashes[f"staged:{s}"] = compute_prompt_hash(
                        ANALYSE_SYSTEM + format_rules_block(conn, "discussion_updates"))
                elif s == "propose_actions":
                    from email_manager.analysis.propose_actions import PROPOSE_SYSTEM
                    current_hashes[f"staged:{s}"] = compute_prompt_hash(
                        PROPOSE_SYSTEM + format_rules_block(conn, "actions"))
                elif s == "label_companies":
                    from email_manager.analysis.company_labels import _build_system_prompt, load_label_config
                    labels_config = load_label_config(getattr(config, "company_labels_path", None))
                    current_hashes[f"staged:{s}"] = compute_prompt_hash(
                        _build_system_prompt(labels_config) + format_rules_block(conn, "labels"))
                elif s == "discover_discussions":
                    from email_manager.analysis.discover_discussions import DISCOVER_SYSTEM
                    current_hashes[f"staged:{s}"] = compute_prompt_hash(DISCOVER_SYSTEM)

        current_model = backend.model_name if backend else None

        filtered = []
        for domain in domains:
            # Get latest run for each relevant mode
            latest_runs = fetchall(
                conn,
                """SELECT mode, model, prompt_hash, email_cutoff_date
                   FROM processing_runs
                   WHERE company_domain = ?
                   ORDER BY id DESC""",
                (domain,),
            )
            # Index by mode (first occurrence = latest)
            by_mode: dict[str, dict] = {}
            for r in latest_runs:
                if r["mode"] not in by_mode:
                    by_mode[r["mode"]] = dict(r) if hasattr(r, 'keys') else r

            include = False

            if only_unprocessed:
                # Include if any requested stage has no runs
                for mode in modes_to_check:
                    if mode not in by_mode:
                        include = True
                        break

            if only_new_emails and not include:
                # Include if company has emails newer than latest email_cutoff_date
                latest_cutoff = None
                for mode in modes_to_check:
                    run = by_mode.get(mode)
                    if run and run.get("email_cutoff_date"):
                        if latest_cutoff is None or run["email_cutoff_date"] > latest_cutoff:
                            latest_cutoff = run["email_cutoff_date"]
                if latest_cutoff is None:
                    include = True  # never processed
                else:
                    like = f"%@{domain}%"
                    newer = fetchone(
                        conn,
                        "SELECT 1 FROM emails WHERE (from_address LIKE ? OR to_addresses LIKE ?) AND date > ? LIMIT 1",
                        (like, like, latest_cutoff),
                    )
                    if newer:
                        include = True

            if only_stale_prompt and not include:
                # Include if any stage's current prompt hash differs from latest run
                for mode, cur_hash in current_hashes.items():
                    run = by_mode.get(mode)
                    if not run or run.get("prompt_hash") != cur_hash:
                        include = True
                        break

            if only_stale_model and not include and current_model:
                # Include if any stage was last run with a different model, or never run
                for mode in modes_to_check:
                    run = by_mode.get(mode)
                    if not run or run.get("model") != current_model:
                        include = True
                        break

            if include:
                filtered.append(domain)

        return filtered

    # Resolve companies if filtering by label, stale_before, last_seen, or company_list
    any_staleness_filter = only_new_emails or only_stale_prompt or only_stale_model or only_unprocessed
    target_domains = None
    if (label or stale_before or last_seen_after or last_seen_before or company_list or any_staleness_filter) and not company:
        target_domains = _resolve_company_domains()
        # If no other filters narrowed it, start with all companies for staleness filtering
        if target_domains is None and any_staleness_filter:
            from email_manager.db import fetchall as _fa_all
            target_domains = [r[0] for r in _fa_all(
                conn, "SELECT domain FROM companies ORDER BY email_count DESC",
            )]
        if target_domains is not None and any_staleness_filter:
            before_count = len(target_domains)
            target_domains = _filter_by_staleness(target_domains)
            reasons = []
            if only_new_emails:
                reasons.append("new emails")
            if only_stale_prompt:
                reasons.append("stale prompt")
            if only_stale_model:
                reasons.append("stale model")
            if only_unprocessed:
                reasons.append("unprocessed")
            console.print(f"[bold]Staleness filter ({', '.join(reasons)}): {before_count} → {len(target_domains)} companies[/bold]")
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
                               limit=limit, force=force, clean=clean, concurrency=concurrency)
            results[stage_name] = count

        for i, domain in enumerate(target_domains):
            # Show company header with latest email date
            from email_manager.db import fetchone as _fo
            like = f"%@{domain}%"
            latest_email = _fo(
                conn,
                "SELECT MAX(date) as d FROM emails WHERE from_address LIKE ? OR to_addresses LIKE ?",
                (like, like),
            )
            latest_str = f"  latest email: {latest_email['d'][:10]}" if latest_email and latest_email["d"] else ""
            console.print(f"\n{'='*60}")
            console.print(f"  [bold cyan]Company {i+1}/{len(target_domains)}: {domain}[/bold cyan]{latest_str}")
            console.print(f"{'='*60}")

            for stage_name in per_co_stages:
                # Only force stages that are actually stale for this company
                sf = force or _is_stage_stale(conn, domain, stage_name, backend,
                                              only_stale_model, only_stale_prompt)
                count = _run_stage(stage_name, conn, backend, config, console,
                                   limit=None, force=sf, clean=clean, company=domain, concurrency=concurrency)
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
                    sf = force or _is_stage_stale(conn, domain, stage_name, backend,
                                                  only_stale_model, only_stale_prompt)
                    c = _run_stage(stage_name, conn, backend, config, console,
                                   limit=None, force=sf, clean=clean, company=domain, concurrency=concurrency)
                    count += max(c, 0)
            results[stage_name] = count
    else:
        # Single company or no filtering — original behavior
        for stage_name in stage_names:
            count = _run_stage(stage_name, conn, backend, config, console,
                               limit=limit, force=force, clean=clean,
                               company=company, label=label, exclude=exclude, contact=contact,
                               concurrency=concurrency)
            results[stage_name] = count

    logger.info("Pipeline finished — results: %s", results)
    conn.close()
    return results
