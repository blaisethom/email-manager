"""Four Prefect flows for continuous pipeline operation.

┌─────────────────────────────────────────────────────────────────┐
│ ingest_flow   every 10 min  Pull raw data from all sources      │
│ enrich_flow   every 30 min  Parse emails, fetch homepages       │
│ label_flow    every 30 min  AI company labelling                 │
│ ai_flow       every 60 min  AI interpretation of dirty companies │
└─────────────────────────────────────────────────────────────────┘

enrich_flow and label_flow are offset by 15 min so labelling starts
after enrichment finishes (enrich at :00/:30, label at :15/:45).

Deploy with:
    prefect deploy --all  # from the email-analyser directory
"""
from __future__ import annotations

import random

from prefect import flow, get_run_logger

from .config import SETTINGS
from .tasks import (
    get_companies_for_extract_events,
    get_companies_for_stage,
    get_dirty_companies,
    get_dirty_companies_with_event_counts,
    process_company_ai,
    run_build_search_index,
    run_extract_base,
    run_fetch_homepages,
    run_hubspot_task_enrichment,
    run_label_companies,
    seed_change_journal,
    sync_calendar,
    sync_email_account,
    sync_hubspot,
)


# ── Ingest flow ──────────────────────────────────────────────────────────────


@flow(name="email-manager-ingest", log_prints=True)
def ingest_flow() -> dict:
    """Pull raw data from all configured sources.

    Runs one task per email account in parallel, then calendar and HubSpot
    concurrently. Safe to run frequently — all underlying sync functions are
    incremental and idempotent.
    """
    log = get_run_logger()

    from email_manager.config import Config

    config = Config()
    accounts = config.get_accounts()

    # One task per email account — parallel
    email_futures = {
        acct.name or f"account-{i}": sync_email_account.submit(acct.name or f"account-{i}")
        for i, acct in enumerate(accounts)
    }

    # Calendar and HubSpot start immediately alongside email sync
    cal_future = sync_calendar.submit()
    hs_future = sync_hubspot.submit()

    # Collect results — swallow individual failures so one bad account
    # doesn't abort the whole flow
    email_results = {
        name: future.result(raise_on_failure=False)
        for name, future in email_futures.items()
    }
    cal_result = cal_future.result(raise_on_failure=False)
    hs_result = hs_future.result(raise_on_failure=False)

    result = {
        "email": email_results,
        "calendar": cal_result,
        "hubspot": hs_result,
    }
    log.info("Ingest complete: %s", result)
    return result


# ── Enrich flow ──────────────────────────────────────────────────────────────


@flow(name="email-manager-enrich", log_prints=True, timeout_seconds=3600)
def enrich_flow() -> dict:
    """Parse emails, fetch non-AI enrichment data, and rebuild the search index.

    Stage order:
      1. extract_base  — parse raw emails into threads/contacts/companies
      2. hubspot_task_enrichment + fetch_homepages — parallel, no AI calls
      3. build_search_index — incremental text index (no embeddings)

    Holds the 'memory-heavy' concurrency slot (limit=1) for its full duration
    so it never overlaps with ai_flow and causes OOM.
    """
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    log = get_run_logger()

    with prefect_concurrency("memory-heavy", occupy=1, strict=True):
        import time
        time.sleep(20)  # Brief pause after slot acquisition so prior process finishes cleanup
        base_count = run_extract_base()

        hs_future = run_hubspot_task_enrichment.submit()
        hp_future = run_fetch_homepages.submit()

        hs_result = hs_future.result(raise_on_failure=False)
        hp_result = hp_future.result(raise_on_failure=False)

        si_count = run_build_search_index(skip_embeddings=True)

    result = {
        "extract_base": base_count,
        "hubspot_task_enrichment": hs_result,
        "fetch_homepages": hp_result,
        "build_search_index": si_count,
    }
    log.info("Enrich complete: %s", result)
    return result


# ── Label flow (AI model calls) ──────────────────────────────────────────────


@flow(name="email-manager-label", log_prints=True, timeout_seconds=600)
def label_flow(batch_size: int = 10, random_sample: bool = True) -> dict:
    """Label companies with AI.

    Runs after enrich_flow completes (offset by 15 min in schedule).
    Search index is built in enrich_flow; this flow only does AI labelling.

    batch_size: max companies to label per run (default 10)
    random_sample: pick randomly from unlabelled set (default True)
    """
    log = get_run_logger()

    lc_result = run_label_companies(limit=batch_size, random_sample=random_sample)

    result = {"label_companies": lc_result}
    log.info("Label complete: %s", result)
    return result


# ── AI flow ──────────────────────────────────────────────────────────────────


_DEFAULT_ANALYSIS_LABELS = ["investor", "cro", "pharma", "clinic"]


@flow(name="email-manager-ai", log_prints=True, timeout_seconds=1200)
def ai_flow(
    event_budget: int = 100,
    solo_threshold: int = 50,
    random_sample: bool = True,
    label_filter: list[str] | None = None,
    seed_unprocessed: bool = False,
) -> dict:
    """Run AI interpretation for companies with unprocessed changes.

    Batches by total unassigned event count rather than company count, so a
    handful of small companies fit in one run while a large one gets the whole
    budget to itself.

    Batching rules:
      - Companies with >= solo_threshold unassigned events run alone (one per
        flow run) so they don't crowd out other companies or starve memory.
      - Otherwise, companies are added greedily (sorted ascending by event count
        after an optional random shuffle) until event_budget is reached. Companies
        with 0 unassigned events are counted as 5 toward the budget (they're fast
        but not free — they still run extract_events and contact_memory).
      - Large companies are automatically split across runs: discover_discussions
        caps at 200 events per company run, so each subsequent run processes the
        next chunk of unassigned events.

    label_filter restricts which companies are eligible — only those whose
    top-confidence label matches one of the given values. Defaults to
    investor / cro / pharma / clinic.

    seed_unprocessed: when True, first adds all labelled companies that have
    never been through analyse_discussions to the change journal.

    Set up the concurrency limit before first run:
        prefect concurrency-limit create ai-llm 3
    """
    log = get_run_logger()
    _ZERO_EVENT_COST = 5  # nominal budget cost for companies with no unassigned events

    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    if _label_filter:
        seeded = seed_change_journal(_label_filter)
        if seeded:
            log.info("Seeded %d companies with pending analysis into change journal", seeded)

    dirty = get_dirty_companies_with_event_counts(label_filter=_label_filter or None)
    if not dirty:
        log.info("No dirty companies — nothing to do")
        return {"processed": 0, "total_dirty": 0}

    total_dirty = len(dirty)

    # Split into large (solo) and small companies
    large = [d for d in dirty if d["event_count"] >= solo_threshold]
    small = [d for d in dirty if d["event_count"] < solo_threshold]

    if random_sample:
        random.shuffle(large)
        random.shuffle(small)

    if large:
        # Pick one large company and run it alone this cycle
        chosen = large[0]
        batch = [chosen["domain"]]
        log.info(
            "Solo run: %s (%d unassigned events >= solo_threshold %d); "
            "%d other large companies deferred",
            chosen["domain"], chosen["event_count"], solo_threshold, len(large) - 1,
        )
    else:
        # Greedily fill up to event_budget with small companies
        batch = []
        spent = 0
        for d in small:
            cost = d["event_count"] or _ZERO_EVENT_COST
            if spent + cost > event_budget and batch:
                break
            batch.append(d["domain"])
            spent += cost
        deferred = total_dirty - len(batch)
        log.info(
            "Batch of %d companies, ~%d events (budget %d); %d deferred",
            len(batch), spent, event_budget, deferred,
        )

    # Run companies sequentially — holds 'memory-heavy' slot so ai and enrich never
    # overlap. Previously all batch companies were submitted in parallel (.submit()),
    # which spawned up to 20 threads simultaneously even though the 'ai-llm' slot
    # only let 3 run at once. Each waiting thread still held a DB connection and
    # Prefect state, contributing to OOM. Sequential calls mean at most 1 pipeline
    # is live at any time; the 'ai-llm' slot handles global throttling across runs.
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    with prefect_concurrency("memory-heavy", occupy=1, strict=True):
        results = {}
        failed = []
        for domain in batch:
            try:
                outcome = process_company_ai(domain)
                results[domain] = outcome
            except Exception as exc:
                log.warning("AI pipeline failed for %s: %s", domain, exc)
                failed.append(domain)

    if results:
        from email_manager.config import Config
        from email_manager.db import get_db
        from email_manager.change_journal import mark_processed, mark_thread_entries_for_companies

        try:
            conn = get_db(Config())
            processed_domains = list(results.keys())
            marked = mark_processed(conn, entity_type="company", entity_ids=processed_domains)
            marked += mark_thread_entries_for_companies(conn, processed_domains)
            # Clear staleness so OR4 doesn't re-seed companies that just ran through
            # the pipeline (even with 0 results). Jobs.ts will re-mark stale if new
            # emails arrive after this point.
            placeholders = ",".join("?" for _ in processed_domains)
            conn.execute(
                f"UPDATE companies SET staleness_status = 'up_to_date' WHERE domain IN ({placeholders})",
                processed_domains,
            )
            conn.commit()
            conn.close()
            log.info("Marked %d change_journal entries as processed", marked)
        except Exception as exc:
            log.warning("Failed to mark change_journal entries: %s", exc)

    summary = {
        "processed": len(results),
        "failed": len(failed),
        "total_dirty": total_dirty,
        "deferred": total_dirty - len(batch),
    }
    if failed:
        log.warning("Failed domains: %s", failed)
    log.info("AI flow complete: %s", summary)
    return summary


# ── Per-stage flows ───────────────────────────────────────────────────────────
#
# Each flow runs a single pipeline stage for all eligible companies and holds
# the 'memory-heavy' concurrency slot (limit=1) so it never overlaps with
# enrich_flow or another stage flow.
#
# Eligibility is driven entirely by processing_runs: a company enters a stage
# flow's batch when its prerequisite stage has a more recent successful run
# than the current stage (or the current stage has never run). No change_journal
# involvement — each flow is self-contained.
#
# Schedule offsets stagger the flows so they don't all wake up simultaneously
# and immediately collide on the memory-heavy slot:
#   extract_events     */20       :00, :20, :40
#   discover          3,23,43     :03, :23, :43
#   analyse           7,27,47     :07, :27, :47
#   propose           12,32,52    :12, :32, :52
#   contact_memory    17 */2      :17 every 2 h (less time-sensitive)


def _run_stage_flow(
    stage: str,
    batch: list[str],
    log,
) -> tuple[list[str], list[str]]:
    """Common body for all single-stage flows: sequential per-company with memory-heavy slot.

    Returns (succeeded_domains, failed_domains).
    """
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    if not batch:
        log.info("No companies need %s — nothing to do", stage)
        return [], []

    log.info("Running %s for %d companies", stage, len(batch))

    with prefect_concurrency("memory-heavy", occupy=1, strict=True):
        succeeded = []
        failed = []
        for i, domain in enumerate(batch):
            log.info("[%d/%d] %s → %s", i + 1, len(batch), stage, domain)
            try:
                process_company_ai(domain, stages=[stage])
                log.info("[%d/%d] %s ✓ %s", i + 1, len(batch), stage, domain)
                succeeded.append(domain)
            except Exception as exc:
                log.warning("[%d/%d] %s ✗ %s: %s", i + 1, len(batch), stage, domain, exc)
                failed.append(domain)

    return succeeded, failed


@flow(name="email-manager-extract-events", log_prints=True, timeout_seconds=1200)
def extract_events_flow(
    label_filter: list[str] | None = None,
    token_budget: int = 150_000,
) -> dict:
    """Extract events for companies with new emails since their last run.

    Eligibility: staleness_status='stale', emails after last extract_events
    cutoff date, or never processed.

    Companies are selected greedily up to token_budget (based on each company's
    last-run input_tokens), so runs with small companies process more of them
    while a single large company can fill a run on its own.

    Clears staleness_status='up_to_date' after successful processing so the
    company doesn't re-enter the batch before new emails arrive.
    """
    log = get_run_logger()
    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    batch = get_companies_for_extract_events(
        label_filter=_label_filter, token_budget=token_budget
    )
    succeeded, failed = _run_stage_flow("extract_events", batch, log)

    if succeeded:
        from email_manager.config import Config
        from email_manager.db import get_db
        try:
            conn = get_db(Config())
            placeholders = ",".join("?" for _ in succeeded)
            conn.execute(
                f"UPDATE companies SET staleness_status = 'up_to_date'"
                f" WHERE domain IN ({placeholders})",
                succeeded,
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            log.warning("Failed to clear staleness_status: %s", exc)

    summary = {"processed": len(succeeded), "failed": len(failed), "total": len(batch)}
    log.info("extract_events complete: %s", summary)
    return summary


@flow(name="email-manager-discover-discussions", log_prints=True, timeout_seconds=1200)
def discover_discussions_flow(
    label_filter: list[str] | None = None,
    token_budget: int = 150_000,
) -> dict:
    """Discover discussions for companies where extract_events has run more recently.

    Prerequisite: staged:extract_events completed after last staged:discover_discussions.
    """
    log = get_run_logger()
    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    batch = get_companies_for_stage(
        stage="discover_discussions",
        prerequisite="extract_events",
        label_filter=_label_filter,
        token_budget=token_budget,
    )
    succeeded, failed = _run_stage_flow("discover_discussions", batch, log)
    summary = {"processed": len(succeeded), "failed": len(failed), "total": len(batch)}
    log.info("discover_discussions complete: %s", summary)
    return summary


@flow(name="email-manager-analyse-discussions", log_prints=True, timeout_seconds=1200)
def analyse_discussions_flow(
    label_filter: list[str] | None = None,
    token_budget: int = 250_000,
) -> dict:
    """Analyse discussions for companies where discover_discussions has run more recently.

    Prerequisite: staged:discover_discussions completed after last staged:analyse_discussions.
    """
    log = get_run_logger()
    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    batch = get_companies_for_stage(
        stage="analyse_discussions",
        prerequisite="discover_discussions",
        label_filter=_label_filter,
        token_budget=token_budget,
    )
    succeeded, failed = _run_stage_flow("analyse_discussions", batch, log)
    summary = {"processed": len(succeeded), "failed": len(failed), "total": len(batch)}
    log.info("analyse_discussions complete: %s", summary)
    return summary


@flow(name="email-manager-propose-actions", log_prints=True, timeout_seconds=1200)
def propose_actions_flow(
    label_filter: list[str] | None = None,
    token_budget: int = 250_000,
) -> dict:
    """Propose actions for companies where analyse_discussions has run more recently.

    Prerequisite: staged:analyse_discussions completed after last staged:propose_actions.
    """
    log = get_run_logger()
    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    batch = get_companies_for_stage(
        stage="propose_actions",
        prerequisite="analyse_discussions",
        label_filter=_label_filter,
        token_budget=token_budget,
    )
    succeeded, failed = _run_stage_flow("propose_actions", batch, log)
    summary = {"processed": len(succeeded), "failed": len(failed), "total": len(batch)}
    log.info("propose_actions complete: %s", summary)
    return summary


@flow(name="email-manager-contact-memory", log_prints=True, timeout_seconds=1200)
def contact_memory_flow(
    label_filter: list[str] | None = None,
    token_budget: int = 150_000,
) -> dict:
    """Build contact memories for companies where propose_actions has run more recently.

    Prerequisite: staged:propose_actions completed after last staged:contact_memory.
    Runs less frequently than the other stages (every 2 hours) since contact
    memory is less time-sensitive.
    """
    log = get_run_logger()
    _label_filter = label_filter if label_filter is not None else _DEFAULT_ANALYSIS_LABELS

    batch = get_companies_for_stage(
        stage="contact_memory",
        prerequisite="propose_actions",
        label_filter=_label_filter,
        token_budget=token_budget,
    )
    succeeded, failed = _run_stage_flow("contact_memory", batch, log)
    summary = {"processed": len(succeeded), "failed": len(failed), "total": len(batch)}
    log.info("contact_memory complete: %s", summary)
    return summary


# ── Single-company flow ───────────────────────────────────────────────────────


@flow(name="email-manager-company-ai", log_prints=True)
def company_flow(domain: str, stages: list[str] | None = None, force: bool = False, clean: bool = False) -> dict:
    """Run the AI interpretation pipeline for a single company.

    Triggered on demand from the web UI (e.g. after discussion feedback or
    from the company detail page). The optional ``stages`` list restricts
    which pipeline stages run; omit it to run all AI stages.

    force=True re-runs all stages even if output appears up-to-date.
    clean=True deletes existing discussions first so they are re-discovered
    from scratch (used when the user submits new feedback rules).
    """
    log = get_run_logger()
    log.info("Running company AI flow for %s (stages=%s force=%s clean=%s)", domain, stages or "all", force, clean)
    result = process_company_ai(domain, stages=stages or None, force=force, clean=clean)
    log.info("Company AI flow complete for %s: %s", domain, result)
    return result


# ── Maintenance: crash zombie RUNNING flows ───────────────────────────────────


@flow(name="email-manager-maintenance", log_prints=True, timeout_seconds=120)
def maintenance_flow(max_age_seconds: int = 5400) -> dict:
    """Crash zombie RUNNING flows and release their stuck concurrency slots.

    Process workers don't detect orphaned child processes when the worker restarts,
    leaving flows stuck in RUNNING state indefinitely. This blocks deployment
    concurrency limits (concurrency_limit=1) so subsequent scheduled runs can't start.
    Crashing flows via set_state alone is insufficient — deployment and global
    concurrency leases (memory-heavy, ai-llm) aren't automatically released on crash,
    so we reset them explicitly.

    Runs every 30 minutes. Default max_age_seconds=5400 (90 min) covers the
    longest legitimate flow (enrich, timeout=3600) plus a generous margin.
    """
    import urllib.request
    import json
    import os
    from datetime import datetime, timezone, timedelta

    log = get_run_logger()
    api_url = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")

    def _post(path: str, body: dict):
        req = urllib.request.Request(
            f"{api_url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None

    def _patch(path: str, body: dict) -> None:
        req = urllib.request.Request(
            f"{api_url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="PATCH",
        )
        urllib.request.urlopen(req)

    def _set_state(flow_run_id: str, message: str) -> None:
        _post(f"/flow_runs/{flow_run_id}/set_state", {
            "state": {"type": "CRASHED", "name": "Crashed", "message": message},
            "force": True,
        })

    # ── 1. Crash zombie flow runs ──────────────────────────────────────────────
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")

    old_runs = _post("/flow_runs/filter", {
        "limit": 200,
        "flow_runs": {
            "state": {"type": {"any_": ["RUNNING", "PENDING"]}},
            "start_time": {"before_": cutoff_iso},
        },
    }) or []

    crashed = 0
    skipped = 0
    crashed_dep_ids: set = set()
    for run in old_runs:
        if not isinstance(run, dict):
            continue
        age_sec = (datetime.now(timezone.utc) - datetime.fromisoformat(run["start_time"].replace("Z", "+00:00"))).total_seconds()
        log.warning("Crashing zombie %s (age=%.0fs)", run["id"], age_sec)
        try:
            _set_state(run["id"], f"Crashed by maintenance_flow: running {age_sec:.0f}s > max {max_age_seconds}s")
            crashed += 1
            if run.get("deployment_id"):
                crashed_dep_ids.add(run["deployment_id"])
        except Exception as exc:
            log.warning("Failed to crash %s: %s", run["id"], exc)
            skipped += 1

    # ── 2. Release stuck concurrency slots ────────────────────────────────────
    # After crashing, leases (deployment slots, memory-heavy, ai-llm) aren't
    # auto-released. Find all slots where active > 0 and no flow is still running.
    slots_reset = 0
    try:
        still_running = _post("/flow_runs/filter", {
            "limit": 200,
            "flow_runs": {"state": {"type": {"any_": ["RUNNING", "PENDING"]}}},
        }) or []
        running_dep_ids = {r["deployment_id"] for r in still_running if isinstance(r, dict) and r.get("deployment_id")}
        running_state_types = {r.get("state_type") for r in still_running if isinstance(r, dict)}

        all_limits = _post("/v2/concurrency_limits/filter", {"limit": 200}) or []
        for lim in all_limits:
            if not isinstance(lim, dict) or lim.get("active_slots", 0) == 0:
                continue
            name = lim["name"]
            should_reset = False
            if name.startswith("deployment:"):
                dep_id = name.split(":", 1)[1]
                # Reset if no running flow holds this deployment slot
                should_reset = dep_id not in running_dep_ids
            elif name in ("memory-heavy", "ai-llm"):
                # Reset if no RUNNING stage flows exist (PENDING are pre-slot)
                should_reset = "RUNNING" not in running_state_types
            if should_reset:
                log.warning("Resetting stuck concurrency slot: %s (active=%d)", name, lim["active_slots"])
                try:
                    _patch(f"/v2/concurrency_limits/{lim['id']}", {"active_slots": 0})
                    slots_reset += 1
                except Exception as exc:
                    log.warning("Failed to reset slot %s: %s", name, exc)
    except Exception as exc:
        log.warning("Slot cleanup failed: %s", exc)

    result = {
        "crashed": crashed,
        "skipped": skipped,
        "slots_reset": slots_reset,
        "cutoff_age_seconds": max_age_seconds,
    }
    log.info("Maintenance complete: %s", result)
    return result


# ── Convenience: run all three flows sequentially ────────────────────────────


@flow(name="email-manager-full-run", log_prints=True)
def full_run_flow() -> dict:
    """One-shot flow that runs ingest → enrich → label → AI in sequence.

    Useful for initial bootstrapping or ad-hoc full refreshes.
    Not scheduled; trigger manually from the Prefect UI or CLI.
    """
    log = get_run_logger()
    log.info("Starting full pipeline run")

    ingest_result = ingest_flow()
    enrich_result = enrich_flow()
    label_result = label_flow()
    ai_result = ai_flow()

    return {
        "ingest": ingest_result,
        "enrich": enrich_result,
        "label": label_result,
        "ai": ai_result,
    }


if __name__ == "__main__":
    full_run_flow()
