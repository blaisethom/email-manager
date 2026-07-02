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


@flow(name="email-manager-enrich", log_prints=True)
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

    # Fan out — holds 'memory-heavy' slot (limit=1) so ai and enrich don't overlap.
    # Concurrency of individual LLM calls is capped by the 'ai-llm' limit inside each task.
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    with prefect_concurrency("memory-heavy", occupy=1, strict=True):
        futures = {domain: process_company_ai.submit(domain) for domain in batch}

        results = {}
        failed = []
        for domain, future in futures.items():
            outcome = future.result(raise_on_failure=False)
            if isinstance(outcome, Exception):
                log.warning("AI pipeline failed for %s: %s", domain, outcome)
                failed.append(domain)
            else:
                results[domain] = outcome

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


# ── Single-company flow ───────────────────────────────────────────────────────


@flow(name="email-manager-company-ai", log_prints=True)
def company_flow(domain: str, stages: list[str] | None = None, force: bool = False) -> dict:
    """Run the AI interpretation pipeline for a single company.

    Triggered on demand from the web UI (e.g. after discussion feedback or
    from the company detail page). The optional ``stages`` list restricts
    which pipeline stages run; omit it to run all AI stages.

    force=True re-runs all stages even if output appears up-to-date.
    """
    log = get_run_logger()
    log.info("Running company AI flow for %s (stages=%s force=%s)", domain, stages or "all", force)
    result = process_company_ai(domain, stages=stages or None, force=force)
    log.info("Company AI flow complete for %s: %s", domain, result)
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
