"""Three Prefect flows for continuous pipeline operation.

┌─────────────────────────────────────────────────────────────────┐
│ ingest_flow   every 10 min  Pull raw data from all sources      │
│ enrich_flow   every 30 min  Build threads, labels, search index │
│ ai_flow       every 60 min  AI interpretation of dirty companies │
└─────────────────────────────────────────────────────────────────┘

Deploy with:
    prefect deploy --all  # from the email-analyser directory
"""
from __future__ import annotations

from prefect import flow, get_run_logger

from .config import SETTINGS
from .tasks import (
    get_dirty_companies,
    process_company_ai,
    run_build_search_index,
    run_extract_base,
    run_fetch_homepages,
    run_hubspot_task_enrichment,
    run_label_companies,
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
    """Run global enrichment pipeline stages.

    Stage order:
      1. extract_base  — must complete first; threads need raw emails parsed
      2. hubspot_task_enrichment, fetch_homepages, label_companies — parallel
      3. build_search_index — after threads and labels are current
    """
    log = get_run_logger()

    # Step 1: extract_base (sequential dependency for everything else)
    base_count = run_extract_base()

    # Step 2: parallel enrichment (these are independent of each other)
    hs_future = run_hubspot_task_enrichment.submit()
    hp_future = run_fetch_homepages.submit()
    lc_future = run_label_companies.submit()

    hs_result = hs_future.result(raise_on_failure=False)
    hp_result = hp_future.result(raise_on_failure=False)
    lc_result = lc_future.result(raise_on_failure=False)

    # Step 3: rebuild search index now that threads and labels are current
    si_count = run_build_search_index()

    result = {
        "extract_base": base_count,
        "hubspot_task_enrichment": hs_result,
        "fetch_homepages": hp_result,
        "label_companies": lc_result,
        "build_search_index": si_count,
    }
    log.info("Enrich complete: %s", result)
    return result


# ── AI flow ──────────────────────────────────────────────────────────────────


@flow(name="email-manager-ai", log_prints=True)
def ai_flow(batch_size: int | None = None) -> dict:
    """Run AI interpretation for companies with unprocessed changes.

    Fans out one task per company. Tasks compete for the 'ai-llm' Prefect
    concurrency limit (default 3 slots), ensuring we never saturate the
    Claude API regardless of how many companies are queued.

    Set up the limit before first run:
        prefect concurrency-limit create ai-llm 3
    """
    log = get_run_logger()
    limit = batch_size or SETTINGS.ai_company_batch_size

    dirty = get_dirty_companies()
    if not dirty:
        log.info("No dirty companies — nothing to do")
        return {"processed": 0, "total_dirty": 0}

    batch = dirty[:limit]
    skipped = len(dirty) - len(batch)
    if skipped:
        log.info("Queuing %d companies (%d deferred to next run)", len(batch), skipped)
    else:
        log.info("Queuing %d companies", len(batch))

    # Fan out — concurrency is capped by the 'ai-llm' limit inside each task
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

    summary = {
        "processed": len(results),
        "failed": len(failed),
        "total_dirty": len(dirty),
        "deferred": skipped,
    }
    if failed:
        log.warning("Failed domains: %s", failed)
    log.info("AI flow complete: %s", summary)
    return summary


# ── Single-company flow ───────────────────────────────────────────────────────


@flow(name="email-manager-company-ai", log_prints=True)
def company_flow(domain: str, stages: list[str] | None = None) -> dict:
    """Run the AI interpretation pipeline for a single company.

    Triggered on demand from the web UI (e.g. after discussion feedback or
    from the company detail page). The optional ``stages`` list restricts
    which pipeline stages run; omit it to run all AI stages.
    """
    log = get_run_logger()
    log.info("Running company AI flow for %s (stages=%s)", domain, stages or "all")
    result = process_company_ai(domain, stages=stages or None)
    log.info("Company AI flow complete for %s: %s", domain, result)
    return result


# ── Convenience: run all three flows sequentially ────────────────────────────


@flow(name="email-manager-full-run", log_prints=True)
def full_run_flow() -> dict:
    """One-shot flow that runs ingest → enrich → AI in sequence.

    Useful for initial bootstrapping or ad-hoc full refreshes.
    Not scheduled; trigger manually from the Prefect UI or CLI.
    """
    log = get_run_logger()
    log.info("Starting full pipeline run")

    ingest_result = ingest_flow()
    enrich_result = enrich_flow()
    ai_result = ai_flow()

    return {
        "ingest": ingest_result,
        "enrich": enrich_result,
        "ai": ai_result,
    }


if __name__ == "__main__":
    full_run_flow()
