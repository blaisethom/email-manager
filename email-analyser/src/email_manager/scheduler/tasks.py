"""Prefect tasks wrapping existing ingestion and pipeline functions.

Each task is self-contained: it opens its own DB connection, does its work,
commits, and closes. Tasks never share connections across task boundaries.
"""
from __future__ import annotations

import logging
from typing import Any

from prefect import get_run_logger, task

logger = logging.getLogger("email_manager.scheduler.tasks")


def _cfg_and_conn():
    """Return a fresh (Config, connection) pair. Call at the start of each task."""
    from email_manager.config import Config
    from email_manager.db import get_db

    config = Config()
    conn = get_db(config)
    return config, conn


def _select_by_budget(
    rows: list,  # each row: (domain, estimated_tokens)
    token_budget: int,
) -> tuple[list[str], int]:
    """Greedily pick companies from rows until token_budget is exhausted.

    Always includes at least one company even if it exceeds the budget alone,
    so very large companies are never permanently skipped.
    Returns (selected_domains, total_estimated_tokens).
    """
    selected: list[str] = []
    total = 0
    for domain, est in rows:
        if selected and total + est > token_budget:
            break
        selected.append(domain)
        total += est
    return selected, total


# ── Ingestion tasks ──────────────────────────────────────────────────────────


@task(name="sync-email-account", retries=2, retry_delay_seconds=120, timeout_seconds=600)
def sync_email_account(account_name: str) -> dict[str, Any]:
    """Sync one email account (Gmail or IMAP)."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        accounts = [a for a in config.get_accounts() if (a.name or "") == account_name]
        if not accounts:
            log.warning("No account found with name %r", account_name)
            return {"account": account_name, "new": 0, "failed": 0}

        acct = accounts[0]
        if acct.backend == "gmail":
            from email_manager.ingestion.gmail_client import sync_emails
            new, failed = sync_emails(conn, acct)
        else:
            from email_manager.ingestion.imap_client import sync_emails
            new, failed = sync_emails(conn, acct)

        conn.commit()
        log.info("Synced %s: %d new, %d failed", account_name, new, failed)
        return {"account": account_name, "new": new, "failed": failed}
    finally:
        conn.close()


@task(name="sync-calendar", retries=1, retry_delay_seconds=60)
def sync_calendar() -> dict[str, Any]:
    """Sync Google Calendar events for all Gmail accounts."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        from email_manager.ingestion.calendar_client import sync_calendar_events

        total = 0
        for acct in config.get_accounts():
            if acct.backend == "gmail":
                n = sync_calendar_events(conn, acct)
                total += n
                log.info("Calendar %s: %d events", acct.name or "gmail", n)
        conn.commit()
        return {"synced": total}
    except Exception as exc:
        log.warning("Calendar sync failed (non-fatal): %s", exc)
        return {"synced": 0, "error": str(exc)}
    finally:
        conn.close()


@task(name="sync-hubspot", retries=2, retry_delay_seconds=120)
def sync_hubspot() -> dict[str, Any]:
    """Full HubSpot sync: companies → contacts → deals → notes → email engagements
    → deal discussions → repair thread links → link notes to discussions."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        from email_manager.integrations.hubspot import (
            HubSpotClient,
            link_notes_to_deal_discussions,
            repair_deal_discussion_threads,
            sync_companies,
            sync_contacts,
            sync_deal_discussions,
            sync_deals,
            sync_email_engagements,
            sync_notes,
        )

        hs_account = config.get_hubspot_account()
        bearer = hs_account.hubspot_bearer_token if hs_account else "HUBSPOT_TOKEN"
        client = HubSpotClient(bearer_token=bearer)

        n_companies, n_company_links = sync_companies(conn, client)
        n_contacts = sync_contacts(conn, client)
        n_deals = sync_deals(conn, client)
        n_notes = sync_notes(conn, client)
        n_engagements = sync_email_engagements(conn, client)
        n_discussions = sync_deal_discussions(conn)
        n_repaired = repair_deal_discussion_threads(conn)
        n_note_links = link_notes_to_deal_discussions(conn)
        conn.commit()

        result = {
            "companies": n_companies,
            "contacts": n_contacts,
            "deals": n_deals,
            "notes": n_notes,
            "email_engagements": n_engagements,
            "deal_discussions": n_discussions,
            "repaired_threads": n_repaired,
            "note_links": n_note_links,
        }
        log.info("HubSpot sync complete: %s", result)
        return result
    finally:
        conn.close()


# ── Enrichment tasks ─────────────────────────────────────────────────────────


@task(name="run-extract-base", retries=1, retry_delay_seconds=60)
def run_extract_base() -> int:
    """Parse raw emails into threads, contacts, and company records."""
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        from email_manager.analysis.base_extract import extract_base

        count = extract_base(conn)
        conn.commit()
        log.info("extract_base: %d items processed", count)
        return count
    finally:
        conn.close()


@task(name="run-hubspot-task-enrichment", retries=1, retry_delay_seconds=60)
def run_hubspot_task_enrichment() -> int:
    """Link HubSpot tasks to email threads."""
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        from email_manager.integrations.hubspot import enrich_tasks_with_threads

        count = enrich_tasks_with_threads(conn)
        conn.commit()
        log.info("hubspot_task_enrichment: %d pairs linked", count)
        return count
    finally:
        conn.close()


@task(name="run-fetch-homepages", retries=1, retry_delay_seconds=60)
def run_fetch_homepages() -> int:
    """Fetch missing company homepages."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        from email_manager.analysis.homepage import fetch_homepages

        max_workers = getattr(config, "homepage_max_workers", 4)
        count = fetch_homepages(conn, max_workers=max_workers)
        conn.commit()
        log.info("fetch_homepages: %d pages fetched", count)
        return count
    finally:
        conn.close()


@task(name="run-label-companies", retries=1, retry_delay_seconds=60)
def run_label_companies(limit: int | None = None, random_sample: bool = False) -> int:
    """Classify unlabelled companies using AI."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        from email_manager.ai.factory import get_backend
        from email_manager.analysis.company_labels import label_companies, load_label_config

        backend = get_backend(config)
        labels_config = load_label_config(getattr(config, "company_labels_path", None))
        count = label_companies(
            conn, backend, labels_config=labels_config,
            limit=limit, random_sample=random_sample,
        )
        conn.commit()
        log.info("label_companies: %d companies labelled", count)
        return count
    finally:
        conn.close()


@task(name="run-build-search-index", retries=1, retry_delay_seconds=60)
def run_build_search_index(force: bool = False, skip_embeddings: bool = False) -> int:
    """Rebuild the thread and discussion search index and optionally embeddings."""
    log = get_run_logger()
    config, conn = _cfg_and_conn()
    try:
        from email_manager.search.indexer import (
            build_discussion_search_index,
            build_search_index,
            generate_discussion_embeddings,
            generate_embeddings,
        )

        count = build_search_index(conn, force=force)
        count += build_discussion_search_index(conn, force=force)

        if not skip_embeddings:
            try:
                count += generate_embeddings(conn, config, force=force)
                count += generate_discussion_embeddings(conn, config, force=force)
            except Exception as exc:
                log.warning("Embedding generation skipped: %s", exc)
        else:
            log.info("Embedding generation skipped (skip_embeddings=True)")

        conn.commit()
        log.info("build_search_index: %d items indexed", count)
        return count
    finally:
        conn.close()


# ── AI pipeline tasks ────────────────────────────────────────────────────────


def _filter_domains_by_label(conn, domains: list[str], label_filter: list[str]) -> list[str]:
    """Filter a list of domains to only those whose top label is in label_filter."""
    if not domains or not label_filter:
        return domains
    placeholders_d = ",".join("?" for _ in domains)
    placeholders_l = ",".join("?" for _ in label_filter)
    rows = conn.execute(
        f"""SELECT c.domain FROM companies c
            WHERE c.domain IN ({placeholders_d})
              AND c.domain IS NOT NULL
              AND (
                  SELECT LOWER(cl.label)
                  FROM company_labels cl
                  WHERE cl.company_id = c.id
                  ORDER BY COALESCE(cl.confidence, 0) DESC, cl.assigned_at DESC
                  LIMIT 1
              ) IN ({placeholders_l})""",
        [*domains, *[lbl.lower() for lbl in label_filter]],
    ).fetchall()
    return [r[0] for r in rows]


@task(name="get-dirty-companies", retries=1, retry_delay_seconds=30)
def get_dirty_companies(label_filter: list[str] | None = None) -> list[str]:
    """Return company domains that have unprocessed changes in the journal.

    If label_filter is given, only returns domains whose top label (highest
    confidence) is one of the specified labels (case-insensitive).
    """
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        from email_manager.change_journal import get_dirty_company_domains

        domains = get_dirty_company_domains(conn)

        if label_filter and domains:
            domains = _filter_domains_by_label(conn, domains, label_filter)
            log.info("%d dirty companies match label filter %s", len(domains), label_filter)
        else:
            log.info("%d dirty companies", len(domains))

        return domains
    finally:
        conn.close()


@task(name="get-dirty-companies-with-event-counts", retries=1, retry_delay_seconds=30)
def get_dirty_companies_with_event_counts(
    label_filter: list[str] | None = None,
) -> list[dict]:
    """Return dirty companies with their unassigned event count.

    Each entry: {domain, event_count}. event_count is the number of unassigned
    events in event_ledger linked to the company via email address patterns —
    this is the primary cost driver for discover_discussions.

    Companies with 0 unassigned events are included; they're cheap (extract_events
    only) and have a nominal cost assigned by the caller for budget purposes.

    Result sorted by event_count ascending so small companies are processed first
    in a greedy budget fill.
    """
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        from email_manager.change_journal import get_dirty_company_domains

        domains = get_dirty_company_domains(conn)

        if label_filter and domains:
            domains = _filter_domains_by_label(conn, domains, label_filter)
            log.info("%d dirty companies match label filter %s", len(domains), label_filter)
        else:
            log.info("%d dirty companies", len(domains))

        result = []
        for domain in domains:
            like = f"%@{domain}%"
            row = conn.execute(
                """SELECT COUNT(DISTINCT el.id)
                   FROM event_ledger el
                   WHERE el.discussion_id IS NULL
                     AND el.thread_id IN (
                         SELECT DISTINCT e.thread_id FROM emails e
                         WHERE e.from_address LIKE ? OR e.to_addresses LIKE ?
                     )""",
                (like, like),
            ).fetchone()
            result.append({"domain": domain, "event_count": row[0] if row else 0})

        result.sort(key=lambda x: x["event_count"])
        total = sum(r["event_count"] for r in result)
        log.info("%d dirty companies, %d total unassigned events", len(result), total)
        return result
    finally:
        conn.close()


@task(name="seed-change-journal", retries=1, retry_delay_seconds=30)
def seed_change_journal(label_filter: list[str]) -> int:
    """Add labelled companies that need analysis to the change journal.

    Seeds companies that:
    - Have never been through analyse_discussions AND have at least one discussion, OR
    - Have discussions with events but no milestone evaluation yet, OR
    - Have discussions with events newer than the last milestone evaluation, OR
    - Are marked stale by the job manager (new emails since last extract_events run).

    Returns the number of companies seeded.
    """
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        from email_manager.change_journal import record_changes

        placeholders_l = ",".join("?" for _ in label_filter)
        rows = conn.execute(
            f"""SELECT DISTINCT c.domain FROM companies c
                JOIN company_labels cl ON cl.company_id = c.id
                WHERE LOWER(cl.label) IN ({placeholders_l})
                  AND c.domain IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM change_journal cj
                    WHERE cj.entity_type = 'company'
                      AND cj.entity_id = c.domain
                      AND cj.change_type = 'seed_for_analysis'
                      AND cj.processed_at IS NULL
                  )
                  AND (
                    (
                      NOT EXISTS (
                        SELECT 1 FROM processing_runs pr
                        WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                          AND pr.mode = 'staged:analyse_discussions'
                          AND pr.error IS NULL
                      )
                      AND EXISTS (
                        SELECT 1 FROM discussions d WHERE d.company_id = c.id
                      )
                    )
                    OR EXISTS (
                      SELECT 1 FROM discussions d
                      JOIN event_ledger el ON el.discussion_id = d.id
                      WHERE d.company_id = c.id
                        AND NOT EXISTS (
                          SELECT 1 FROM milestones m WHERE m.discussion_id = d.id
                        )
                    )
                    OR EXISTS (
                      SELECT 1 FROM discussions d
                      JOIN event_ledger el ON el.discussion_id = d.id
                      WHERE d.company_id = c.id
                        AND el.created_at > COALESCE(
                            (SELECT MAX(m2.last_evaluated_at) FROM milestones m2
                             WHERE m2.discussion_id = d.id),
                            '1970-01-01'
                        )
                    )
                    OR c.staleness_status = 'stale'
                  )""",
            [lbl.lower() for lbl in label_filter],
        ).fetchall()
        domains = [r[0] for r in rows]

        if not domains:
            log.info("No new companies to seed into change journal")
            return 0

        record_changes(conn, [("company", d, "seed_for_analysis", "ai_flow") for d in domains])
        conn.commit()
        log.info("Seeded %d companies into change journal", len(domains))
        return len(domains)
    finally:
        conn.close()


@task(name="get-companies-for-extract-events", retries=1, retry_delay_seconds=30)
def get_companies_for_extract_events(
    label_filter: list[str],
    token_budget: int = 150_000,
    first_run_estimate: int = 30_000,
) -> list[str]:
    """Companies needing extract_events: never processed, new emails since last cutoff, or stale.

    Uses the indexed from_domain column for the email date check (not LIKE scans).
    Selects companies greedily until the token_budget (based on last-run input_tokens) is
    exhausted, so a run processes more small companies or fewer large ones automatically.
    """
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        placeholders_l = ",".join("?" for _ in label_filter)
        # Fetch up to 50 candidates with their last-run token estimate
        rows = conn.execute(
            f"""SELECT DISTINCT c.domain,
                       COALESCE((
                           SELECT pr.input_tokens FROM processing_runs pr
                           WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                             AND pr.mode = 'staged:extract_events'
                             AND pr.error IS NULL
                           ORDER BY pr.id DESC LIMIT 1
                       ), ?) AS estimated_tokens
                FROM companies c
                JOIN company_labels cl ON cl.company_id = c.id
                WHERE LOWER(cl.label) IN ({placeholders_l})
                  AND c.domain IS NOT NULL
                  AND (
                    c.staleness_status = 'stale'
                    OR NOT EXISTS (
                      SELECT 1 FROM processing_runs pr
                      WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                        AND pr.mode = 'staged:extract_events'
                        AND pr.error IS NULL
                    )
                    OR EXISTS (
                      SELECT 1 FROM emails e
                      WHERE e.from_domain = c.domain
                        AND e.date > COALESCE((
                          SELECT pr.email_cutoff_date
                          FROM processing_runs pr
                          WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                            AND pr.mode = 'staged:extract_events'
                            AND pr.error IS NULL
                          ORDER BY pr.id DESC LIMIT 1
                        ), '')
                    )
                  )
                ORDER BY estimated_tokens ASC
                LIMIT 50""",
            [first_run_estimate] + [lbl.lower() for lbl in label_filter],
        ).fetchall()
        selected, total = _select_by_budget(rows, token_budget)
        log.info(
            "%d companies selected for extract_events (est. %d tokens, budget %d)",
            len(selected), total, token_budget,
        )
        return selected
    finally:
        conn.close()


@task(name="get-companies-for-stage", retries=1, retry_delay_seconds=30)
def get_companies_for_stage(
    stage: str,
    prerequisite: str,
    label_filter: list[str],
    token_budget: int = 250_000,
    first_run_estimate: int = 30_000,
) -> list[str]:
    """Companies where prerequisite has a more recent successful run than stage.

    A company is eligible if:
    - prerequisite has at least one successful processing_run, AND
    - stage has never run successfully, OR the latest successful prerequisite run
      has a higher id than the latest successful stage run (id is monotonically
      increasing, so higher id = more recent).

    Selects companies greedily until token_budget (based on last-run input_tokens) is
    exhausted, favouring smaller companies first so more get through per run.
    """
    log = get_run_logger()
    _, conn = _cfg_and_conn()
    try:
        placeholders_l = ",".join("?" for _ in label_filter)
        prereq_mode = f"staged:{prerequisite}"
        stage_mode = f"staged:{stage}"
        rows = conn.execute(
            f"""SELECT DISTINCT c.domain,
                       COALESCE((
                           SELECT pr.input_tokens FROM processing_runs pr
                           WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                             AND pr.mode = ? AND pr.error IS NULL
                           ORDER BY pr.id DESC LIMIT 1
                       ), ?) AS estimated_tokens
                FROM companies c
                JOIN company_labels cl ON cl.company_id = c.id
                WHERE LOWER(cl.label) IN ({placeholders_l})
                  AND c.domain IS NOT NULL
                  AND EXISTS (
                    SELECT 1 FROM processing_runs pr
                    WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                      AND pr.mode = ?
                      AND pr.error IS NULL
                  )
                  AND (
                    NOT EXISTS (
                      SELECT 1 FROM processing_runs pr
                      WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                        AND pr.mode = ?
                        AND pr.error IS NULL
                    )
                    OR (
                      SELECT MAX(pr.id) FROM processing_runs pr
                      WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                        AND pr.mode = ? AND pr.error IS NULL
                    ) > (
                      SELECT MAX(pr.id) FROM processing_runs pr
                      WHERE LOWER(pr.company_domain) = LOWER(c.domain)
                        AND pr.mode = ? AND pr.error IS NULL
                    )
                  )
                ORDER BY estimated_tokens ASC
                LIMIT 50""",
            [stage_mode, first_run_estimate]
            + [lbl.lower() for lbl in label_filter]
            + [prereq_mode, stage_mode, prereq_mode, stage_mode],
        ).fetchall()
        selected, total = _select_by_budget(rows, token_budget)
        log.info(
            "%d companies selected for %s (est. %d tokens, budget %d, prerequisite=%s)",
            len(selected), stage, total, token_budget, prerequisite,
        )
        return selected
    finally:
        conn.close()


@task(
    name="process-company-ai",
    retries=1,
    retry_delay_seconds=60,
    task_run_name="{domain}",
)
def process_company_ai(
    domain: str,
    stages: list[str] | None = None,
    force: bool = False,
    clean: bool = False,
) -> dict[str, int]:
    """Run the AI interpretation pipeline for a single company.

    Uses a Prefect concurrency limit to cap simultaneous LLM calls across
    all parallel company tasks.

    force=True re-runs all stages even if the output appears up-to-date.
    clean=True deletes existing discussions first (used after user feedback).
    """
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    from .config import SETTINGS

    log = get_run_logger()
    _stages = list(stages or SETTINGS.ai_stages)

    with prefect_concurrency(SETTINGS.ai_concurrency_limit_name, occupy=1):
        from email_manager.config import Config
        from email_manager.pipeline.runner import run_pipeline

        config = Config()
        results = run_pipeline(config, stages=_stages, company=domain, force=force, clean=clean)
        log.info("AI pipeline %s (force=%s clean=%s): %s", domain, force, clean, results)
        return results
