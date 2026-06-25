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
            domains = [r[0] for r in rows]
            log.info(
                "%d dirty companies match label filter %s",
                len(domains),
                label_filter,
            )
        else:
            log.info("%d dirty companies", len(domains))

        return domains
    finally:
        conn.close()


@task(
    name="process-company-ai",
    retries=1,
    retry_delay_seconds=300,
    task_run_name="{domain}",
)
def process_company_ai(domain: str, stages: list[str] | None = None) -> dict[str, int]:
    """Run the AI interpretation pipeline for a single company.

    Uses a Prefect concurrency limit to cap simultaneous LLM calls across
    all parallel company tasks.
    """
    from prefect.concurrency.sync import concurrency as prefect_concurrency

    from .config import SETTINGS

    log = get_run_logger()
    _stages = list(stages or SETTINGS.ai_stages)

    with prefect_concurrency(SETTINGS.ai_concurrency_limit_name, occupy=1):
        from email_manager.config import Config
        from email_manager.pipeline.runner import run_pipeline

        config = Config()
        results = run_pipeline(config, stages=_stages, company=domain)
        log.info("AI pipeline %s: %s", domain, results)
        return results
