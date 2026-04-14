"""Extract business events from email threads using the event ledger model.

Each thread is processed in a single LLM call that:
1. Classifies the business domain(s) present
2. Extracts fine-grained events using domain-specific vocabulary
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from email_manager.ai.base import LLMBackend
from email_manager.ai.prompts import (
    EXTRACT_EVENTS_SYSTEM,
    EXTRACT_EVENTS_USER,
    EXTRACT_EVENTS_BATCH_SYSTEM,
    EXTRACT_EVENTS_BATCH_USER,
)
from email_manager.change_journal import record_changes
from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.analysis.events")

PROMPT_VERSION = "v2"


# ── Category config ─────────────────────────────────────────────────────────


def load_category_config(config_path: Path | None = None) -> list[dict[str, Any]]:
    """Load discussion category definitions including event_types from YAML."""
    if config_path is None:
        for candidate in (
            Path("discussion_categories.yaml"),
            Path("discussion_categories.yml"),
            Path("data/discussion_categories.yaml"),
        ):
            if candidate.exists():
                config_path = candidate
                break

    if config_path is None or not config_path.exists():
        return []

    text = config_path.read_text()
    data = yaml.safe_load(text)

    if isinstance(data, dict):
        categories = data.get("categories", [])
    else:
        categories = data

    return categories if isinstance(categories, list) else []


def _build_domains_block(categories: list[dict[str, Any]]) -> str:
    """Build the domains + event vocabulary block for the prompt."""
    parts = []
    for cat in categories:
        event_types = cat.get("event_types", [])
        if not event_types:
            continue
        lines = [f'Domain: "{cat["name"]}" — {cat["description"]}']
        lines.append("  Event types:")
        for et in event_types:
            if isinstance(et, dict):
                lines.append(f'    - {et["name"]}: {et["description"]}')
            else:
                lines.append(f"    - {et}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


# ── Quote stripping (reuse from discussions module) ─────────────────────────

_ON_WROTE_RE = re.compile(r"^On .{10,80} wrote:\s*$", re.MULTILINE)


def _strip_quoted_text(body: str) -> str:
    """Remove quoted/forwarded content from an email body."""
    if not body:
        return ""
    lines = body.split("\n")
    cleaned: list[str] = []
    skip_rest = False
    for line in lines:
        stripped = line.strip()
        if skip_rest:
            continue
        if stripped.startswith(">"):
            continue
        if _ON_WROTE_RE.match(stripped):
            skip_rest = True
            continue
        if re.match(r"^-{2,}\s*Original Message\s*-{2,}$", stripped, re.IGNORECASE):
            skip_rest = True
            continue
        if re.match(r"^From:\s+\S+.*", stripped) and cleaned:
            prev = cleaned[-1].strip() if cleaned else ""
            if prev == "" or prev.startswith("--") or prev.startswith("__"):
                skip_rest = True
                continue
        cleaned.append(line)
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()
    return "\n".join(cleaned)


def _dedup_against_previous(body: str, previous_bodies: list[str], min_dup_lines: int = 3) -> str:
    """Remove runs of lines that appeared in previous emails."""
    if not body or not previous_bodies:
        return body
    prev_lines: set[str] = set()
    for pb in previous_bodies:
        for line in pb.split("\n"):
            norm = line.strip().lower()
            if len(norm) > 20:
                prev_lines.add(norm)
    lines = body.split("\n")
    is_dup = [line.strip().lower() in prev_lines for line in lines]
    result: list[str] = []
    i = 0
    while i < len(lines):
        if is_dup[i]:
            run_start = i
            while i < len(lines) and is_dup[i]:
                i += 1
            if i - run_start < min_dup_lines:
                result.extend(lines[run_start:i])
        else:
            result.append(lines[i])
            i += 1
    while result and result[-1].strip() == "":
        result.pop()
    return "\n".join(result)


def _format_thread_emails(emails: list[dict], body_per_email: int = 800) -> list[str]:
    """Format emails for a thread with quote stripping and deduplication."""
    previous_bodies: list[str] = []
    formatted: list[str] = []
    for idx, e in enumerate(emails):
        raw_body = e["body_text"] or ""
        clean_body = _strip_quoted_text(raw_body)
        clean_body = _dedup_against_previous(clean_body, previous_bodies)
        previous_bodies.append(clean_body)
        body = clean_body[:body_per_email]
        sender = e["from_name"] or e["from_address"]
        date = (e["date"] or "")[:10]
        formatted.append(
            f"[Email {idx}] [{date}] From: {sender} <{e['from_address']}> "
            f"To: {e['to_addresses'] or ''}\n"
            f"Subject: {e['subject'] or '(no subject)'}\n"
            f"{body}\n"
        )
    return formatted


# ── Account owner detection ─────────────────────────────────────────────────

def _detect_account_owner(conn: sqlite3.Connection) -> str | None:
    row = fetchone(
        conn,
        "SELECT from_address, COUNT(*) as cnt FROM emails GROUP BY from_address ORDER BY cnt DESC LIMIT 1",
    )
    return row["from_address"] if row else None


# ── Calendar context ────────────────────────────────────────────────────────

def _get_calendar_context(
    conn: sqlite3.Connection, participant_emails: set[str],
    start_date: str | None, end_date: str | None,
) -> str:
    """Find calendar events involving the same participants in the time window."""
    if not participant_emails or not start_date:
        return ""

    # Extend window by 7 days on each side
    events = fetchall(
        conn,
        """SELECT event_id, title, start_time, end_time, attendees, organizer_email
           FROM calendar_events
           WHERE start_time >= date(?, '-7 days') AND start_time <= date(?, '+7 days')
           ORDER BY start_time ASC""",
        (start_date, end_date or start_date),
    )
    if not events:
        return ""

    # Filter to events that share at least one participant
    relevant = []
    for ev in events:
        attendees_str = (ev["attendees"] or "") + " " + (ev["organizer_email"] or "")
        attendee_emails = {a.lower() for a in re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", attendees_str)}
        if attendee_emails & participant_emails:
            relevant.append(ev)

    if not relevant:
        return ""

    lines = ["\nRelated calendar events:"]
    for ev in relevant[:10]:  # limit to 10
        lines.append(
            f"  [{ev['start_time'][:10]}] {ev['title']} "
            f"(attendees: {ev['attendees'] or 'unknown'}, id: {ev['event_id']})"
        )
    return "\n".join(lines)


# ── Core extraction ─────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _get_threads_to_process(
    conn: sqlite3.Connection, limit: int | None = None, force: bool = False,
    company_domain: str | None = None, company_label: str | None = None,
) -> list[str]:
    """Get thread IDs that need event extraction.

    A thread needs processing if it has no events in the ledger yet,
    or if force=True. Can filter by company domain or label.
    """
    # Build company filter
    company_filter = ""
    params: list[Any] = []

    if company_domain:
        like = f"%@{company_domain}%"
        company_filter = """AND e.thread_id IN (
            SELECT DISTINCT e2.thread_id FROM emails e2
            WHERE e2.from_address LIKE ? OR e2.to_addresses LIKE ? OR e2.cc_addresses LIKE ?
        )"""
        params = [like, like, like]
    elif company_label:
        company_filter = """AND e.thread_id IN (
            SELECT DISTINCT e2.thread_id FROM emails e2
            JOIN company_contacts cc ON (e2.from_address = cc.contact_email
                                         OR e2.to_addresses LIKE '%' || cc.contact_email || '%')
            JOIN company_labels cl ON cc.company_id = cl.company_id
            WHERE cl.label = ?
        )"""
        params = [company_label]

    if force:
        sql = f"""SELECT DISTINCT e.thread_id FROM emails e
                 WHERE e.thread_id IS NOT NULL
                 {company_filter}
                 ORDER BY e.date DESC"""
    else:
        # Threads that either:
        # 1. Have no events yet, OR
        # 2. Have new emails since the last extraction
        sql = f"""SELECT DISTINCT e.thread_id
                 FROM emails e
                 WHERE e.thread_id IS NOT NULL
                   AND (
                       e.thread_id NOT IN (
                           SELECT DISTINCT el.thread_id FROM event_ledger el
                           WHERE el.thread_id IS NOT NULL
                       )
                       OR e.thread_id IN (
                           SELECT el2.thread_id FROM event_ledger el2
                           WHERE el2.thread_id IS NOT NULL
                           GROUP BY el2.thread_id
                           HAVING MAX(el2.created_at) < (
                               SELECT MAX(e2.date) FROM emails e2
                               WHERE e2.thread_id = el2.thread_id
                           )
                       )
                   )
                 {company_filter}
                 ORDER BY e.date DESC"""

    if limit:
        sql += f" LIMIT {limit}"

    rows = fetchall(conn, sql, tuple(params))
    return [r["thread_id"] for r in rows]


MAX_EMAILS_PER_CHUNK = 25  # Split threads larger than this into chunks


def _chunk_emails(emails: list[dict], max_per_chunk: int = MAX_EMAILS_PER_CHUNK) -> list[list[dict]]:
    """Split a large email list into overlapping chunks.

    Each chunk gets 1 email of overlap with the previous chunk so the LLM
    has context for what came before.
    """
    if len(emails) <= max_per_chunk:
        return [emails]

    chunks = []
    start = 0
    while start < len(emails):
        end = min(start + max_per_chunk, len(emails))
        chunks.append(emails[start:end])
        if end >= len(emails):
            break
        start = end - 1  # 1 email overlap
    return chunks


def _process_thread(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    thread_id: str,
    categories: list[dict[str, Any]],
    domains_block: str,
    account_owner: str | None,
    system_prompt: str | None = None,
) -> list[dict[str, Any]]:
    """Extract events from a single thread, chunking large threads."""
    all_emails = fetchall(
        conn,
        """SELECT message_id, date, from_address, from_name, to_addresses, cc_addresses,
                  subject, body_text
           FROM emails WHERE thread_id = ? ORDER BY date ASC""",
        (thread_id,),
    )
    if not all_emails:
        return []

    # Get participant emails for calendar context (from all emails)
    participant_emails: set[str] = set()
    for e in all_emails:
        for field in ("from_address", "to_addresses", "cc_addresses"):
            val = e[field]
            if val:
                participant_emails.update(a.lower() for a in _EMAIL_RE.findall(val))

    start_date = all_emails[0]["date"][:10] if all_emails[0]["date"] else None
    end_date = all_emails[-1]["date"][:10] if all_emails[-1]["date"] else None
    calendar_block = _get_calendar_context(conn, participant_emails, start_date, end_date)

    subject = all_emails[0]["subject"] or "(no subject)"
    participants = ", ".join(sorted(participant_emails))
    owner_line = f"\nAccount owner (\"me\"): {account_owner}" if account_owner else ""

    # Process in chunks for large threads
    chunks = _chunk_emails(all_emails)
    all_parsed: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for chunk_idx, emails in enumerate(chunks):
        formatted = _format_thread_emails(emails)
        messages_text = "\n".join(formatted)

        chunk_note = ""
        if len(chunks) > 1:
            chunk_note = f"\n\nNote: This is part {chunk_idx + 1} of {len(chunks)} of a long thread. Extract events from ALL emails shown."

        user_prompt = EXTRACT_EVENTS_USER.format(
            owner_line=owner_line,
            subject=subject,
            participants=participants,
            domains_block=domains_block,
            messages=messages_text,
            calendar_block=calendar_block if chunk_idx == 0 else "",
        ) + chunk_note

        try:
            result = backend.complete_json(system_prompt or EXTRACT_EVENTS_SYSTEM, user_prompt)
        except Exception as e:
            logger.error("LLM call failed for thread %s (chunk %d): %s", thread_id, chunk_idx, e)
            continue

        raw_events = result.get("events", [])

        # Build type-to-domain lookup once
        all_type_to_domain: dict[str, str] = {}
        for cat in categories:
            for et in cat.get("event_types", []):
                type_name = et["name"] if isinstance(et, dict) else et
                all_type_to_domain[type_name] = cat["name"]

        for ev in raw_events:
            event_type = ev.get("type", "")
            domain = ev.get("domain", "")

            # Validate event type
            valid_types_for_domain: set[str] = set()
            for cat in categories:
                if cat["name"] == domain:
                    for et in cat.get("event_types", []):
                        valid_types_for_domain.add(et["name"] if isinstance(et, dict) else et)
                    break

            if event_type not in valid_types_for_domain:
                if event_type in all_type_to_domain:
                    correct_domain = all_type_to_domain[event_type]
                    logger.info(
                        "Reassigning event type '%s' from domain '%s' to '%s' in thread %s",
                        event_type, domain, correct_domain, thread_id,
                    )
                    domain = correct_domain
                else:
                    logger.warning(
                        "Skipping unknown event type '%s' for domain '%s' in thread %s",
                        event_type, domain, thread_id,
                    )
                    continue

            # Resolve source email from this chunk's email list
            source_idx = ev.get("source_email_index")
            source_email_id = None
            if source_idx is not None and 0 <= source_idx < len(emails):
                source_email_id = emails[source_idx]["message_id"]

            source_calendar_id = ev.get("calendar_event_id")

            # Determine source type
            if source_calendar_id:
                src_type, src_id = "calendar", source_calendar_id
            elif source_email_id:
                src_type, src_id = "email", source_email_id
            else:
                src_type, src_id = "email", None

            all_parsed.append({
                "id": f"evt_{uuid.uuid4().hex[:12]}",
                "thread_id": thread_id,
                "source_email_id": source_email_id,
                "source_calendar_event_id": source_calendar_id,
                "source_type": src_type,
                "source_id": src_id,
                "domain": domain,
                "type": event_type,
                "actor": ", ".join(ev["actor"]) if isinstance(ev.get("actor"), list) else ev.get("actor"),
                "target": ", ".join(ev["target"]) if isinstance(ev.get("target"), list) else ev.get("target"),
                "event_date": ev.get("event_date"),
                "detail": ev.get("detail"),
                "confidence": ev.get("confidence", 0.5),
                "model_version": backend.model_name,
                "prompt_version": PROMPT_VERSION,
                "created_at": now,
            })

    if len(chunks) > 1:
        logger.info("Thread %s: %d chunks, %d events before dedup", thread_id, len(chunks), len(all_parsed))

    return _dedup_events(all_parsed)


def _dedup_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove duplicate events from the same thread.

    Two events are considered duplicates if they have the same type, domain,
    and event_date, and similar actor/target. Keep the one with higher confidence
    (or the first one if tied).
    """
    if len(events) <= 1:
        return events

    seen: dict[str, dict[str, Any]] = {}
    for ev in events:
        # Build a dedup key: domain + type + date + actor (normalized)
        raw_actor = ev.get("actor") or ""
        raw_target = ev.get("target") or ""
        actor = (raw_actor if isinstance(raw_actor, str) else ", ".join(raw_actor)).lower().strip()
        target = (raw_target if isinstance(raw_target, str) else ", ".join(raw_target)).lower().strip()
        date = ev.get("event_date") or ""
        key = f"{ev['domain']}|{ev['type']}|{date}|{actor}|{target}"

        if key in seen:
            # Keep the one with higher confidence
            if (ev.get("confidence") or 0) > (seen[key].get("confidence") or 0):
                seen[key] = ev
        else:
            seen[key] = ev

    deduped = list(seen.values())
    if len(deduped) < len(events):
        logger.info("Deduped %d → %d events", len(events), len(deduped))
    return deduped


# ── Thread batching ────────────────────────────────────────────────────────

MAX_EMAILS_FOR_BATCH = 3        # Threads with more emails go through single-thread path
MAX_BODY_CHARS_FOR_BATCH = 2000 # Per-thread body size threshold
BATCH_CONTENT_BUDGET = 8000     # Max total chars of email content per batch


def _measure_thread(
    conn: sqlite3.Connection, thread_id: str,
) -> tuple[int, int, list[dict]]:
    """Return (email_count, total_body_chars, emails) for a thread."""
    emails = fetchall(
        conn,
        """SELECT message_id, date, from_address, from_name, to_addresses, cc_addresses,
                  subject, body_text
           FROM emails WHERE thread_id = ? ORDER BY date ASC""",
        (thread_id,),
    )
    total_chars = sum(len(e["body_text"] or "") for e in emails)
    return len(emails), total_chars, emails


def _group_into_batches(
    small_threads: list[tuple[str, int, list[dict]]],
    budget: int = BATCH_CONTENT_BUDGET,
) -> list[list[tuple[str, list[dict]]]]:
    """Group small threads into batches that fit a content budget.

    Each entry in small_threads: (thread_id, total_body_chars, emails).
    Returns list of batches, each batch is [(thread_id, emails), ...].
    """
    batches: list[list[tuple[str, list[dict]]]] = []
    current_batch: list[tuple[str, list[dict]]] = []
    current_size = 0

    for thread_id, body_chars, emails in small_threads:
        if current_batch and current_size + body_chars > budget:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append((thread_id, emails))
        current_size += body_chars

    if current_batch:
        batches.append(current_batch)

    return batches


def _format_batch_threads(
    batch: list[tuple[str, list[dict]]],
    body_per_email: int = 800,
) -> str:
    """Format multiple threads for a batched prompt."""
    parts: list[str] = []
    for thread_id, emails in batch:
        subject = emails[0]["subject"] or "(no subject)" if emails else "(no subject)"
        participant_emails: set[str] = set()
        for e in emails:
            for field in ("from_address", "to_addresses", "cc_addresses"):
                val = e[field]
                if val:
                    participant_emails.update(a.lower() for a in _EMAIL_RE.findall(val))

        formatted = _format_thread_emails(emails, body_per_email=body_per_email)
        messages_text = "\n".join(formatted)

        parts.append(
            f"=== THREAD: {thread_id} ===\n"
            f"Subject: {subject}\n"
            f"Participants: {', '.join(sorted(participant_emails))}\n\n"
            f"{messages_text}\n"
            f"=== END THREAD: {thread_id} ==="
        )

    return "\n\n".join(parts)


def _process_batch(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    batch: list[tuple[str, list[dict]]],
    categories: list[dict[str, Any]],
    domains_block: str,
    account_owner: str | None,
    system_prompt: str | None = None,
) -> list[dict[str, Any]]:
    """Extract events from a batch of small threads in a single LLM call."""
    owner_line = f"\nAccount owner (\"me\"): {account_owner}" if account_owner else ""
    threads_block = _format_batch_threads(batch)

    user_prompt = EXTRACT_EVENTS_BATCH_USER.format(
        owner_line=owner_line,
        domains_block=domains_block,
        threads_block=threads_block,
    )

    try:
        result = backend.complete_json(system_prompt or EXTRACT_EVENTS_BATCH_SYSTEM, user_prompt)
    except Exception as e:
        thread_ids = [tid for tid, _ in batch]
        logger.error("LLM call failed for batch of %d threads: %s", len(batch), e)
        # Fall back to processing individually
        all_events: list[dict[str, Any]] = []
        for thread_id, _ in batch:
            events = _process_thread(conn, backend, thread_id, categories, domains_block, account_owner)
            all_events.extend(events)
        return all_events

    # Build type-to-domain lookup
    all_type_to_domain: dict[str, str] = {}
    for cat in categories:
        for et in cat.get("event_types", []):
            type_name = et["name"] if isinstance(et, dict) else et
            all_type_to_domain[type_name] = cat["name"]

    # Build lookup of emails by thread_id for source resolution
    emails_by_thread: dict[str, list[dict]] = {tid: emails for tid, emails in batch}

    now = datetime.now(timezone.utc).isoformat()
    all_parsed: list[dict[str, Any]] = []

    threads_data = result.get("threads", {})
    for thread_id, thread_result in threads_data.items():
        if thread_id not in emails_by_thread:
            logger.warning("LLM returned unknown thread_id '%s' in batch response", thread_id)
            continue

        emails = emails_by_thread[thread_id]
        for ev in thread_result.get("events", []):
            event_type = ev.get("type", "")
            domain = ev.get("domain", "")

            # Validate event type
            valid_types_for_domain: set[str] = set()
            for cat in categories:
                if cat["name"] == domain:
                    for et in cat.get("event_types", []):
                        valid_types_for_domain.add(et["name"] if isinstance(et, dict) else et)
                    break

            if event_type not in valid_types_for_domain:
                if event_type in all_type_to_domain:
                    domain = all_type_to_domain[event_type]
                else:
                    logger.warning(
                        "Skipping unknown event type '%s' for domain '%s' in thread %s (batch)",
                        event_type, domain, thread_id,
                    )
                    continue

            source_idx = ev.get("source_email_index")
            source_email_id = None
            if source_idx is not None and 0 <= source_idx < len(emails):
                source_email_id = emails[source_idx]["message_id"]

            source_calendar_id = ev.get("calendar_event_id")

            if source_calendar_id:
                src_type, src_id = "calendar", source_calendar_id
            elif source_email_id:
                src_type, src_id = "email", source_email_id
            else:
                src_type, src_id = "email", None

            all_parsed.append({
                "id": f"evt_{uuid.uuid4().hex[:12]}",
                "thread_id": thread_id,
                "source_email_id": source_email_id,
                "source_calendar_event_id": source_calendar_id,
                "source_type": src_type,
                "source_id": src_id,
                "domain": domain,
                "type": event_type,
                "actor": ", ".join(ev["actor"]) if isinstance(ev.get("actor"), list) else ev.get("actor"),
                "target": ", ".join(ev["target"]) if isinstance(ev.get("target"), list) else ev.get("target"),
                "event_date": ev.get("event_date"),
                "detail": ev.get("detail"),
                "confidence": ev.get("confidence", 0.5),
                "model_version": backend.model_name,
                "prompt_version": PROMPT_VERSION,
                "created_at": now,
            })

    return all_parsed


def _save_events(conn: sqlite3.Connection, events: list[dict[str, Any]], run_id: int | None = None) -> int:
    """Save events to the event_ledger table."""
    if not events:
        return 0

    # Stamp run_id on all events if provided
    if run_id is not None:
        for ev in events:
            ev["run_id"] = run_id
    else:
        for ev in events:
            ev.setdefault("run_id", None)

    conn.executemany(
        """INSERT OR IGNORE INTO event_ledger
           (id, thread_id, source_email_id, source_calendar_event_id,
            source_type, source_id, run_id, discussion_id,
            domain, type, actor, target, event_date, detail, confidence,
            model_version, prompt_version, created_at)
           VALUES (:id, :thread_id, :source_email_id, :source_calendar_event_id,
                   :source_type, :source_id, :run_id, NULL,
                   :domain, :type, :actor, :target, :event_date, :detail, :confidence,
                   :model_version, :prompt_version, :created_at)""",
        events,
    )

    # Record in change journal — mark affected threads as having new events
    thread_ids = {e["thread_id"] for e in events if e.get("thread_id")}
    if thread_ids:
        record_changes(
            conn,
            [("thread", tid, "new_event", "extract_events") for tid in thread_ids],
        )

    conn.commit()
    return len(events)


# ── Public entry point ──────────────────────────────────────────────────────

def _clean_events(
    conn: sqlite3.Connection,
    company_domain: str | None = None,
    company_label: str | None = None,
) -> int:
    """Delete existing events for the given scope. Returns count deleted."""
    if company_domain:
        like = f"%@{company_domain}%"
        result = conn.execute(
            """DELETE FROM event_ledger WHERE thread_id IN (
                SELECT DISTINCT e.thread_id FROM emails e
                WHERE e.from_address LIKE ? OR e.to_addresses LIKE ?
            )""",
            (like, like),
        )
    elif company_label:
        result = conn.execute(
            """DELETE FROM event_ledger WHERE thread_id IN (
                SELECT DISTINCT e.thread_id FROM emails e
                JOIN company_contacts cc ON (e.from_address = cc.contact_email
                                             OR e.to_addresses LIKE '%' || cc.contact_email || '%')
                JOIN company_labels cl ON cc.company_id = cl.company_id
                WHERE cl.label = ?
            )""",
            (company_label,),
        )
    else:
        result = conn.execute("DELETE FROM event_ledger")
    conn.commit()
    count = result.changes if hasattr(result, 'changes') else 0
    if count:
        logger.info("Cleaned %d events", count)
    return count


def extract_events_propose(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    concurrency: int = 1,
) -> dict[str, Any] | None:
    """Run LLM calls and return a ProposedChanges-compatible dict without writing to DB.

    Returns None if there's nothing to do.
    """
    if clean:
        _clean_events(conn, company_domain=company_domain, company_label=company_label)

    if categories_config is None:
        categories_config = load_category_config(config_path)

    if not categories_config:
        logger.warning("No category config found — cannot extract events")
        return None

    domains_block = _build_domains_block(categories_config)
    account_owner = _detect_account_owner(conn)

    # Enrich system prompts with learned rules
    from email_manager.analysis.feedback import format_rules_block, LAYER_EVENTS
    rules_block = format_rules_block(conn, LAYER_EVENTS)
    system_prompt = EXTRACT_EVENTS_SYSTEM + rules_block
    batch_system_prompt = EXTRACT_EVENTS_BATCH_SYSTEM + rules_block

    thread_ids = _get_threads_to_process(
        conn, limit=limit, force=force or clean,
        company_domain=company_domain, company_label=company_label,
    )
    if not thread_ids:
        logger.info("No threads to process for event extraction")
        return None

    logger.info("Extracting events from %d threads", len(thread_ids))

    # Classify threads as small (batchable) or large
    small_threads: list[tuple[str, int, list[dict]]] = []
    large_thread_ids: list[str] = []

    for thread_id in thread_ids:
        email_count, body_chars, emails = _measure_thread(conn, thread_id)
        if (
            email_count <= MAX_EMAILS_FOR_BATCH
            and body_chars <= MAX_BODY_CHARS_FOR_BATCH
        ):
            small_threads.append((thread_id, body_chars, emails))
        else:
            large_thread_ids.append(thread_id)

    batches = _group_into_batches(small_threads)

    if batches:
        logger.info(
            "Batching: %d small threads in %d batches, %d large threads individual",
            len(small_threads), len(batches), len(large_thread_ids),
        )

    all_events: list[dict[str, Any]] = []
    progress_idx = 0

    if concurrency > 1:
        # ── Parallel extraction ───────────────────────────────────────────
        import asyncio

        sem = asyncio.Semaphore(concurrency)

        async def _do_batch(batch: list[tuple[str, list[dict]]]) -> list[dict[str, Any]]:
            """Process a batch of small threads with async LLM call."""
            owner_line = f"\nAccount owner (\"me\"): {account_owner}" if account_owner else ""
            threads_block = _format_batch_threads(batch)
            user_prompt = EXTRACT_EVENTS_BATCH_USER.format(
                owner_line=owner_line,
                domains_block=domains_block,
                threads_block=threads_block,
            )
            async with sem:
                try:
                    result = await backend.acomplete_json(batch_system_prompt, user_prompt)
                except Exception as e:
                    logger.error("Async batch LLM call failed: %s", e)
                    return []

            all_type_to_domain: dict[str, str] = {}
            for cat in categories_config:
                for et in cat.get("event_types", []):
                    type_name = et["name"] if isinstance(et, dict) else et
                    all_type_to_domain[type_name] = cat["name"]

            emails_by_thread: dict[str, list[dict]] = {tid: emails for tid, emails in batch}
            now = datetime.now(timezone.utc).isoformat()
            parsed: list[dict[str, Any]] = []

            for thread_id, thread_result in result.get("threads", {}).items():
                if thread_id not in emails_by_thread:
                    continue
                emails = emails_by_thread[thread_id]
                for ev in thread_result.get("events", []):
                    event_type = ev.get("type", "")
                    domain = ev.get("domain", "")
                    valid = set()
                    for cat in categories_config:
                        if cat["name"] == domain:
                            for et in cat.get("event_types", []):
                                valid.add(et["name"] if isinstance(et, dict) else et)
                            break
                    if event_type not in valid:
                        if event_type in all_type_to_domain:
                            domain = all_type_to_domain[event_type]
                        else:
                            continue
                    source_idx = ev.get("source_email_index")
                    source_email_id = None
                    if source_idx is not None and 0 <= source_idx < len(emails):
                        source_email_id = emails[source_idx]["message_id"]
                    source_calendar_id = ev.get("calendar_event_id")
                    src_type = "calendar" if source_calendar_id else "email"
                    src_id = source_calendar_id or source_email_id
                    parsed.append({
                        "id": f"evt_{uuid.uuid4().hex[:12]}", "thread_id": thread_id,
                        "source_email_id": source_email_id, "source_calendar_event_id": source_calendar_id,
                        "source_type": src_type, "source_id": src_id,
                        "domain": domain, "type": event_type,
                        "actor": ", ".join(ev["actor"]) if isinstance(ev.get("actor"), list) else ev.get("actor"),
                        "target": ", ".join(ev["target"]) if isinstance(ev.get("target"), list) else ev.get("target"),
                        "event_date": ev.get("event_date"), "detail": ev.get("detail"),
                        "confidence": ev.get("confidence", 0.5),
                        "model_version": backend.model_name, "prompt_version": PROMPT_VERSION, "created_at": now,
                    })
            return parsed

        async def _do_thread(tid: str) -> list[dict[str, Any]]:
            """Process a large thread with async LLM calls (chunks sequential)."""
            all_emails = fetchall(
                conn,
                """SELECT message_id, date, from_address, from_name, to_addresses, cc_addresses,
                          subject, body_text
                   FROM emails WHERE thread_id = ? ORDER BY date ASC""",
                (tid,),
            )
            if not all_emails:
                return []

            participant_emails: set[str] = set()
            for e in all_emails:
                for field in ("from_address", "to_addresses", "cc_addresses"):
                    val = e[field]
                    if val:
                        participant_emails.update(a.lower() for a in _EMAIL_RE.findall(val))

            start_date = all_emails[0]["date"][:10] if all_emails[0]["date"] else None
            end_date = all_emails[-1]["date"][:10] if all_emails[-1]["date"] else None
            calendar_block = _get_calendar_context(conn, participant_emails, start_date, end_date)

            subject = all_emails[0]["subject"] or "(no subject)"
            participants = ", ".join(sorted(participant_emails))
            owner_line = f"\nAccount owner (\"me\"): {account_owner}" if account_owner else ""

            chunks = _chunk_emails(all_emails)
            all_parsed: list[dict[str, Any]] = []
            now = datetime.now(timezone.utc).isoformat()

            for chunk_idx, emails in enumerate(chunks):
                formatted = _format_thread_emails(emails)
                messages_text = "\n".join(formatted)
                chunk_note = ""
                if len(chunks) > 1:
                    chunk_note = f"\n\nNote: This is part {chunk_idx + 1} of {len(chunks)} of a long thread."
                user_prompt = EXTRACT_EVENTS_USER.format(
                    owner_line=owner_line, subject=subject, participants=participants,
                    domains_block=domains_block, messages=messages_text,
                    calendar_block=calendar_block if chunk_idx == 0 else "",
                ) + chunk_note

                async with sem:
                    try:
                        result = await backend.acomplete_json(system_prompt, user_prompt)
                    except Exception as e:
                        logger.error("Async LLM call failed for thread %s (chunk %d): %s", tid, chunk_idx, e)
                        continue

                all_type_to_domain: dict[str, str] = {}
                for cat in categories_config:
                    for et in cat.get("event_types", []):
                        type_name = et["name"] if isinstance(et, dict) else et
                        all_type_to_domain[type_name] = cat["name"]

                for ev in result.get("events", []):
                    event_type = ev.get("type", "")
                    domain = ev.get("domain", "")
                    valid = set()
                    for cat in categories_config:
                        if cat["name"] == domain:
                            for et in cat.get("event_types", []):
                                valid.add(et["name"] if isinstance(et, dict) else et)
                            break
                    if event_type not in valid:
                        if event_type in all_type_to_domain:
                            domain = all_type_to_domain[event_type]
                        else:
                            continue
                    source_idx = ev.get("source_email_index")
                    source_email_id = None
                    if source_idx is not None and 0 <= source_idx < len(emails):
                        source_email_id = emails[source_idx]["message_id"]
                    source_calendar_id = ev.get("calendar_event_id")
                    src_type = "calendar" if source_calendar_id else "email"
                    src_id = source_calendar_id or source_email_id
                    all_parsed.append({
                        "id": f"evt_{uuid.uuid4().hex[:12]}", "thread_id": tid,
                        "source_email_id": source_email_id, "source_calendar_event_id": source_calendar_id,
                        "source_type": src_type, "source_id": src_id,
                        "domain": domain, "type": event_type,
                        "actor": ", ".join(ev["actor"]) if isinstance(ev.get("actor"), list) else ev.get("actor"),
                        "target": ", ".join(ev["target"]) if isinstance(ev.get("target"), list) else ev.get("target"),
                        "event_date": ev.get("event_date"), "detail": ev.get("detail"),
                        "confidence": ev.get("confidence", 0.5),
                        "model_version": backend.model_name, "prompt_version": PROMPT_VERSION, "created_at": now,
                    })

            return _dedup_events(all_parsed)

        async def _run_parallel():
            tasks: list[asyncio.Task] = []
            for batch in batches:
                tasks.append(asyncio.create_task(_do_batch(batch)))
            for tid in large_thread_ids:
                tasks.append(asyncio.create_task(_do_thread(tid)))
            return await asyncio.gather(*tasks, return_exceptions=True)

        all_results = asyncio.run(_run_parallel())

        for i, result in enumerate(all_results):
            if isinstance(result, Exception):
                logger.error("Parallel extraction task %d failed: %s", i, result)
                continue
            if result:
                all_events.extend(result)
            progress_idx += 1
            if on_progress:
                on_progress(min(progress_idx, len(thread_ids)), len(thread_ids))

        logger.info("Parallel extraction: %d events from %d tasks (concurrency=%d)",
                     len(all_events), len(all_results), concurrency)

    else:
        # ── Sequential extraction (original path) ─────────────────────────

        for batch in batches:
            if on_progress:
                on_progress(progress_idx, len(thread_ids))

            events = _process_batch(
                conn, backend, batch, categories_config, domains_block, account_owner,
                system_prompt=batch_system_prompt,
            )
            all_events.extend(events)

            if events:
                logger.info(
                    "Batch of %d threads: extracted %d events",
                    len(batch), len(events),
                )

            progress_idx += len(batch)

        for thread_id in large_thread_ids:
            if on_progress:
                on_progress(progress_idx, len(thread_ids))

            events = _process_thread(
                conn, backend, thread_id, categories_config, domains_block, account_owner,
                system_prompt=system_prompt,
            )
            all_events.extend(events)

            if events:
                logger.info(
                    "Thread %s: extracted %d events (domains: %s)",
                    thread_id, len(events),
                    ", ".join(set(e["domain"] for e in events)),
                )

            progress_idx += 1

    if on_progress:
        on_progress(len(thread_ids), len(thread_ids))

    if not all_events:
        return None

    return {"events": all_events}


def extract_events(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    concurrency: int = 1,
) -> int:
    """Extract business events from email threads.

    Args:
        concurrency: Max concurrent LLM calls. >1 enables parallel extraction.

    Returns the number of events extracted.
    """
    from email_manager.ai.agent_backend import ProposedChanges, apply_changes
    from email_manager.analysis.feedback import compute_prompt_hash, format_rules_block, LAYER_EVENTS

    proposed_dict = extract_events_propose(
        conn, backend, categories_config=categories_config,
        config_path=config_path, limit=limit, force=force, clean=clean,
        company_domain=company_domain, company_label=company_label,
        on_progress=on_progress, concurrency=concurrency,
    )
    if not proposed_dict:
        return 0

    # Compute prompt hash for versioning (matches what extract_events_propose used)
    rules_block = format_rules_block(conn, LAYER_EVENTS)
    p_hash = compute_prompt_hash(EXTRACT_EVENTS_SYSTEM + rules_block)

    all_events = proposed_dict.get("events", [])

    # If already scoped to one company, apply directly
    if company_domain:
        proposed = ProposedChanges(proposed_dict)
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (company_domain,))
        cid = row["id"] if row else 0
        counts = apply_changes(
            conn, proposed, cid, company_domain,
            mode="staged:extract_events", model=backend.model_name,
            token_tracker=getattr(backend, "token_tracker", None),
            prompt_hash=p_hash,
        )
        return counts.get("events", 0)

    # Group events by company domain for per-company processing_runs
    events_by_company: dict[str, list[dict]] = {}
    thread_company_cache: dict[str, str | None] = {}

    for ev in all_events:
        tid = ev.get("thread_id")
        if tid and tid not in thread_company_cache:
            row = fetchone(
                conn,
                """SELECT c.domain FROM emails e
                   JOIN company_contacts cc ON e.from_address = cc.contact_email
                   JOIN companies c ON cc.company_id = c.id
                   WHERE e.thread_id = ? LIMIT 1""",
                (tid,),
            )
            thread_company_cache[tid] = row["domain"] if row else None

        domain = thread_company_cache.get(tid) if tid else None
        domain = domain or company_label or "unknown"
        events_by_company.setdefault(domain, []).append(ev)

    total_events = 0
    for domain, events in events_by_company.items():
        proposed = ProposedChanges({"events": events})
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (domain,))
        cid = row["id"] if row else 0
        counts = apply_changes(
            conn, proposed, cid, domain,
            mode="staged:extract_events", model=backend.model_name,
            prompt_hash=p_hash,
        )
        total_events += counts.get("events", 0)

    return total_events
