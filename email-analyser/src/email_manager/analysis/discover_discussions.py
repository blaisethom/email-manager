"""Discover and cluster discussions from the event ledger.

Groups events by company + domain + topic into discussions. A thread can
contribute events to multiple discussions. Existing discussions are updated
rather than duplicated.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Callable

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.analysis.discover_discussions")


# ── Prompt construction ─────────────────────────────────────────────────────

DISCOVER_SYSTEM = """You are a discussion discovery system. Given a set of business events extracted from email threads for a company, group them into distinct discussions.

A "discussion" is a coherent business interaction — a deal, a hiring process, a partnership exploration, a support issue, etc. Events belong to a discussion if they are about the same underlying topic/deal/process with the same counterparty.

Rules:
1. Each event should be assigned to exactly one discussion.
2. A discussion has a single category/domain (e.g. "investment", "pharma-deal").
3. Give each discussion a short, descriptive title (5-10 words).
4. If events match an existing discussion, assign them to it (use existing_id).
5. Multiple threads can contribute to the same discussion.
6. List all participant email addresses involved in each discussion.
7. Set company_domain to the primary external company domain for the discussion.
8. IMPORTANT — merge aggressively. If two groups of events are about the same underlying deal, process, or topic with the same company, they are ONE discussion even if they span different threads or time periods. For example:
   - An investor intro thread and a follow-up DD thread for the same round = ONE investment discussion.
   - A scheduling thread and the actual meeting follow-up for the same deal = part of that deal's discussion.
   - Multiple emails about the same hiring candidate across threads = ONE hiring discussion.
   Do NOT create separate discussions just because events come from different threads or have gaps in time. Only create separate discussions when the underlying business matter is genuinely different (e.g. a seed round vs a later Series A are different discussions).
9. When existing discussions are provided, prefer assigning events to them rather than creating new overlapping ones. Only create a new discussion if the events clearly don't fit any existing one.

Respond with JSON only."""


def _build_discover_prompt(
    company_name: str,
    domain: str,
    events_text: str,
    existing_discussions: list[dict] | None = None,
    account_owner: str | None = None,
) -> str:
    owner_line = f"\nAccount owner: {account_owner}" if account_owner else ""

    existing_block = ""
    if existing_discussions:
        existing_block = "\n\nExisting discussions (assign events to these if they match, or create new ones):\n"
        for d in existing_discussions:
            existing_block += (
                f'- ID {d["id"]}: "{d["title"]}" [{d["category"]}] '
                f'state={d.get("current_state", "?")} threads={d.get("thread_ids", [])}\n'
            )

    return f"""Group these business events into discussions for this company.
{owner_line}
Company: {company_name}
Domain: {domain}
{existing_block}
Events:
{events_text}

Respond with this exact JSON structure:
{{
  "discussions": [
    {{
      "existing_id": null,
      "title": "Short descriptive title",
      "category": "domain-name",
      "company_domain": "example.com",
      "participants": ["email@example.com"],
      "event_ids": ["evt_abc123", "evt_def456"],
      "thread_ids": ["thread-id-1"]
    }}
  ]
}}

Notes:
- Set "existing_id" to the ID number if updating an existing discussion, or null for new ones.
- "category" must match the domain of the events being assigned.
- "event_ids" should list all event IDs that belong to this discussion.
- "thread_ids" should list all thread IDs that contributed events to this discussion.
- "company_domain" should be the domain of the external counterparty."""


# ── Data gathering ──────────────────────────────────────────────────────────

def _detect_account_owner(conn: sqlite3.Connection) -> str | None:
    row = fetchone(
        conn,
        "SELECT from_address, COUNT(*) as cnt FROM emails GROUP BY from_address ORDER BY cnt DESC LIMIT 1",
    )
    return row["from_address"] if row else None


def _get_companies_with_unassigned_events(
    conn: sqlite3.Connection, limit: int | None = None,
    company_domain: str | None = None, company_label: str | None = None,
) -> list[dict[str, Any]]:
    """Find companies that have unassigned events.

    Strategy: get all unassigned events, extract participant email domains,
    match to companies. This avoids expensive LIKE joins.
    """
    if company_domain:
        # Direct lookup
        row = fetchone(
            conn,
            "SELECT id, name, domain FROM companies WHERE domain = ? COLLATE NOCASE",
            (company_domain,),
        )
        if not row:
            return []
        # Count unassigned events for this company's domain
        like = f"%@{company_domain}%"
        cnt = fetchone(
            conn,
            """SELECT COUNT(DISTINCT el.id)
               FROM event_ledger el
               WHERE el.discussion_id IS NULL
                 AND el.thread_id IN (
                     SELECT DISTINCT e.thread_id FROM emails e
                     WHERE e.from_address LIKE ? OR e.to_addresses LIKE ?
                 )""",
            (like, like),
        )
        return [{"id": row["id"], "name": row["name"], "domain": row["domain"],
                 "event_count": cnt[0]}] if cnt[0] > 0 else []

    # Get companies filtered by label if specified
    if company_label:
        companies = fetchall(
            conn,
            """SELECT c.id, c.name, c.domain FROM companies c
               JOIN company_labels cl ON c.id = cl.company_id
               WHERE cl.label = ?
               ORDER BY c.email_count DESC""",
            (company_label,),
        )
    else:
        companies = fetchall(
            conn,
            "SELECT id, name, domain FROM companies ORDER BY email_count DESC",
        )

    # For each company, check if it has unassigned events
    result = []
    for c in companies:
        like = f"%@{c['domain']}%"
        cnt = fetchone(
            conn,
            """SELECT COUNT(DISTINCT el.id)
               FROM event_ledger el
               WHERE el.discussion_id IS NULL
                 AND el.thread_id IN (
                     SELECT DISTINCT e.thread_id FROM emails e
                     WHERE e.from_address LIKE ? OR e.to_addresses LIKE ?
                 )""",
            (like, like),
        )
        if cnt and cnt[0] > 0:
            result.append({"id": c["id"], "name": c["name"], "domain": c["domain"],
                          "event_count": cnt[0]})
        if limit and len(result) >= limit:
            break

    return result


def _get_events_for_company(
    conn: sqlite3.Connection, company_domain: str, max_events: int = 50,
) -> list[dict[str, Any]]:
    """Get unassigned events related to a company domain, limited to avoid huge prompts."""
    like_pattern = f"%@{company_domain}%"
    rows = fetchall(
        conn,
        """SELECT DISTINCT el.*
           FROM event_ledger el
           JOIN emails e ON el.source_email_id = e.message_id
           WHERE el.discussion_id IS NULL
             AND (e.from_address LIKE ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?)
           ORDER BY el.event_date DESC
           LIMIT ?""",
        (like_pattern, like_pattern, like_pattern, max_events),
    )
    # Return in chronological order
    result = [dict(r) for r in rows]
    result.sort(key=lambda x: x.get("event_date") or "")
    return result


def _get_existing_discussions_for_company(
    conn: sqlite3.Connection, company_id: int,
) -> list[dict[str, Any]]:
    """Get existing discussions for a company."""
    rows = fetchall(
        conn,
        """SELECT d.id, d.title, d.category, d.current_state, d.summary
           FROM discussions d
           WHERE d.company_id = ?
           ORDER BY d.last_seen DESC""",
        (company_id,),
    )
    result = []
    for r in rows:
        d = dict(r)
        thread_rows = fetchall(
            conn,
            "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
            (d["id"],),
        )
        d["thread_ids"] = [tr["thread_id"] for tr in thread_rows]
        result.append(d)
    return result


def _format_events_for_prompt(events: list[dict[str, Any]]) -> str:
    """Format events into a compact prompt-friendly text block."""
    lines = []
    for ev in events:
        detail = (ev.get("detail") or "")[:80]
        thread = (ev.get("thread_id") or "")[:20]
        actor = ev.get("actor", "")
        target = ev.get("target", "")
        target_str = f"→{target}" if target else ""
        lines.append(
            f"[{ev['id']}] {ev.get('event_date', '?')} {ev['domain']}/{ev['type']} "
            f"{actor}{target_str} t={thread} \"{detail}\""
        )
    return "\n".join(lines)


# ── Saving results ──────────────────────────────────────────────────────────

def _save_discussion(
    conn: sqlite3.Connection,
    disc: dict[str, Any],
    company_id: int,
    model_used: str,
) -> int:
    """Save or update a discussion and link events to it."""
    now = datetime.now(timezone.utc).isoformat()
    existing_id = disc.get("existing_id")

    event_ids = disc.get("event_ids", [])
    thread_ids = disc.get("thread_ids", [])
    participants = disc.get("participants", [])

    if existing_id:
        # Update existing discussion
        conn.execute(
            """UPDATE discussions SET
               current_state = COALESCE(?, current_state),
               participants = ?,
               last_seen = ?,
               updated_at = ?
               WHERE id = ?""",
            (
                disc.get("current_state"),
                json.dumps(participants),
                now, now,
                existing_id,
            ),
        )
        disc_id = existing_id
    else:
        # Resolve company_id from domain if provided
        resolved_company_id = company_id
        if disc.get("company_domain"):
            row = fetchone(
                conn,
                "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE",
                (disc["company_domain"],),
            )
            if row:
                resolved_company_id = row["id"]

        cursor = conn.execute(
            """INSERT INTO discussions (title, category, current_state, company_id,
               summary, participants, first_seen, last_seen, model_used, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                disc.get("title", "Untitled Discussion"),
                disc.get("category", "other"),
                None,  # state will be set by analyse_discussions
                resolved_company_id,
                None,  # summary will be set by analyse_discussions
                json.dumps(participants),
                now, now, model_used, now,
            ),
        )
        disc_id = cursor.lastrowid

    # Link threads
    for tid in thread_ids:
        conn.execute(
            "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
            (disc_id, tid),
        )

    # Assign events to this discussion
    if event_ids:
        placeholders = ",".join("?" for _ in event_ids)
        conn.execute(
            f"UPDATE event_ledger SET discussion_id = ? WHERE id IN ({placeholders})",
            (disc_id, *event_ids),
        )

    return disc_id


# ── Post-discovery merge ────────────────────────────────────────────────────

def _merge_overlapping_discussions(conn: sqlite3.Connection, company_id: int) -> int:
    """Merge discussions within the same company + category that overlap.

    Two discussions are candidates for merging if they share:
    - Same company_id and category
    - Overlapping thread IDs, OR high title similarity (>=0.6)

    The discussion with more events is kept; the other's events/threads are
    reassigned to it, and it is deleted.

    Returns the number of merges performed.
    """
    categories = fetchall(
        conn,
        "SELECT DISTINCT category FROM discussions WHERE company_id = ?",
        (company_id,),
    )

    total_merges = 0
    for cat_row in categories:
        category = cat_row["category"]
        discs = fetchall(
            conn,
            """SELECT d.id, d.title,
                      (SELECT COUNT(*) FROM event_ledger el WHERE el.discussion_id = d.id) as event_count
               FROM discussions d
               WHERE d.company_id = ? AND d.category = ?
               ORDER BY event_count DESC""",
            (company_id, category),
        )
        if len(discs) < 2:
            continue

        # Build thread sets for overlap detection
        disc_threads: dict[int, set[str]] = {}
        for d in discs:
            threads = fetchall(
                conn,
                "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
                (d["id"],),
            )
            disc_threads[d["id"]] = {t["thread_id"] for t in threads}

        # Find merge pairs (greedy: merge into the one with more events)
        merged_into: dict[int, int] = {}  # loser_id -> winner_id
        disc_list = list(discs)

        for i, d1 in enumerate(disc_list):
            if d1["id"] in merged_into:
                continue
            for j in range(i + 1, len(disc_list)):
                d2 = disc_list[j]
                if d2["id"] in merged_into:
                    continue

                # Check thread overlap
                threads_overlap = bool(disc_threads.get(d1["id"], set()) & disc_threads.get(d2["id"], set()))

                # Check title similarity
                title_sim = SequenceMatcher(
                    None, d1["title"].lower(), d2["title"].lower()
                ).ratio()

                # Check if titles share key words (beyond stopwords)
                stopwords = {"the", "a", "an", "and", "or", "of", "in", "to", "for", "with", "on", "at", "by", "from"}
                words1 = {w for w in d1["title"].lower().split() if w not in stopwords and len(w) > 2}
                words2 = {w for w in d2["title"].lower().split() if w not in stopwords and len(w) > 2}
                shared_words = words1 & words2
                word_overlap = len(shared_words) / max(len(words1 | words2), 1)

                if threads_overlap or title_sim >= 0.6 or (title_sim >= 0.4 and word_overlap >= 0.4):
                    # Merge d2 into d1 (d1 has more events since sorted DESC)
                    winner, loser = d1["id"], d2["id"]
                    merged_into[loser] = winner

                    # Reassign events
                    conn.execute(
                        "UPDATE event_ledger SET discussion_id = ? WHERE discussion_id = ?",
                        (winner, loser),
                    )
                    # Move threads
                    conn.execute(
                        """INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id)
                           SELECT ?, thread_id FROM discussion_threads WHERE discussion_id = ?""",
                        (winner, loser),
                    )
                    conn.execute(
                        "DELETE FROM discussion_threads WHERE discussion_id = ?",
                        (loser,),
                    )
                    # Move milestones (keep winner's, delete loser's)
                    conn.execute(
                        "DELETE FROM milestones WHERE discussion_id = ?",
                        (loser,),
                    )
                    # Move state history
                    conn.execute(
                        "UPDATE discussion_state_history SET discussion_id = ? WHERE discussion_id = ?",
                        (winner, loser),
                    )
                    # Move actions
                    conn.execute(
                        "UPDATE actions SET discussion_id = ? WHERE discussion_id = ?",
                        (winner, loser),
                    )
                    # Move calendar event links
                    conn.execute(
                        "DELETE FROM discussion_events WHERE discussion_id = ?",
                        (loser,),
                    )
                    # Delete loser discussion
                    conn.execute("DELETE FROM discussions WHERE id = ?", (loser,))
                    total_merges += 1
                    logger.info(
                        "Merged discussion %d (%s) into %d (%s) [%s, title_sim=%.2f, thread_overlap=%s]",
                        loser, d2["title"][:40], winner, d1["title"][:40],
                        category, title_sim, threads_overlap,
                    )

    if total_merges:
        conn.commit()
    return total_merges


# ── Public entry point ──────────────────────────────────────────────────────

def discover_discussions(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    limit: int | None = None,
    force: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    """Discover discussions by clustering events from the event ledger.

    Returns the number of discussions created or updated.
    """
    account_owner = _detect_account_owner(conn)

    companies = _get_companies_with_unassigned_events(
        conn, limit=limit, company_domain=company_domain,
        company_label=company_label,
    )
    if not companies:
        logger.info("No companies with unassigned events")
        return 0

    logger.info("Discovering discussions for %d companies", len(companies))
    total_discussions = 0

    for i, company in enumerate(companies):
        if on_progress:
            on_progress(i, len(companies), company["name"])

        events = _get_events_for_company(conn, company["domain"])
        if not events:
            continue

        existing = _get_existing_discussions_for_company(conn, company["id"])
        events_text = _format_events_for_prompt(events)

        user_prompt = _build_discover_prompt(
            company["name"], company["domain"], events_text,
            existing_discussions=existing,
            account_owner=account_owner,
        )

        try:
            result = backend.complete_json(DISCOVER_SYSTEM, user_prompt)
        except Exception as e:
            logger.error("LLM call failed for company %s: %s", company["domain"], e)
            continue

        discussions = result.get("discussions", [])
        for disc in discussions:
            _save_discussion(conn, disc, company["id"], backend.model_name)
            total_discussions += 1

        conn.commit()

        # Post-discovery merge: detect and merge overlapping discussions
        merges = _merge_overlapping_discussions(conn, company["id"])

        logger.info(
            "Company %s: %d discussions (%d events, %d merges)",
            company["domain"], len(discussions), len(events), merges,
        )

    if on_progress:
        on_progress(len(companies), len(companies), "")

    return total_discussions
