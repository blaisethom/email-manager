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
8. Merge events about the same underlying deal, process, or topic into ONE discussion even if they span different threads. For example:
   - An investor intro thread and a follow-up DD thread for the same round = ONE investment discussion.
   - Multiple emails about the same hiring candidate across threads = ONE hiring discussion.
   However, SPLIT into separate discussions when there is a terminal outcome (passed, signed, lost, etc.) or a large time gap (1+ years) between activity. Each round/attempt is its own discussion. For example:
   - Investor passed in 2024, then new intro in 2025 = TWO separate investment discussions.
   - Deal signed in 2023, then a new deal with the same company in 2024 = TWO discussions.
   - Investment exploration in 2021 with no outcome, then new approach in 2023 = TWO discussions.
   Events are grouped into time clusters separated by "--- Cluster N ---" lines. Each cluster is a SEPARATE discussion — do NOT merge events from different clusters into the same discussion.
9. When existing discussions are provided, prefer assigning events to them rather than creating new overlapping ones. Only create a new discussion if the events clearly don't fit any existing one.
10. A discussion may be a **sub-discussion** of another. Set "parent_id" to the ID of a parent discussion when events form a supporting activity (scheduling logistics, admin tasks, a specific workstream) within a larger discussion. Examples:
   - Scheduling back-and-forth to arrange an investor meeting → sub-discussion of the investment discussion.
   - A specific due-diligence workstream → sub-discussion of the deal.
   - Contract admin within a partnership → sub-discussion of the partnership.
   A sub-discussion's category should match the nature of its own events (e.g. "scheduling"), which may differ from the parent's category. Only use parent_id when there is a clear hierarchical relationship, not just topical similarity.

Respond with JSON only."""


def _build_discover_prompt(
    company_name: str,
    domain: str,
    events_text: str,
    existing_discussions: list[dict] | None = None,
    account_owner: str | None = None,
    sub_discussion_categories: list[str] | None = None,
) -> str:
    owner_line = f"\nAccount owner: {account_owner}" if account_owner else ""

    existing_block = ""
    if existing_discussions:
        existing_block = "\n\nExisting discussions (assign events to these if they match, or create new ones):\n"
        for d in existing_discussions:
            parent_info = f' parent_id={d["parent_id"]}' if d.get("parent_id") else ""
            existing_block += (
                f'- ID {d["id"]}: "{d["title"]}" [{d["category"]}] '
                f'state={d.get("current_state", "?")}{parent_info} threads={d.get("thread_ids", [])}\n'
            )

    sub_disc_block = ""
    if sub_discussion_categories:
        cats = ", ".join(f'"{c}"' for c in sub_discussion_categories)
        sub_disc_block = (
            f"\n\nSub-discussion categories: {cats}"
            "\nDiscussions in these categories should almost always be sub-discussions of a parent discussion. "
            "Set their parent_id to the ID of the related parent discussion."
        )

    return f"""Group these business events into discussions for this company.
{owner_line}
Company: {company_name}
Domain: {domain}
{existing_block}{sub_disc_block}
Events:
{events_text}

Respond with this exact JSON structure:
{{
  "discussions": [
    {{
      "existing_id": null,
      "parent_id": null,
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
- Set "parent_id" to the ID of an existing discussion if this is a sub-discussion, or null for top-level. If the parent is a NEW discussion in this same response, use its array index as "parent_idx" instead (e.g. 0 for the first discussion in the array).
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
        """SELECT d.id, d.title, d.category, d.current_state, d.summary, d.parent_id
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


POST_TERMINAL_GAP_DAYS = 90   # Gap after a terminal event to start a new cluster
LARGE_GAP_DAYS = 365           # Gap without any terminal event still splits


def _cluster_events(
    events: list[dict[str, Any]],
    terminal_event_types: set[str],
) -> list[list[dict[str, Any]]]:
    """Split events into time clusters at natural boundaries.

    A new cluster starts when:
    - A terminal event (passed, signed, etc.) is followed by a gap of
      >= POST_TERMINAL_GAP_DAYS, OR
    - Any gap of >= LARGE_GAP_DAYS between consecutive events
    """
    if not events:
        return []

    from datetime import datetime

    def _parse_date(s: str) -> datetime | None:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    # Events should already be sorted chronologically
    clusters: list[list[dict[str, Any]]] = [[]]
    saw_terminal = False
    last_date_str: str | None = None

    for ev in events:
        date_str = ev.get("event_date") or ""

        # Check if this event starts a new cluster
        if last_date_str and date_str:
            prev = _parse_date(last_date_str)
            curr = _parse_date(date_str)
            if prev and curr:
                gap = (curr - prev).days
                if (saw_terminal and gap >= POST_TERMINAL_GAP_DAYS) or gap >= LARGE_GAP_DAYS:
                    clusters.append([])
                    saw_terminal = False

        clusters[-1].append(ev)

        if date_str:
            last_date_str = date_str
        if ev.get("type", "") in terminal_event_types:
            saw_terminal = True

    return [c for c in clusters if c]


def _build_event_cluster_map(
    events: list[dict[str, Any]],
    terminal_event_types: set[str],
) -> dict[str, int]:
    """Build a mapping from event ID → cluster index."""
    clusters = _cluster_events(events, terminal_event_types)
    event_to_cluster: dict[str, int] = {}
    for ci, cluster in enumerate(clusters):
        for ev in cluster:
            event_to_cluster[ev["id"]] = ci
    return event_to_cluster


def _enforce_cluster_boundaries(
    conn: sqlite3.Connection,
    discussions: list[dict[str, Any]],
    event_cluster_map: dict[str, int],
    company_id: int,
    model_used: str,
) -> int:
    """Split discussions whose events span multiple clusters.

    For each discussion, check if its event_ids fall into different clusters.
    If so, keep the largest cluster's events in the original discussion and
    create new discussions for the remaining clusters.

    Returns the number of splits performed.
    """
    splits = 0
    for disc in discussions:
        event_ids = disc.get("event_ids", [])
        if not event_ids:
            continue

        # Group events by cluster
        cluster_groups: dict[int, list[str]] = {}
        for eid in event_ids:
            ci = event_cluster_map.get(eid)
            if ci is not None:
                cluster_groups.setdefault(ci, []).append(eid)

        if len(cluster_groups) <= 1:
            continue

        # Keep the cluster with the most events in the original discussion
        disc_id = disc.get("_saved_id")
        if not disc_id:
            continue

        sorted_clusters = sorted(cluster_groups.items(), key=lambda x: -len(x[1]))
        keep_cluster, keep_events = sorted_clusters[0]

        # Update original discussion to only keep events from the largest cluster
        for eid in event_ids:
            if event_cluster_map.get(eid) != keep_cluster:
                conn.execute(
                    "UPDATE event_ledger SET discussion_id = NULL WHERE id = ?",
                    (eid,),
                )

        # Recalculate date range for the kept discussion
        if keep_events:
            placeholders = ",".join("?" for _ in keep_events)
            date_rows = conn.execute(
                f"SELECT MIN(event_date) as mn, MAX(event_date) as mx FROM event_ledger WHERE id IN ({placeholders})",
                tuple(keep_events),
            ).fetchone()
            if date_rows:
                conn.execute(
                    "UPDATE discussions SET first_seen = ?, last_seen = ? WHERE id = ?",
                    (date_rows[0], date_rows[1], disc_id),
                )

        # Create new discussions for remaining clusters
        for ci, cluster_events in sorted_clusters[1:]:
            new_disc = {
                "title": disc.get("title", "Untitled") + f" ({ci + 1})",
                "category": disc.get("category", "other"),
                "event_ids": cluster_events,
                "thread_ids": disc.get("thread_ids", []),
                "participants": disc.get("participants", []),
                "company_domain": disc.get("company_domain"),
            }
            _save_discussion(conn, new_disc, company_id, model_used)
            splits += 1

        logger.info(
            "Split discussion %d into %d parts (cluster boundary enforcement)",
            disc_id, len(cluster_groups),
        )

    if splits:
        conn.commit()
    return splits


def _format_events_for_prompt(
    events: list[dict[str, Any]],
    terminal_event_types: set[str] | None = None,
) -> str:
    """Format events into a prompt-friendly text block with cluster boundaries."""
    if terminal_event_types:
        clusters = _cluster_events(events, terminal_event_types)
    else:
        clusters = [events]

    lines = []
    for ci, cluster in enumerate(clusters):
        if len(clusters) > 1:
            dates = [e.get("event_date", "?") for e in cluster if e.get("event_date")]
            date_range = f"{min(dates)[:10]} to {max(dates)[:10]}" if dates else "?"
            lines.append(f"\n--- Cluster {ci + 1} ({date_range}) ---")

        for ev in cluster:
            detail = (ev.get("detail") or "")[:80]
            thread = ev.get("thread_id") or ""
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

    # Compute first_seen/last_seen from event dates
    event_dates = []
    if event_ids:
        placeholders = ",".join("?" for _ in event_ids)
        date_rows = fetchall(
            conn,
            f"SELECT event_date FROM event_ledger WHERE id IN ({placeholders}) AND event_date IS NOT NULL",
            tuple(event_ids),
        )
        event_dates = [r["event_date"] for r in date_rows]

    first_seen = min(event_dates) if event_dates else now
    last_seen = max(event_dates) if event_dates else now

    if existing_id:
        # Update existing discussion — extend date range
        conn.execute(
            """UPDATE discussions SET
               current_state = COALESCE(?, current_state),
               participants = ?,
               first_seen = MIN(first_seen, ?),
               last_seen = MAX(last_seen, ?),
               updated_at = ?
               WHERE id = ?""",
            (
                disc.get("current_state"),
                json.dumps(participants),
                first_seen, last_seen, now,
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

        # Validate parent_id: must exist, belong to same company, not be self
        parent_id = disc.get("parent_id")
        if parent_id is not None:
            parent_row = fetchone(
                conn,
                "SELECT id, company_id FROM discussions WHERE id = ?",
                (parent_id,),
            )
            if not parent_row or parent_row["company_id"] != resolved_company_id:
                logger.warning("Ignoring invalid parent_id %s for new discussion", parent_id)
                parent_id = None

        cursor = conn.execute(
            """INSERT INTO discussions (title, category, current_state, company_id, parent_id,
               summary, participants, first_seen, last_seen, model_used, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                disc.get("title", "Untitled Discussion"),
                disc.get("category", "other"),
                None,  # state will be set by analyse_discussions
                resolved_company_id,
                parent_id,
                None,  # summary will be set by analyse_discussions
                json.dumps(participants),
                first_seen, last_seen, model_used, now,
            ),
        )
        disc_id = cursor.lastrowid

    # Link threads — resolve potentially truncated thread IDs from LLM
    for tid in thread_ids:
        if tid:
            # Try exact match first, then prefix match
            exact = fetchone(conn, "SELECT thread_id FROM threads WHERE thread_id = ?", (tid,))
            if exact:
                resolved_tid = exact["thread_id"]
            else:
                prefix = fetchone(
                    conn,
                    "SELECT thread_id FROM threads WHERE thread_id LIKE ? LIMIT 1",
                    (tid + "%",),
                )
                resolved_tid = prefix["thread_id"] if prefix else tid
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (disc_id, resolved_tid),
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
            """SELECT d.id, d.title, d.first_seen, d.last_seen,
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

                # Block merge if date ranges are far apart (separate rounds)
                date_gap_too_large = False
                d1_last = d1["last_seen"] or ""
                d2_first = d2["first_seen"] or ""
                d1_first = d1["first_seen"] or ""
                d2_last = d2["last_seen"] or ""
                if d1_last and d2_first and d1_first and d2_last:
                    from datetime import datetime
                    try:
                        # Check if the ranges are non-overlapping with a large gap
                        r1_end = datetime.fromisoformat(d1_last[:10])
                        r1_start = datetime.fromisoformat(d1_first[:10])
                        r2_end = datetime.fromisoformat(d2_last[:10])
                        r2_start = datetime.fromisoformat(d2_first[:10])
                        gap = max(
                            (r2_start - r1_end).days,
                            (r1_start - r2_end).days,
                        )
                        if gap >= LARGE_GAP_DAYS:
                            date_gap_too_large = True
                    except (ValueError, TypeError):
                        pass

                if date_gap_too_large:
                    continue  # Skip merge — these are separate rounds

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
                    # Delete proposed actions for loser
                    conn.execute(
                        "DELETE FROM proposed_actions WHERE discussion_id = ?",
                        (loser,),
                    )
                    # Reparent any child discussions
                    conn.execute(
                        "UPDATE discussions SET parent_id = ? WHERE parent_id = ?",
                        (winner, loser),
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

def _clean_discussions(
    conn: sqlite3.Connection,
    company_domain: str | None = None,
    company_label: str | None = None,
) -> int:
    """Delete discussions (and related data) for the given scope. Returns count deleted."""
    # Find company IDs in scope
    if company_domain:
        company_ids = [r[0] for r in fetchall(
            conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (company_domain,)
        )]
    elif company_label:
        company_ids = [r[0] for r in fetchall(
            conn,
            "SELECT company_id FROM company_labels WHERE label = ?",
            (company_label,),
        )]
    else:
        company_ids = None  # all

    if company_ids is not None and not company_ids:
        return 0

    # Build WHERE clause
    if company_ids is not None:
        placeholders = ",".join("?" for _ in company_ids)
        disc_where = f"company_id IN ({placeholders})"
        params = tuple(company_ids)
    else:
        disc_where = "1=1"
        params = ()

    # Get discussion IDs to clean
    disc_ids = [r[0] for r in fetchall(
        conn, f"SELECT id FROM discussions WHERE {disc_where}", params
    )]
    if not disc_ids:
        return 0

    id_placeholders = ",".join("?" for _ in disc_ids)
    id_params = tuple(disc_ids)

    # Unassign events (don't delete them — they belong to extract_events)
    conn.execute(
        f"UPDATE event_ledger SET discussion_id = NULL WHERE discussion_id IN ({id_placeholders})",
        id_params,
    )
    # Detach children from discussions being deleted
    conn.execute(
        f"UPDATE discussions SET parent_id = NULL WHERE parent_id IN ({id_placeholders})",
        id_params,
    )
    # Delete related records
    conn.execute(f"DELETE FROM milestones WHERE discussion_id IN ({id_placeholders})", id_params)
    conn.execute(f"DELETE FROM discussion_state_history WHERE discussion_id IN ({id_placeholders})", id_params)
    conn.execute(f"DELETE FROM discussion_threads WHERE discussion_id IN ({id_placeholders})", id_params)
    conn.execute(f"DELETE FROM actions WHERE discussion_id IN ({id_placeholders})", id_params)
    conn.execute(f"DELETE FROM discussion_events WHERE discussion_id IN ({id_placeholders})", id_params)
    conn.execute(f"DELETE FROM proposed_actions WHERE discussion_id IN ({id_placeholders})", id_params)
    # Delete discussions themselves
    conn.execute(f"DELETE FROM discussions WHERE id IN ({id_placeholders})", id_params)
    conn.commit()

    logger.info("Cleaned %d discussions", len(disc_ids))
    return len(disc_ids)


def discover_discussions(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
    categories_config: list[dict[str, Any]] | None = None,
) -> int:
    """Discover discussions by clustering events from the event ledger.

    Returns the number of discussions created or updated.
    """
    from email_manager.ai.agent_backend import ProposedChanges, apply_changes as _apply_changes

    if clean:
        _clean_discussions(conn, company_domain=company_domain, company_label=company_label)

    account_owner = _detect_account_owner(conn)

    # Extract category names flagged as sub_discussion in config
    sub_discussion_categories: list[str] = []
    terminal_event_types: set[str] = set()
    if categories_config:
        sub_discussion_categories = [
            c["name"] for c in categories_config if c.get("sub_discussion")
        ]
        for cat in categories_config:
            for t in cat.get("terminal_event_types", []):
                terminal_event_types.add(t)

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

        DISCOVER_BATCH_SIZE = 30
        all_events = _get_events_for_company(conn, company["domain"], max_events=200)
        if not all_events:
            continue

        # Process in batches to avoid CLI timeout on large event sets
        event_batches = [all_events[i:i + DISCOVER_BATCH_SIZE]
                         for i in range(0, len(all_events), DISCOVER_BATCH_SIZE)]
        discussions: list[dict] = []

        for batch_idx, batch_events in enumerate(event_batches):
            existing = _get_existing_discussions_for_company(conn, company["id"])
            event_cluster_map = _build_event_cluster_map(batch_events, terminal_event_types) if terminal_event_types else {}
            events_text = _format_events_for_prompt(batch_events, terminal_event_types or None)

            user_prompt = _build_discover_prompt(
                company["name"], company["domain"], events_text,
                existing_discussions=existing,
                account_owner=account_owner,
                sub_discussion_categories=sub_discussion_categories or None,
            )

            if len(event_batches) > 1:
                logger.info("Company %s: discover batch %d/%d (%d events)",
                            company["domain"], batch_idx + 1, len(event_batches), len(batch_events))

            try:
                result = backend.complete_json(DISCOVER_SYSTEM, user_prompt)
            except Exception as e:
                logger.error("LLM call failed for company %s (batch %d): %s",
                             company["domain"], batch_idx + 1, e)
                continue

            batch_discussions = result.get("discussions", [])
            discussions.extend(batch_discussions)

            # Save this batch immediately so subsequent batches see the new discussions
            if batch_discussions and batch_idx < len(event_batches) - 1:
                saved_ids_batch: dict[int, int] = {}
                for idx, disc in enumerate(batch_discussions):
                    parent_id = disc.pop("parent_id", None)
                    parent_idx = disc.pop("parent_idx", None)
                    disc_id = _save_discussion(conn, disc, company["id"], backend.model_name)
                    saved_ids_batch[idx] = disc_id
                    disc["_saved_id"] = disc_id
                    disc["_parent_id"] = parent_id
                    disc["_parent_idx"] = parent_idx
                for idx, disc in enumerate(batch_discussions):
                    parent_id = disc.get("_parent_id")
                    parent_idx = disc.get("_parent_idx")
                    resolved_parent = None
                    if isinstance(parent_id, int) and parent_id > 0:
                        parent_row = fetchone(conn, "SELECT company_id FROM discussions WHERE id = ?", (parent_id,))
                        if parent_row and parent_row["company_id"] == company["id"]:
                            resolved_parent = parent_id
                    elif parent_idx is not None:
                        try:
                            resolved_parent = saved_ids_batch[int(parent_idx)]
                        except (ValueError, KeyError):
                            pass
                    if resolved_parent and resolved_parent != saved_ids_batch[idx]:
                        conn.execute("UPDATE discussions SET parent_id = ? WHERE id = ?",
                                     (resolved_parent, saved_ids_batch[idx]))
                conn.commit()
                total_discussions += len(batch_discussions)

        # Build a ProposedChanges snapshot of the LLM's clustering decision
        proposed_new: list[dict] = []
        proposed_assignments: list[dict] = []
        proposed_links: list[dict] = []
        for disc in discussions:
            event_ids = disc.get("event_ids", [])
            thread_ids_disc = disc.get("thread_ids", [])
            existing_id = disc.get("existing_id")
            if existing_id:
                # Assigning events to existing discussion
                for eid in event_ids:
                    proposed_assignments.append({"event_id": eid, "discussion_id": existing_id})
                for tid in thread_ids_disc:
                    proposed_links.append({"discussion_id": existing_id, "thread_id": tid})
            else:
                temp_id = f"new_{len(proposed_new)}"
                proposed_new.append({
                    "temp_id": temp_id,
                    "title": disc.get("title", "Untitled"),
                    "category": disc.get("category", "other"),
                    "parent_id": disc.get("parent_id"),
                    "participants": disc.get("participants", []),
                })
                for eid in event_ids:
                    proposed_assignments.append({"event_id": eid, "discussion_id": temp_id})
                for tid in thread_ids_disc:
                    proposed_links.append({"discussion_id": temp_id, "thread_id": tid})

        proposed = ProposedChanges({
            "new_discussions": proposed_new,
            "event_assignments": proposed_assignments,
            "thread_links": proposed_links,
        })

        # Snapshot the LLM decision as a processing_run with chain tracking
        now_ts = datetime.now(timezone.utc).isoformat()
        run_mode = "staged:discover_discussions"
        parent_row = fetchone(
            conn,
            "SELECT id FROM processing_runs WHERE company_domain = ? AND mode = ? ORDER BY id DESC LIMIT 1",
            (company["domain"], run_mode),
        )
        parent_run_id = parent_row["id"] if parent_row else None
        like = f"%@{company['domain']}%"
        cutoff_row = fetchone(
            conn,
            "SELECT MAX(date) as cutoff FROM emails WHERE from_address LIKE ? OR to_addresses LIKE ?",
            (like, like),
        )
        email_cutoff = cutoff_row["cutoff"] if cutoff_row and cutoff_row["cutoff"] else None
        from email_manager.analysis.feedback import compute_prompt_hash
        p_hash = compute_prompt_hash(DISCOVER_SYSTEM)
        cursor = conn.execute(
            """INSERT INTO processing_runs
               (company_domain, mode, model, started_at, proposed_changes_json,
                parent_run_id, email_cutoff_date, prompt_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (company["domain"], run_mode, backend.model_name,
             now_ts, json.dumps(proposed.to_dict()),
             parent_run_id, email_cutoff, p_hash),
        )
        snapshot_run_id = cursor.lastrowid

        # Apply using existing save path — skip discussions already saved by batch processing
        saved_ids: dict[int, int] = {}  # array index -> saved discussion ID
        for idx, disc in enumerate(discussions):
            if "_saved_id" in disc:
                # Already saved in a batch pass
                saved_ids[idx] = disc["_saved_id"]
                continue
            parent_id = disc.pop("parent_id", None)
            parent_idx = disc.pop("parent_idx", None)
            disc_id = _save_discussion(conn, disc, company["id"], backend.model_name)
            saved_ids[idx] = disc_id
            disc["_saved_id"] = disc_id
            disc["_parent_id"] = parent_id
            disc["_parent_idx"] = parent_idx
            total_discussions += 1

        # Second pass: resolve parent_id references (with same-company validation)
        for idx, disc in enumerate(discussions):
            parent_id = disc.get("_parent_id")
            parent_idx = disc.get("_parent_idx")
            resolved_parent = None
            if isinstance(parent_id, int) and parent_id > 0:
                # Validate: parent must belong to same company
                parent_row = fetchone(conn, "SELECT company_id FROM discussions WHERE id = ?", (parent_id,))
                if parent_row and parent_row["company_id"] == company["id"]:
                    resolved_parent = parent_id
                else:
                    logger.warning("Ignoring cross-company parent_id %s for discussion %s",
                                   parent_id, saved_ids[idx])
            elif parent_idx is not None:
                try:
                    resolved_parent = saved_ids[int(parent_idx)]
                except (ValueError, KeyError):
                    pass
            if resolved_parent and resolved_parent != saved_ids[idx]:
                conn.execute(
                    "UPDATE discussions SET parent_id = ? WHERE id = ?",
                    (resolved_parent, saved_ids[idx]),
                )

        # Complete the processing run
        conn.execute(
            """UPDATE processing_runs SET completed_at = ?, discussions_created = ?
               WHERE id = ?""",
            (datetime.now(timezone.utc).isoformat(), len([d for d in discussions if not d.get("existing_id")]),
             snapshot_run_id),
        )

        conn.commit()

        # Enforce cluster boundaries: split discussions that span multiple clusters
        cluster_splits = 0
        if event_cluster_map:
            cluster_splits = _enforce_cluster_boundaries(
                conn, discussions, event_cluster_map, company["id"], backend.model_name,
            )
            if cluster_splits:
                total_discussions += cluster_splits

        # Post-discovery merge: detect and merge overlapping discussions
        merges = _merge_overlapping_discussions(conn, company["id"])
        if merges:
            total_discussions -= merges

        # Count surviving discussions
        surviving = fetchall(
            conn,
            "SELECT COUNT(*) as cnt FROM discussions WHERE company_id = ?",
            (company["id"],),
        )
        surviving_count = surviving[0]["cnt"] if surviving else 0

        logger.info(
            "Company %s: %d created → %d after merges (%d events, %d merges, %d splits)",
            company["domain"], len(discussions), surviving_count, len(events), merges, cluster_splits,
        )
        if merges and on_progress:
            # Log merge info for TUI visibility
            from rich.console import Console as _C
            _C(stderr=True).print(
                f"  [dim]discover_discussions: {len(discussions)} created, "
                f"{merges} merged → {surviving_count} surviving[/dim]"
            )

    if on_progress:
        on_progress(len(companies), len(companies), "")

    return total_discussions
