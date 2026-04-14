"""Quick incremental update: process new emails for a company in a single LLM call.

Given new emails and existing discussion context, produces:
- New events extracted from the emails
- Discussion assignments (existing or new)
- Updated state/summary for affected discussions
- Proposed next actions

This is much faster than the full pipeline for the common case of a few new
emails arriving for a company that already has a full analysis history.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from email_manager.ai.base import LLMBackend
from email_manager.analysis.events import (
    _build_domains_block,
    _detect_account_owner,
    _format_thread_emails,
    _strip_quoted_text,
    _dedup_against_previous,
    load_category_config,
    PROMPT_VERSION,
)
from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.analysis.quick_update")


# ── Prompt ─────────────────────────────────────────────────────────────────

QUICK_UPDATE_SYSTEM = """You are a business email analysis system. You will receive:
1. New emails that just arrived for a company
2. The company's existing discussions with their current state, summary, milestones, and recent events
3. A vocabulary of event types and domains

Your job is to process the new emails and return a single JSON response that:

A. **Extracts events** from the new emails using the domain-specific event vocabulary.
B. **Assigns events to discussions** — either existing discussions (by ID) or a new discussion you create.
C. **Updates affected discussions** — provide an updated workflow state, summary, and milestone evaluations for any discussion that received new events.
D. **Proposes next actions** — for each affected non-terminal discussion, propose 1-3 specific next actions.

Rules for event extraction:
- Each event must have a type from the provided vocabulary.
- Use the email date as event_date unless the email references a different date.
- The "actor" is the person who performed the action (email address).
- Assign a confidence score (0.0-1.0).
- Do NOT re-extract events that are already listed in the existing discussion context.
- Examine every new email for events. Replies often contain critical events (passes, acceptances, etc.).
- IMPORTANT: Use the correct domain for each event. Emails about scheduling meetings should use the "scheduling" domain and its event types (meeting_proposed, times_suggested, time_confirmed, etc.), NOT the domain of the thing being discussed. For example, an email saying "Can we meet Tuesday to discuss the deal?" is a scheduling/meeting_proposed event, not an investment event. The actual business events (deal progressed, terms discussed) happen AT the meeting and should only be extracted if the email evidences them.

Rules for discussion assignment:
- Prefer assigning to existing discussions when the topic matches.
- Only create a new discussion if the emails clearly don't fit any existing one.

Rules for sub-discussions:
- Scheduling/logistics emails that support a larger discussion should be tracked as a SUB-DISCUSSION with a parent_id pointing to the main discussion.
- For example, emails coordinating a meeting time for an investment due diligence call should create a "scheduling" sub-discussion with parent_id set to the investment discussion's ID.
- The sub-discussion should have category="scheduling" with its own state (proposed/confirmed/completed/cancelled).
- Set parent_id to an existing discussion ID, or to a temp_id if the parent is also being created.

Rules for state/summary updates:
- The workflow state should reflect where the discussion currently stands after the new events.
- The summary should be updated to incorporate the new developments (2-4 sentences total).
- Only mark milestones as achieved if clearly evidenced.

Rules for proposed actions:
- Be specific and actionable.
- Priority: "high" (this week), "medium" (soon), "low" (can wait).
- If the right action is to wait, set wait_until to a date (YYYY-MM-DD).
- Only use the "stale" state for discussions with NO activity in 3+ months and no explicit terminal outcome.

Respond with JSON only."""


def _build_quick_update_prompt(
    company_name: str,
    company_domain: str,
    new_emails_text: str,
    discussions_context: str,
    domains_block: str,
    account_owner: str | None,
    today: str,
) -> str:
    owner_line = f"\nAccount owner (\"me\"): {account_owner}" if account_owner else ""

    return f"""Process these new emails and update the analysis for {company_name} ({company_domain}).
{owner_line}
Today's date: {today}

{discussions_context}

Available domains and event vocabularies:
{domains_block}

New emails:
{new_emails_text}

Respond with this exact JSON structure:
{{
  "events": [
    {{
      "type": "event_type_name",
      "domain": "domain-name",
      "actor": "email@example.com",
      "target": "email@example.com or null",
      "event_date": "YYYY-MM-DD",
      "detail": "Brief factual description",
      "confidence": 0.9,
      "discussion_id": 123,
      "source_email_index": 0
    }}
  ],
  "new_discussions": [
    {{
      "temp_id": "new_1",
      "title": "Short descriptive title",
      "category": "domain-name",
      "parent_id": null,
      "participants": ["email@example.com"]
    }}
  ],
  "discussion_updates": [
    {{
      "discussion_id": 123,
      "workflow_state": "state-name",
      "summary": "Updated 2-4 sentence summary.",
      "milestones": [
        {{
          "name": "milestone_name",
          "achieved": true,
          "achieved_date": "YYYY-MM-DD",
          "confidence": 0.9
        }}
      ],
      "proposed_actions": [
        {{
          "action": "Specific action to take",
          "reasoning": "Why this is the right next step",
          "priority": "high|medium|low",
          "wait_until": "YYYY-MM-DD or null",
          "assignee": "email@example.com or null"
        }}
      ]
    }}
  ]
}}

Notes:
- "discussion_id" in events should be the ID of an existing discussion, or a "temp_id" from new_discussions.
- "parent_id" in new_discussions can be an existing discussion ID (integer) or a temp_id of another new discussion. Use this for scheduling sub-discussions that support a main discussion. Set to null for top-level discussions.
- Only include discussion_updates for discussions that were affected by the new events.
- For milestones, include ALL milestones for the discussion's category (achieved and not), not just new ones.
- If no business events are found in the new emails, return {{"events": [], "new_discussions": [], "discussion_updates": []}}."""


# ── Context building ───────────────────────────────────────────────────────

def _get_new_emails_for_company(
    conn: sqlite3.Connection,
    company_domain: str,
    since: str | None = None,
) -> list[dict[str, Any]]:
    """Get emails for a company that haven't been processed for events yet.

    Finds emails in threads that either:
    - Have no events at all, OR
    - Have new emails since the last event extraction
    """
    like = f"%@{company_domain}%"

    # Get threads needing processing (same logic as _get_threads_to_process)
    thread_rows = fetchall(
        conn,
        """SELECT DISTINCT e.thread_id
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
             AND e.thread_id IN (
                 SELECT DISTINCT e2.thread_id FROM emails e2
                 WHERE e2.from_address LIKE ? OR e2.to_addresses LIKE ? OR e2.cc_addresses LIKE ?
             )
           ORDER BY e.date DESC
           LIMIT 20""",
        (like, like, like),
    )

    if not thread_rows:
        return []

    # Get all emails from these threads
    thread_ids = [r["thread_id"] for r in thread_rows]
    placeholders = ",".join("?" for _ in thread_ids)
    emails = fetchall(
        conn,
        f"""SELECT message_id, date, from_address, from_name, to_addresses,
                   cc_addresses, subject, body_text, thread_id
            FROM emails
            WHERE thread_id IN ({placeholders})
            ORDER BY date ASC""",
        tuple(thread_ids),
    )
    return [dict(e) for e in emails]


def count_new_threads_for_company(
    conn: sqlite3.Connection,
    company_domain: str,
) -> int:
    """Count threads with unprocessed emails for a company. Lightweight check."""
    like = f"%@{company_domain}%"
    row = fetchone(
        conn,
        """SELECT COUNT(DISTINCT e.thread_id) as cnt
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
             AND e.thread_id IN (
                 SELECT DISTINCT e2.thread_id FROM emails e2
                 WHERE e2.from_address LIKE ? OR e2.to_addresses LIKE ? OR e2.cc_addresses LIKE ?
             )""",
        (like, like, like),
    )
    return row["cnt"] if row else 0


def _build_discussions_context(
    conn: sqlite3.Connection,
    company_id: int,
    categories_config: list[dict[str, Any]],
) -> str:
    """Build a text block describing all existing discussions for a company."""
    discussions = fetchall(
        conn,
        """SELECT d.id, d.title, d.category, d.current_state, d.summary,
                  d.participants, d.first_seen, d.last_seen, d.parent_id
           FROM discussions d
           WHERE d.company_id = ?
           ORDER BY d.last_seen DESC""",
        (company_id,),
    )

    if not discussions:
        return "No existing discussions for this company."

    # Build category milestone lookup
    cat_milestones: dict[str, list[str]] = {}
    cat_states: dict[str, list[str]] = {}
    cat_terminal: dict[str, set[str]] = {}
    for cat in categories_config:
        ms = cat.get("milestones", [])
        cat_milestones[cat["name"]] = [m["name"] if isinstance(m, dict) else m for m in ms]
        cat_states[cat["name"]] = cat.get("workflow_states", [])
        cat_terminal[cat["name"]] = set(cat.get("terminal_states", []))

    blocks = ["Existing discussions:"]
    for disc in discussions:
        disc_id = disc["id"]
        cat = disc["category"] or "other"
        parent_note = f" (sub-discussion of {disc['parent_id']})" if disc["parent_id"] else ""

        # Get recent events (last 5)
        events = fetchall(
            conn,
            """SELECT event_date, type, domain, detail
               FROM event_ledger WHERE discussion_id = ?
               ORDER BY event_date DESC LIMIT 5""",
            (disc_id,),
        )
        events_text = ""
        if events:
            events_text = "\n  Recent events:\n"
            for ev in reversed(events):  # chronological
                events_text += f"    {ev['event_date']} {ev['domain']}/{ev['type']}: {(ev['detail'] or '')[:80]}\n"

        # Get milestones
        milestones = fetchall(
            conn,
            "SELECT name, achieved, achieved_date FROM milestones WHERE discussion_id = ?",
            (disc_id,),
        )
        achieved = [f"{m['name']} ({m['achieved_date']})" for m in milestones if m["achieved"]]
        milestones_text = f"\n  Milestones achieved: {', '.join(achieved)}" if achieved else ""

        # Workflow states for this category
        states = cat_states.get(cat, [])
        terminal = cat_terminal.get(cat, set())
        states_text = ""
        if states:
            parts = [f"{s}*" if s in terminal else s for s in states]
            states_text = f"\n  States: {' → '.join(parts)}  (* = terminal)"

        blocks.append(
            f"\n- ID {disc_id}: \"{disc['title']}\" [{cat}]{parent_note}"
            f"\n  State: {disc['current_state'] or '?'} | "
            f"{disc['first_seen'][:10] if disc['first_seen'] else '?'} to "
            f"{disc['last_seen'][:10] if disc['last_seen'] else '?'}"
            f"\n  Summary: {disc['summary'] or 'No summary yet.'}"
            f"{milestones_text}{states_text}{events_text}"
        )

    return "\n".join(blocks)


def _format_new_emails(emails: list[dict[str, Any]], body_limit: int = 800) -> str:
    """Format new emails for the prompt, grouped by thread."""
    if not emails:
        return "No new emails."

    # Group by thread
    threads: dict[str, list[dict]] = {}
    for e in emails:
        tid = e.get("thread_id", "unknown")
        threads.setdefault(tid, []).append(e)

    parts = []
    email_idx = 0
    for tid, thread_emails in threads.items():
        subject = thread_emails[0].get("subject") or "(no subject)"
        parts.append(f"\n--- Thread: {subject} ---")

        # Format with quote stripping
        formatted = _format_thread_emails(thread_emails, body_per_email=body_limit)
        for i, fmt in enumerate(formatted):
            # Replace the [Email N] index with global index
            fmt = fmt.replace(f"[Email {i}]", f"[Email {email_idx}]", 1)
            parts.append(fmt)
            email_idx += 1

    return "\n".join(parts)


# ── Saving results ─────────────────────────────────────────────────────────

def _save_quick_update_results(
    conn: sqlite3.Connection,
    result: dict[str, Any],
    company_id: int,
    emails: list[dict[str, Any]],
    model_name: str,
    categories_config: list[dict[str, Any]],
) -> dict[str, int]:
    """Save all results from a quick update LLM call."""
    now = datetime.now(timezone.utc).isoformat()
    counts = {"events": 0, "new_discussions": 0, "updates": 0, "actions": 0}

    # Build type-to-domain lookup for validation
    all_type_to_domain: dict[str, str] = {}
    for cat in categories_config:
        for et in cat.get("event_types", []):
            type_name = et["name"] if isinstance(et, dict) else et
            all_type_to_domain[type_name] = cat["name"]

    # 1. Create new discussions first (so we can resolve temp_ids)
    temp_to_real: dict[str, int] = {}
    for new_disc in result.get("new_discussions", []):
        temp_id = new_disc.get("temp_id", "")
        cursor = conn.execute(
            """INSERT INTO discussions (title, category, current_state, company_id,
               summary, participants, first_seen, last_seen, model_used, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                new_disc.get("title", "Untitled"),
                new_disc.get("category", "other"),
                None,
                company_id,
                None,
                json.dumps(new_disc.get("participants", [])),
                now, now, model_name, now,
            ),
        )
        temp_to_real[temp_id] = cursor.lastrowid
        counts["new_discussions"] += 1

    # 2. Save events and link to discussions
    for ev in result.get("events", []):
        event_type = ev.get("type", "")
        domain = ev.get("domain", "")

        # Validate event type
        if event_type not in all_type_to_domain:
            logger.warning("Skipping unknown event type '%s'", event_type)
            continue

        # Resolve discussion_id
        disc_id = ev.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        elif not isinstance(disc_id, int):
            disc_id = None

        # Resolve source email
        source_email_id = None
        source_idx = ev.get("source_email_index")
        if source_idx is not None and 0 <= source_idx < len(emails):
            source_email_id = emails[source_idx]["message_id"]

        raw_actor = ev.get("actor") or ""
        raw_target = ev.get("target") or ""
        actor = ", ".join(raw_actor) if isinstance(raw_actor, list) else raw_actor
        target = ", ".join(raw_target) if isinstance(raw_target, list) else raw_target

        thread_id = None
        if source_email_id:
            row = fetchone(conn, "SELECT thread_id FROM emails WHERE message_id = ?", (source_email_id,))
            if row:
                thread_id = row["thread_id"]

        conn.execute(
            """INSERT OR IGNORE INTO event_ledger
               (id, thread_id, source_email_id, source_calendar_event_id,
                source_type, source_id, discussion_id,
                domain, type, actor, target, event_date, detail, confidence,
                model_version, prompt_version, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                f"evt_{uuid.uuid4().hex[:12]}",
                thread_id,
                source_email_id,
                None,
                "email",
                source_email_id,
                disc_id,
                domain,
                event_type,
                actor,
                target,
                ev.get("event_date"),
                ev.get("detail"),
                ev.get("confidence", 0.5),
                model_name,
                PROMPT_VERSION,
                now,
            ),
        )
        counts["events"] += 1

        # Link thread to discussion
        if disc_id and thread_id:
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (disc_id, thread_id),
            )

    # 3. Apply discussion updates
    for update in result.get("discussion_updates", []):
        disc_id = update.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        if not isinstance(disc_id, int):
            continue

        # Update state and summary
        new_state = update.get("workflow_state")
        summary = update.get("summary")

        old = fetchone(conn, "SELECT current_state FROM discussions WHERE id = ?", (disc_id,))
        old_state = old["current_state"] if old else None

        conn.execute(
            """UPDATE discussions SET
               current_state = COALESCE(?, current_state),
               summary = COALESCE(?, summary),
               model_used = ?,
               updated_at = ?
               WHERE id = ?""",
            (new_state, summary, model_name, now, disc_id),
        )

        # Update date range
        conn.execute(
            """UPDATE discussions SET
               first_seen = MIN(first_seen, (SELECT MIN(event_date) FROM event_ledger WHERE discussion_id = ?)),
               last_seen = MAX(last_seen, (SELECT MAX(event_date) FROM event_ledger WHERE discussion_id = ?))
               WHERE id = ?""",
            (disc_id, disc_id, disc_id),
        )

        # Record state transition
        if new_state and new_state != old_state:
            conn.execute(
                """INSERT INTO discussion_state_history
                   (discussion_id, state, entered_at, reasoning, model_used, detected_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (disc_id, new_state, now, "Quick update from new emails", model_name, now),
            )

        # Save milestones
        for m in update.get("milestones", []):
            conn.execute(
                """INSERT INTO milestones (discussion_id, name, achieved, achieved_date,
                   evidence_event_ids, confidence, last_evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(discussion_id, name) DO UPDATE SET
                   achieved = excluded.achieved,
                   achieved_date = excluded.achieved_date,
                   confidence = excluded.confidence,
                   last_evaluated_at = excluded.last_evaluated_at""",
                (
                    disc_id,
                    m.get("name", ""),
                    1 if m.get("achieved") else 0,
                    m.get("achieved_date"),
                    json.dumps([]),
                    m.get("confidence", 0.0),
                    now,
                ),
            )

        # Save proposed actions (replace existing)
        actions = update.get("proposed_actions", [])
        if actions:
            conn.execute("DELETE FROM proposed_actions WHERE discussion_id = ?", (disc_id,))
            for action in actions:
                conn.execute(
                    """INSERT INTO proposed_actions
                       (discussion_id, action, reasoning, priority, wait_until, assignee, model_used, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        disc_id,
                        action.get("action", ""),
                        action.get("reasoning"),
                        action.get("priority", "medium"),
                        action.get("wait_until"),
                        action.get("assignee"),
                        model_name,
                        now,
                    ),
                )
                counts["actions"] += 1

        counts["updates"] += 1

    conn.commit()
    return counts


# ── Public entry points ────────────────────────────────────────────────────

def _llm_result_to_proposed(
    result: dict[str, Any],
    emails: list[dict[str, Any]],
    categories_config: list[dict[str, Any]],
) -> dict[str, Any]:
    """Convert a quick_update LLM response into a ProposedChanges-compatible dict.

    Resolves source_email_index to message_id and thread_id, validates event types.
    """
    # Build type-to-domain lookup
    all_type_to_domain: dict[str, str] = {}
    for cat in categories_config:
        for et in cat.get("event_types", []):
            type_name = et["name"] if isinstance(et, dict) else et
            all_type_to_domain[type_name] = cat["name"]

    # Build email index → (message_id, thread_id) lookup
    email_lookup: dict[int, dict] = {}
    for i, e in enumerate(emails):
        email_lookup[i] = e

    events = []
    for ev in result.get("events", []):
        event_type = ev.get("type", "")
        if event_type not in all_type_to_domain:
            continue

        source_idx = ev.get("source_email_index")
        source_email_id = None
        thread_id = None
        if source_idx is not None and source_idx in email_lookup:
            source_email_id = email_lookup[source_idx].get("message_id")
            thread_id = email_lookup[source_idx].get("thread_id")

        raw_actor = ev.get("actor") or ""
        raw_target = ev.get("target") or ""
        actor = ", ".join(raw_actor) if isinstance(raw_actor, list) else raw_actor
        target = ", ".join(raw_target) if isinstance(raw_target, list) else raw_target

        events.append({
            "thread_id": thread_id,
            "source_email_id": source_email_id,
            "discussion_id": ev.get("discussion_id"),
            "domain": ev.get("domain", ""),
            "type": event_type,
            "actor": actor,
            "target": target,
            "event_date": ev.get("event_date"),
            "detail": ev.get("detail"),
            "confidence": ev.get("confidence", 0.5),
        })

    new_discussions = []
    for nd in result.get("new_discussions", []):
        new_discussions.append({
            "temp_id": nd.get("temp_id", ""),
            "title": nd.get("title", "Untitled"),
            "category": nd.get("category", "other"),
            "parent_id": nd.get("parent_id"),
            "participants": nd.get("participants", []),
        })

    discussion_updates = []
    for upd in result.get("discussion_updates", []):
        discussion_updates.append({
            "discussion_id": upd.get("discussion_id"),
            "state": upd.get("workflow_state"),
            "summary": upd.get("summary"),
            "milestones": upd.get("milestones", []),
            "proposed_actions": upd.get("proposed_actions", []),
        })

    # Build thread links from events
    thread_links = []
    seen_links: set[tuple] = set()
    for ev in events:
        disc_id = ev.get("discussion_id")
        tid = ev.get("thread_id")
        if disc_id and tid and (disc_id, tid) not in seen_links:
            thread_links.append({"discussion_id": disc_id, "thread_id": tid})
            seen_links.add((disc_id, tid))

    return {
        "events": events,
        "new_discussions": new_discussions,
        "discussion_updates": discussion_updates,
        "thread_links": thread_links,
    }


def quick_update_propose(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    company_domain: str,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
) -> tuple[dict[str, Any] | None, dict]:
    """Run the LLM and return a ProposedChanges-compatible dict without writing to DB.

    Returns (proposed_dict, company_info) or (None, company_info) if no new emails.
    company_info has keys: id, name, domain.
    """
    if categories_config is None:
        categories_config = load_category_config(config_path)

    company = fetchone(
        conn,
        "SELECT id, name, domain FROM companies WHERE domain = ? COLLATE NOCASE",
        (company_domain,),
    )
    if not company:
        logger.warning("Company not found: %s", company_domain)
        return None, {}

    company_info = {"id": company["id"], "name": company["name"], "domain": company["domain"]}

    emails = _get_new_emails_for_company(conn, company["domain"])
    if not emails:
        logger.info("No new emails for %s", company_domain)
        return None, company_info

    logger.info("Quick update for %s: %d new emails", company_domain, len(emails))

    account_owner = _detect_account_owner(conn)
    domains_block = _build_domains_block(categories_config)
    discussions_context = _build_discussions_context(conn, company["id"], categories_config)
    new_emails_text = _format_new_emails(emails)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Enrich system prompt with learned rules
    from email_manager.analysis.feedback import format_rules_block, LAYER_QUICK_UPDATE
    system_prompt = QUICK_UPDATE_SYSTEM + format_rules_block(conn, LAYER_QUICK_UPDATE)

    user_prompt = _build_quick_update_prompt(
        company["name"], company["domain"],
        new_emails_text, discussions_context, domains_block,
        account_owner, today,
    )

    try:
        result = backend.complete_json(system_prompt, user_prompt)
    except Exception as e:
        logger.error("LLM call failed for quick update %s: %s", company_domain, e)
        return None, company_info

    proposed = _llm_result_to_proposed(result, emails, categories_config)
    return proposed, company_info


def quick_update(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    company_domain: str,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
) -> dict[str, int]:
    """Process new emails for a company in a single LLM call.

    Returns dict with counts: events, new_discussions, updates, actions.
    Uses the unified propose-then-apply path with provenance tracking.
    """
    from email_manager.ai.agent_backend import ProposedChanges, apply_changes

    proposed_dict, company_info = quick_update_propose(
        conn, backend, company_domain,
        categories_config=categories_config,
        config_path=config_path,
    )

    if not company_info or not proposed_dict:
        return {"events": 0, "new_discussions": 0, "updates": 0, "actions": 0}

    proposed = ProposedChanges(proposed_dict)
    if proposed.is_empty:
        return {"events": 0, "new_discussions": 0, "updates": 0, "actions": 0}

    counts = apply_changes(
        conn, proposed, company_info["id"], company_info["domain"],
        mode="quick", model=backend.model_name,
        token_tracker=getattr(backend, 'token_tracker', None),
    )

    logger.info(
        "Quick update %s: %d events, %d new discussions, %d updates, %d actions",
        company_domain, counts["events"], counts["new_discussions"],
        counts["updates"], counts["actions"],
    )

    return counts
