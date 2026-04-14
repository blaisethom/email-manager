"""Agent-based company processing using the Claude Code SDK.

The agent gets read-only database tools and proposes a structured changeset.
We review and apply the changes ourselves, maintaining control of the database.

Flow:
1. Agent reads emails, discussions, and category config via tools
2. Agent analyzes and calls propose_changes with a structured diff
3. We receive the diff and apply it to the database
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

# Lazy import — claude_agent_sdk is only needed for agent mode, not for
# ProposedChanges/apply_changes which are used by all pipeline stages.
def _import_agent_sdk():
    from claude_agent_sdk import (
        ClaudeAgentOptions,
        ResultMessage,
        AssistantMessage,
        TextBlock,
        ToolUseBlock,
        create_sdk_mcp_server,
        query,
        tool,
    )
    return (ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock,
            ToolUseBlock, create_sdk_mcp_server, query, tool)

from email_manager.ai.base import TokenTracker, TokenUsage
from email_manager.analysis.events import (
    _detect_account_owner,
    _strip_quoted_text,
    _dedup_against_previous,
    load_category_config,
    PROMPT_VERSION,
)
from email_manager.change_journal import record_change
from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.ai.agent_backend")


# ── Proposed changeset structure ──────────────────────────────────────────

class ProposedChanges:
    """Structured changeset proposed by an LLM analysis stage.

    Used by all codepaths: quick_update, agent mode, and staged pipeline.
    Each processing run snapshots its ProposedChanges for later review/evaluation.
    """

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw
        self.events: list[dict] = raw.get("events", [])
        self.new_discussions: list[dict] = raw.get("new_discussions", [])
        self.discussion_updates: list[dict] = raw.get("discussion_updates", [])
        self.thread_links: list[dict] = raw.get("thread_links", [])
        # For staged pipeline: assign existing events to discussions
        self.event_assignments: list[dict] = raw.get("event_assignments", [])
        # For company labeling
        self.label_updates: list[dict] = raw.get("label_updates", [])

    @property
    def is_empty(self) -> bool:
        return (not self.events and not self.new_discussions
                and not self.discussion_updates and not self.event_assignments
                and not self.label_updates)

    def to_dict(self) -> dict[str, Any]:
        """Serialisable representation for snapshotting."""
        d: dict[str, Any] = {}
        if self.events:
            d["events"] = self.events
        if self.new_discussions:
            d["new_discussions"] = self.new_discussions
        if self.discussion_updates:
            d["discussion_updates"] = self.discussion_updates
        if self.thread_links:
            d["thread_links"] = self.thread_links
        if self.event_assignments:
            d["event_assignments"] = self.event_assignments
        if self.label_updates:
            d["label_updates"] = self.label_updates
        return d

    def summary_lines(self) -> list[str]:
        """Human-readable summary of proposed changes."""
        lines: list[str] = []
        if self.events:
            lines.append(f"  Events to add: {len(self.events)}")
            for ev in self.events:
                disc = f" → discussion #{ev.get('discussion_id', '?')}" if ev.get("discussion_id") else ""
                lines.append(
                    f"    {ev.get('event_date', '?')} {ev.get('domain', '?')}/{ev.get('type', '?')}: "
                    f"{(ev.get('detail') or '')[:60]}{disc}"
                )
        if self.new_discussions:
            lines.append(f"  New discussions: {len(self.new_discussions)}")
            for d in self.new_discussions:
                parent = f" (sub of #{d['parent_id']})" if d.get("parent_id") else ""
                lines.append(f"    \"{d.get('title', '?')}\" [{d.get('category', '?')}]{parent}")
        if self.discussion_updates:
            lines.append(f"  Discussion updates: {len(self.discussion_updates)}")
            for u in self.discussion_updates:
                parts = []
                if u.get("state"):
                    parts.append(f"state → {u['state']}")
                if u.get("summary"):
                    parts.append("summary updated")
                if u.get("milestones"):
                    achieved = [m["name"] for m in u["milestones"] if m.get("achieved")]
                    if achieved:
                        parts.append(f"milestones: {', '.join(achieved)}")
                if u.get("proposed_actions"):
                    parts.append(f"{len(u['proposed_actions'])} actions")
                lines.append(f"    #{u.get('discussion_id', '?')}: {', '.join(parts) or 'no changes'}")
        if self.event_assignments:
            lines.append(f"  Event assignments: {len(self.event_assignments)}")
        if self.label_updates:
            lines.append(f"  Label updates: {len(self.label_updates)}")
            for lu in self.label_updates:
                labels = [l["label"] for l in lu.get("labels", [])]
                lines.append(f"    company #{lu.get('company_id', '?')}: {', '.join(labels)}")
        if self.thread_links:
            lines.append(f"  Thread links: {len(self.thread_links)}")
        return lines


def apply_changes(
    conn: sqlite3.Connection,
    changes: ProposedChanges,
    company_id: int,
    company_domain: str,
    mode: str = "agent",
    model: str | None = None,
    token_tracker: Any | None = None,
    run_id: int | None = None,
    prompt_hash: str | None = None,
    started_at: str | None = None,
) -> dict[str, int]:
    """Apply a proposed changeset to the database. Returns counts.

    Creates a processing_run record (or reuses run_id if provided) and stamps
    all derived data with run_id. Snapshots the ProposedChanges JSON for
    later review/evaluation.

    If token_tracker is provided, records token usage from the tracker.
    If prompt_hash is provided, records the system prompt content hash for
    versioning (detect when prompts change and stages need re-running).
    """
    now = datetime.now(timezone.utc).isoformat()
    counts = {"events": 0, "new_discussions": 0, "updates": 0, "actions": 0,
              "event_assignments": 0}

    # Create processing run (or reuse existing)
    if run_id is None:
        # Find parent: latest run for the same company + mode
        parent_row = fetchone(
            conn,
            """SELECT id FROM processing_runs
               WHERE company_domain = ? AND mode = ?
               ORDER BY id DESC LIMIT 1""",
            (company_domain, mode),
        )
        parent_run_id = parent_row["id"] if parent_row else None

        # Compute input boundary: latest email date for this company
        email_cutoff = None
        if company_domain and company_domain != "all":
            like = f"%@{company_domain}%"
            cutoff_row = fetchone(
                conn,
                """SELECT MAX(date) as cutoff FROM emails
                   WHERE from_address LIKE ? OR to_addresses LIKE ?""",
                (like, like),
            )
            if cutoff_row and cutoff_row["cutoff"]:
                email_cutoff = cutoff_row["cutoff"]

        cursor = conn.execute(
            """INSERT INTO processing_runs
               (company_domain, mode, model, started_at, parent_run_id, email_cutoff_date, prompt_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (company_domain, mode, model or "unknown", started_at or now, parent_run_id, email_cutoff, prompt_hash),
        )
        run_id = cursor.lastrowid

    # Snapshot the proposed changes for review/evaluation
    conn.execute(
        "UPDATE processing_runs SET proposed_changes_json = ? WHERE id = ?",
        (json.dumps(changes.to_dict()), run_id),
    )

    # Build type-to-domain lookup for validation
    categories = load_category_config()
    valid_types: set[str] = set()
    for cat in categories:
        for et in cat.get("event_types", []):
            valid_types.add(et["name"] if isinstance(et, dict) else et)

    # 1. Create new discussions (track temp_id → real_id mapping)
    # Two-pass: first create all discussions, then resolve parent_id references
    temp_to_real: dict[str, int] = {}
    for new_disc in changes.new_discussions:
        temp_id = new_disc.get("temp_id", "")
        cursor = conn.execute(
            """INSERT INTO discussions
               (title, category, current_state, company_id, run_id,
                summary, participants, first_seen, last_seen, model_used, updated_at)
               VALUES (?, ?, NULL, ?, ?, NULL, ?, NULL, NULL, ?, ?)""",
            (
                new_disc.get("title", "Untitled"),
                new_disc.get("category", "other"),
                company_id, run_id,
                json.dumps(new_disc.get("participants", [])),
                model or mode, now,
            ),
        )
        real_id = cursor.lastrowid
        temp_to_real[temp_id] = real_id
        counts["new_discussions"] += 1

    # Second pass: resolve parent_id references
    for new_disc in changes.new_discussions:
        parent_id = new_disc.get("parent_id")
        if parent_id is None:
            continue
        temp_id = new_disc.get("temp_id", "")
        real_id = temp_to_real.get(temp_id)
        if not real_id:
            continue
        # Resolve parent: could be a temp_id or an existing discussion ID
        if isinstance(parent_id, str) and parent_id in temp_to_real:
            resolved_parent = temp_to_real[parent_id]
        elif isinstance(parent_id, int):
            resolved_parent = parent_id
        else:
            continue
        conn.execute(
            "UPDATE discussions SET parent_id = ? WHERE id = ?",
            (resolved_parent, real_id),
        )

    # 2. Save events
    thread_ids_with_new_events: set[str] = set()  # for change journal
    for ev in changes.events:
        event_type = ev.get("type", "")
        if event_type not in valid_types:
            logger.warning("Skipping unknown event type '%s'", event_type)
            continue

        # Resolve temp discussion IDs
        disc_id = ev.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        elif not isinstance(disc_id, int):
            disc_id = None

        source_email_id = ev.get("source_email_id")
        # Use pre-generated ID if present (from extract_events), else generate
        evt_id = ev.get("id") or f"evt_{uuid.uuid4().hex[:12]}"
        # Support richer source fields from extract_events
        source_cal_id = ev.get("source_calendar_event_id")
        source_type = ev.get("source_type", "email")
        source_id = ev.get("source_id", source_email_id)

        conn.execute(
            """INSERT OR IGNORE INTO event_ledger
               (id, thread_id, source_email_id, source_calendar_event_id,
                source_type, source_id, run_id, discussion_id,
                domain, type, actor, target, event_date, detail, confidence,
                model_version, prompt_version, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                evt_id, ev.get("thread_id"), source_email_id,
                source_cal_id, source_type, source_id,
                run_id, disc_id, ev.get("domain", ""), event_type,
                ev.get("actor"), ev.get("target"),
                ev.get("event_date"), ev.get("detail"),
                ev.get("confidence", 0.5),
                ev.get("model_version", model or mode),
                ev.get("prompt_version", PROMPT_VERSION),
                ev.get("created_at", now),
            ),
        )
        counts["events"] += 1
        if ev.get("thread_id"):
            thread_ids_with_new_events.add(ev["thread_id"])

    # Record thread-level changes in the journal (for discover_discussions to pick up)
    if thread_ids_with_new_events:
        from email_manager.change_journal import record_changes as _record_changes
        stage_name = mode.split(":")[-1] if ":" in mode else mode
        _record_changes(
            conn,
            [("thread", tid, "new_event", stage_name) for tid in thread_ids_with_new_events],
        )

    # 2b. Compute first_seen/last_seen from event dates for all affected discussions
    all_disc_ids = set(temp_to_real.values())
    for ev in changes.events:
        disc_id = ev.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            all_disc_ids.add(temp_to_real[disc_id])
        elif isinstance(disc_id, int):
            all_disc_ids.add(disc_id)

    for disc_id in all_disc_ids:
        conn.execute(
            """UPDATE discussions SET
               first_seen = COALESCE(
                   (SELECT MIN(event_date) FROM event_ledger WHERE discussion_id = ?),
                   first_seen
               ),
               last_seen = COALESCE(
                   (SELECT MAX(event_date) FROM event_ledger WHERE discussion_id = ?),
                   last_seen
               )
               WHERE id = ?""",
            (disc_id, disc_id, disc_id),
        )

    # 3. Link threads to discussions
    for link in changes.thread_links:
        disc_id = link.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        if isinstance(disc_id, int) and link.get("thread_id"):
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (disc_id, link["thread_id"]),
            )

    # 3b. Apply event assignments (staged discover_discussions)
    for assignment in changes.event_assignments:
        evt_id = assignment.get("event_id")
        disc_id = assignment.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        if evt_id and isinstance(disc_id, int):
            conn.execute(
                "UPDATE event_ledger SET discussion_id = ? WHERE id = ?",
                (disc_id, evt_id),
            )
            counts["event_assignments"] += 1
            # Track affected discussion for date recomputation
            all_disc_ids.add(disc_id)

    # Recompute date ranges for discussions affected by event assignments
    if changes.event_assignments:
        for disc_id in all_disc_ids:
            conn.execute(
                """UPDATE discussions SET
                   first_seen = COALESCE(
                       (SELECT MIN(event_date) FROM event_ledger WHERE discussion_id = ?),
                       first_seen
                   ),
                   last_seen = COALESCE(
                       (SELECT MAX(event_date) FROM event_ledger WHERE discussion_id = ?),
                       last_seen
                   )
                   WHERE id = ?""",
                (disc_id, disc_id, disc_id),
            )

    # 4. Apply discussion updates
    for update in changes.discussion_updates:
        disc_id = update.get("discussion_id")
        if isinstance(disc_id, str) and disc_id in temp_to_real:
            disc_id = temp_to_real[disc_id]
        if not isinstance(disc_id, int):
            continue

        new_state = update.get("state")
        summary = update.get("summary")

        old = fetchone(conn, "SELECT current_state FROM discussions WHERE id = ?", (disc_id,))
        if not old:
            continue
        old_state = old["current_state"]

        conn.execute(
            """UPDATE discussions SET
               current_state = COALESCE(?, current_state),
               summary = COALESCE(?, summary),
               run_id = ?, model_used = ?, updated_at = ?
               WHERE id = ?""",
            (new_state, summary, run_id, model or mode, now, disc_id),
        )

        if new_state and new_state != old_state:
            conn.execute(
                """INSERT INTO discussion_state_history
                   (discussion_id, state, entered_at, reasoning, model_used, detected_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (disc_id, new_state, now, f"{mode} update", model or mode, now),
            )

        for m in update.get("milestones", []):
            conn.execute(
                """INSERT INTO milestones (discussion_id, run_id, name, achieved, achieved_date,
                   evidence_event_ids, confidence, last_evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(discussion_id, name) DO UPDATE SET
                   run_id = excluded.run_id,
                   achieved = excluded.achieved,
                   achieved_date = excluded.achieved_date,
                   confidence = excluded.confidence,
                   last_evaluated_at = excluded.last_evaluated_at""",
                (
                    disc_id, run_id, m.get("name", ""),
                    1 if m.get("achieved") else 0,
                    m.get("achieved_date"),
                    json.dumps(m.get("evidence_event_ids", [])),
                    m.get("confidence", 0.0), now,
                ),
            )

        actions = update.get("proposed_actions", [])
        if actions:
            conn.execute("DELETE FROM proposed_actions WHERE discussion_id = ?", (disc_id,))
            for a in actions:
                conn.execute(
                    """INSERT INTO proposed_actions
                       (discussion_id, run_id, action, reasoning, priority, wait_until, assignee, model_used, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        disc_id, run_id, a.get("action", ""), a.get("reasoning"),
                        a.get("priority", "medium"), a.get("wait_until"),
                        a.get("assignee"), model or mode, now,
                    ),
                )
                counts["actions"] += 1

        counts["updates"] += 1

    # 5. Apply label updates
    for lu in changes.label_updates:
        cid = lu.get("company_id")
        if not isinstance(cid, int):
            continue

        # Update company name/description if provided
        updates = []
        update_params: list[Any] = []
        if lu.get("company_name"):
            updates.append("name = ?")
            update_params.append(lu["company_name"])
        if lu.get("company_description"):
            updates.append("description = ?")
            update_params.append(lu["company_description"])
        if updates:
            update_params.append(cid)
            conn.execute(
                f"UPDATE companies SET {', '.join(updates)} WHERE id = ?",
                update_params,
            )

        # Replace labels
        if lu.get("labels"):
            conn.execute("DELETE FROM company_labels WHERE company_id = ?", (cid,))
            for entry in lu["labels"]:
                label_name = entry.get("label", "").strip()
                if not label_name:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO company_labels
                       (company_id, label, confidence, reasoning, model_used, assigned_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (cid, label_name,
                     min(1.0, max(0.0, float(entry.get("confidence", 0.5)))),
                     entry.get("reasoning", ""),
                     model or mode, now),
                )
        counts["label_updates"] = counts.get("label_updates", 0) + 1

    # Journal entry
    if not changes.is_empty:
        record_change(conn, "company", company_domain, f"{mode}_update", mode)

    # Record token usage if tracker provided
    total_input = 0
    total_output = 0
    total_calls = 0
    if token_tracker is not None:
        from email_manager.ai.base import TokenTracker
        if isinstance(token_tracker, TokenTracker):
            total_input = token_tracker.total_input
            total_output = token_tracker.total_output
            total_calls = token_tracker.call_count
            # Write individual call records
            for usage in token_tracker.calls:
                conn.execute(
                    """INSERT INTO llm_calls (run_id, stage, model, input_tokens, output_tokens, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (run_id, mode, model or "unknown", usage.input_tokens, usage.output_tokens, now),
                )

    # Complete the processing run record (fresh timestamp for actual completion time)
    completed_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE processing_runs SET
           completed_at = ?, events_created = ?, discussions_created = ?,
           discussions_updated = ?, actions_proposed = ?,
           input_tokens = ?, output_tokens = ?, llm_calls = ?
           WHERE id = ?""",
        (completed_at, counts["events"], counts["new_discussions"],
         counts["updates"], counts["actions"],
         total_input, total_output, total_calls, run_id),
    )

    conn.commit()
    counts["run_id"] = run_id
    return counts


# ── Read-only tools + propose_changes ─────────────────────────────────────

def _build_tools(conn: sqlite3.Connection, company_domain: str, company_id: int):
    """Build MCP tools: read-only DB access + one propose_changes output tool."""
    (_, _, _, _, _, create_sdk_mcp_server, query, tool) = _import_agent_sdk()

    @tool(
        "get_new_emails",
        "Get emails from threads that have new/unprocessed emails for this company. "
        "Returns emails grouped by thread, with quote-stripped bodies.",
        {"limit": int},
    )
    async def get_new_emails(args: dict[str, Any]) -> dict[str, Any]:
        limit = args.get("limit", 20)
        like = f"%@{company_domain}%"

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
                     WHERE e2.from_address LIKE ? OR e2.to_addresses LIKE ?
                       OR e2.cc_addresses LIKE ?
                 )
               ORDER BY e.date DESC
               LIMIT ?""",
            (like, like, like, limit),
        )

        if not thread_rows:
            return _text("No new emails found for this company.")

        thread_ids = [r["thread_id"] for r in thread_rows]
        placeholders = ",".join("?" for _ in thread_ids)
        emails = fetchall(
            conn,
            f"""SELECT message_id, date, from_address, from_name, to_addresses,
                       cc_addresses, subject, body_text, thread_id
                FROM emails
                WHERE thread_id IN ({placeholders})
                ORDER BY thread_id, date ASC""",
            tuple(thread_ids),
        )

        threads: dict[str, list] = {}
        for e in emails:
            threads.setdefault(e["thread_id"], []).append(dict(e))

        parts = []
        for tid, thread_emails in threads.items():
            subject = thread_emails[0].get("subject") or "(no subject)"
            parts.append(f"\n=== Thread: {tid} ===")
            parts.append(f"Subject: {subject}")
            prev_bodies: list[str] = []
            for i, e in enumerate(thread_emails):
                body = _strip_quoted_text(e.get("body_text") or "")
                body = _dedup_against_previous(body, prev_bodies)
                prev_bodies.append(body)
                body = body[:800]
                parts.append(
                    f"[Email {i}] [{(e['date'] or '')[:10]}] "
                    f"From: {e['from_name'] or e['from_address']} <{e['from_address']}> "
                    f"To: {e['to_addresses'] or ''}\n"
                    f"Message-ID: {e['message_id']}\n"
                    f"{body}"
                )
            parts.append(f"=== End Thread: {tid} ===\n")

        return _text(f"Found {len(threads)} threads with new emails:\n" + "\n".join(parts))

    @tool(
        "get_discussions",
        "Get all existing discussions for this company with their current state, "
        "summary, recent events, and milestones.",
        {},
    )
    async def get_discussions(args: dict[str, Any]) -> dict[str, Any]:
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
            return _text("No existing discussions for this company.")

        parts = []
        for disc in discussions:
            events = fetchall(
                conn,
                """SELECT event_date, type, domain, detail
                   FROM event_ledger WHERE discussion_id = ?
                   ORDER BY event_date DESC LIMIT 5""",
                (disc["id"],),
            )
            events_text = ""
            if events:
                events_text = "\n  Recent events:"
                for ev in reversed(events):
                    events_text += f"\n    {ev['event_date']} {ev['domain']}/{ev['type']}: {(ev['detail'] or '')[:80]}"

            milestones = fetchall(
                conn,
                "SELECT name, achieved, achieved_date FROM milestones WHERE discussion_id = ?",
                (disc["id"],),
            )
            achieved = [f"{m['name']} ({m['achieved_date']})" for m in milestones if m["achieved"]]
            ms_text = f"\n  Milestones achieved: {', '.join(achieved)}" if achieved else ""

            parent = f" (sub-discussion of {disc['parent_id']})" if disc["parent_id"] else ""
            parts.append(
                f"- ID {disc['id']}: \"{disc['title']}\" [{disc['category']}]{parent}\n"
                f"  State: {disc['current_state'] or '?'} | "
                f"{(disc['first_seen'] or '?')[:10]} to {(disc['last_seen'] or '?')[:10]}\n"
                f"  Summary: {disc['summary'] or 'No summary yet.'}"
                f"{ms_text}{events_text}"
            )

        return _text("Existing discussions:\n\n" + "\n\n".join(parts))

    @tool(
        "get_category_config",
        "Get the available discussion categories, event types, states, and milestones.",
        {},
    )
    async def get_category_config(args: dict[str, Any]) -> dict[str, Any]:
        categories = load_category_config()
        if not categories:
            return _text("No category configuration found.")

        parts = []
        for cat in categories:
            lines = [f"Category: {cat['name']} — {cat.get('description', '')}"]

            event_types = cat.get("event_types", [])
            if event_types:
                lines.append("  Event types:")
                for et in event_types:
                    if isinstance(et, dict):
                        lines.append(f"    - {et['name']}: {et['description']}")
                    else:
                        lines.append(f"    - {et}")

            states = cat.get("states", cat.get("workflow_states", []))
            if states:
                terminal = set(cat.get("terminal_states", []))
                parts_s = [f"{s}*" if s in terminal else s for s in states]
                lines.append(f"  States: {' → '.join(parts_s)}  (* = terminal)")

            milestones = cat.get("milestones", [])
            if milestones:
                lines.append("  Milestones:")
                for m in milestones:
                    if isinstance(m, dict):
                        lines.append(f"    - {m['name']}: {m.get('description', '')}")
                    else:
                        lines.append(f"    - {m}")

            parts.append("\n".join(lines))

        return _text("\n\n".join(parts))

    # Accumulator for incremental changes
    changeset: dict[str, list] = {
        "events": [],
        "new_discussions": [],
        "discussion_updates": [],
        "thread_links": [],
    }

    @tool(
        "add_event",
        "Add a proposed event to the changeset. Call once per event as you process each thread.",
        {
            "thread_id": str,
            "source_email_id": str,
            "discussion_id": str,
            "domain": str,
            "type": str,
            "actor": str,
            "target": str,
            "event_date": str,
            "detail": str,
            "confidence": float,
        },
    )
    async def add_event(args: dict[str, Any]) -> dict[str, Any]:
        # Coerce discussion_id to int if it's a numeric string
        disc_id = args.get("discussion_id")
        if disc_id and isinstance(disc_id, str):
            try:
                disc_id = int(disc_id)
            except ValueError:
                pass  # Keep as string (temp_id like "new_1")
            args["discussion_id"] = disc_id

        changeset["events"].append(args)

        # Auto-add thread link
        if args.get("thread_id") and args.get("discussion_id"):
            link = {"discussion_id": args["discussion_id"], "thread_id": args["thread_id"]}
            if link not in changeset["thread_links"]:
                changeset["thread_links"].append(link)

        return _text(f"Event added ({len(changeset['events'])} total): {args['domain']}/{args['type']} on {args.get('event_date', '?')}")

    @tool(
        "add_discussion",
        "Propose a new discussion to create. Returns a temp_id you can reference in events and updates.",
        {
            "temp_id": str,
            "title": str,
            "category": str,
            "parent_id": str,
            "participants": str,
        },
    )
    async def add_discussion(args: dict[str, Any]) -> dict[str, Any]:
        # Coerce parent_id
        parent_id = args.get("parent_id")
        if parent_id:
            try:
                parent_id = int(parent_id)
            except ValueError:
                pass  # Keep as string temp_id
        else:
            parent_id = None

        try:
            participants = json.loads(args.get("participants", "[]"))
        except (json.JSONDecodeError, TypeError):
            participants = []

        disc = {
            "temp_id": args["temp_id"],
            "title": args["title"],
            "category": args["category"],
            "parent_id": parent_id,
            "participants": participants,
        }
        changeset["new_discussions"].append(disc)
        parent_note = f" (sub of #{parent_id})" if parent_id else ""
        return _text(
            f"Discussion proposed ({len(changeset['new_discussions'])} total): "
            f"\"{args['title']}\" [{args['category']}]{parent_note} — use temp_id \"{args['temp_id']}\" in events"
        )

    @tool(
        "update_discussion",
        "Propose an update to an existing (or newly proposed) discussion's state, summary, milestones, and actions.",
        {
            "discussion_id": str,
            "state": str,
            "summary": str,
            "milestones_json": str,
            "actions_json": str,
        },
    )
    async def update_discussion_tool(args: dict[str, Any]) -> dict[str, Any]:
        disc_id = args.get("discussion_id")
        if disc_id:
            try:
                disc_id = int(disc_id)
            except ValueError:
                pass  # temp_id

        milestones = []
        if args.get("milestones_json"):
            try:
                milestones = json.loads(args["milestones_json"])
            except json.JSONDecodeError:
                pass

        actions = []
        if args.get("actions_json"):
            try:
                actions = json.loads(args["actions_json"])
            except json.JSONDecodeError:
                pass

        update = {
            "discussion_id": disc_id,
            "state": args.get("state"),
            "summary": args.get("summary"),
            "milestones": milestones,
            "proposed_actions": actions,
        }
        changeset["discussion_updates"].append(update)

        parts = []
        if args.get("state"):
            parts.append(f"state → {args['state']}")
        if args.get("summary"):
            parts.append("summary")
        if milestones:
            parts.append(f"{len(milestones)} milestones")
        if actions:
            parts.append(f"{len(actions)} actions")

        return _text(
            f"Discussion #{disc_id} update proposed ({len(changeset['discussion_updates'])} total): "
            f"{', '.join(parts) or 'no changes'}"
        )

    @tool(
        "finalise_changes",
        "Call this when you are done processing all threads. Returns a summary of all proposed changes.",
        {},
    )
    async def finalise_changes(args: dict[str, Any]) -> dict[str, Any]:
        n_events = len(changeset["events"])
        n_new = len(changeset["new_discussions"])
        n_updates = len(changeset["discussion_updates"])
        n_links = len(changeset["thread_links"])
        return _text(
            f"Changeset finalised: {n_events} events, {n_new} new discussions, "
            f"{n_updates} discussion updates, {n_links} thread links. "
            f"Changes will be reviewed before applying."
        )

    return [
        get_new_emails, get_discussions, get_category_config,
        add_event, add_discussion, update_discussion_tool, finalise_changes,
    ], changeset


def _text(msg: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": msg}]}


def _error(msg: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": msg}], "isError": True}


# ── Agent session ──────────────────────────────────────────────────────────

AGENT_SYSTEM_PROMPT = """You are a business email analysis agent. You process emails for one company at a time.

Your workflow:
1. Call get_category_config to understand the available event types, states, and milestones.
2. Call get_discussions to see existing discussions and their current state.
3. Call get_new_emails to read unprocessed emails.
4. Process the emails thread by thread. For each thread:
   a. Identify the business events evidenced in the emails.
   b. Call add_event for each event you find.
   c. If the thread belongs to a new topic, call add_discussion first to get a temp_id.
   d. After processing a thread's events, call update_discussion for any affected discussions.
5. Call finalise_changes when you are done with all threads.

IMPORTANT: Build up changes incrementally — call add_event, add_discussion, and update_discussion as you go. Do NOT try to accumulate everything and submit at the end.

You do NOT write to the database directly. You propose changes that will be reviewed before being applied.

Rules for event extraction:
- Extract only events clearly evidenced by email content.
- Each event must use a type from the category config vocabulary.
- Set source_email_id to the message_id of the email that evidences the event.
- Set thread_id to the thread the event came from.
- The "actor" is who performed the action (email address).
- Assign a confidence score (0.0-1.0).
- Do NOT re-extract events already listed in existing discussion context.
- IMPORTANT: Use the correct domain for each event. Emails about scheduling meetings should use the "scheduling" domain (meeting_proposed, times_suggested, time_confirmed, etc.), NOT the domain of what's being discussed. An email saying "Can we meet Tuesday to discuss the deal?" is scheduling/meeting_proposed, not an investment event.

Rules for discussions:
- Prefer assigning to existing discussions when the topic matches.
- Only call add_discussion if the emails clearly don't fit any existing one.
- Use temp_id like "new_1", "new_2" and reference it in add_event's discussion_id.

Rules for sub-discussions:
- Scheduling/logistics emails that support a larger discussion should be a sub-discussion.
- Call add_discussion with parent_id set to the main discussion's ID (or temp_id).
- The sub-discussion should have category="scheduling".

Rules for discussion updates:
- Call update_discussion for each discussion that received new events.
- State should reflect where the discussion stands after the new events.
- Summary should be 2-4 sentences.
- milestones_json: JSON array of [{name, achieved, achieved_date, confidence}].
- actions_json: JSON array of [{action, reasoning, priority, wait_until, assignee}].

After calling finalise_changes, provide a brief human-readable summary."""


async def _run_agent_for_company(
    conn: sqlite3.Connection,
    company_domain: str,
    company_id: int,
    company_name: str,
    model: str | None = None,
) -> tuple[ProposedChanges | None, str]:
    """Run an agent session. Returns (proposed_changes, summary_text)."""
    (ClaudeAgentOptions, ResultMessage, AssistantMessage, TextBlock,
     ToolUseBlock, create_sdk_mcp_server, query, _tool) = _import_agent_sdk()

    tools, changeset = _build_tools(conn, company_domain, company_id)

    server = create_sdk_mcp_server(
        name="email_db",
        version="1.0.0",
        tools=tools,
    )

    tool_names = [f"mcp__email_db__{t.name}" for t in tools]

    account_owner = _detect_account_owner(conn)
    owner_note = f"\nThe account owner is: {account_owner}" if account_owner else ""

    prompt = (
        f"Process all new emails for {company_name} ({company_domain}).{owner_note}\n"
        f"Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
        f"Follow your workflow: read config, check existing discussions, "
        f"read new emails, then process thread by thread — adding events, "
        f"discussions, and updates incrementally. Call finalise_changes when done."
    )

    import shutil
    import sys

    options_kwargs: dict[str, Any] = dict(
        system_prompt=AGENT_SYSTEM_PROMPT,
        allowed_tools=tool_names,
        mcp_servers={"email_db": server},
        permission_mode="bypassPermissions",
        max_turns=50,
    )

    # Use system CLI if available (bundled may be outdated)
    system_claude = shutil.which("claude")
    if system_claude:
        options_kwargs["cli_path"] = system_claude
    if model:
        options_kwargs["model"] = model

    options = ClaudeAgentOptions(**options_kwargs)

    result_text = ""
    tool_calls = 0
    agent_tracker = TokenTracker()
    cost_usd: float | None = None

    try:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        tool_calls += 1
                        logger.info("Agent tool call: %s", block.name)
                    elif isinstance(block, TextBlock):
                        result_text = block.text
            elif isinstance(message, ResultMessage):
                cost_usd = message.total_cost_usd
                # Extract token usage from the SDK result
                usage = message.usage or {}
                if usage.get("input_tokens") or usage.get("output_tokens"):
                    # Total usage across all iterations
                    total_input = (usage.get("input_tokens", 0)
                                   + usage.get("cache_creation_input_tokens", 0)
                                   + usage.get("cache_read_input_tokens", 0))
                    total_output = usage.get("output_tokens", 0)
                    # Record per-iteration if available
                    iterations = usage.get("iterations", [])
                    if iterations:
                        for it in iterations:
                            agent_tracker.record(TokenUsage(
                                input_tokens=(it.get("input_tokens", 0)
                                              + it.get("cache_creation_input_tokens", 0)
                                              + it.get("cache_read_input_tokens", 0)),
                                output_tokens=it.get("output_tokens", 0),
                            ))
                    else:
                        agent_tracker.record(TokenUsage(
                            input_tokens=total_input,
                            output_tokens=total_output,
                        ))
                    logger.info(
                        "Agent %s: %d input + %d output tokens, cost=$%.4f",
                        company_domain, total_input, total_output, cost_usd or 0,
                    )

                if message.subtype == "success":
                    logger.info("Agent completed for %s (%d tool calls)", company_domain, tool_calls)
                else:
                    logger.warning("Agent ended with %s for %s", message.subtype, company_domain)
    except Exception as e:
        logger.error("Agent session failed for %s: %s", company_domain, e)
        if any(changeset[k] for k in changeset):
            return ProposedChanges(changeset), f"Agent error (partial results): {e}", agent_tracker
        return None, f"Agent error: {e}", agent_tracker

    # Return the accumulated changeset + token tracker
    if any(changeset[k] for k in changeset):
        return ProposedChanges(changeset), result_text, agent_tracker
    return None, result_text, agent_tracker


def agent_update_company(
    conn: sqlite3.Connection,
    company_domain: str,
    model: str | None = None,
    auto_apply: bool = False,
    console: Any = None,
) -> dict[str, Any]:
    """Run agent for a company, propose changes, optionally apply them.

    Returns dict with: proposed (ProposedChanges), counts (if applied), summary.
    """
    company = fetchone(
        conn,
        "SELECT id, name, domain FROM companies WHERE domain = ? COLLATE NOCASE",
        (company_domain,),
    )
    if not company:
        logger.warning("Company not found: %s", company_domain)
        return {"proposed": None, "counts": None, "summary": "Company not found"}

    proposed, summary, agent_tracker = asyncio.run(
        _run_agent_for_company(
            conn, company["domain"], company["id"], company["name"], model=model,
        )
    )

    result: dict[str, Any] = {
        "proposed": proposed,
        "counts": None,
        "summary": summary,
        "token_tracker": agent_tracker,
    }

    if proposed and not proposed.is_empty and auto_apply:
        counts = apply_changes(conn, proposed, company["id"], company["domain"])
        result["counts"] = counts

    return result
