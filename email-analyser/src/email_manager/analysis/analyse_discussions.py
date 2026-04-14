"""Analyse discussions: evaluate milestones, infer state, and generate summary.

All three outputs are produced in a single LLM call per discussion, then
stored in separate tables for independent feedback targeting.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.analysis.analyse_discussions")


# ── Category config ─────────────────────────────────────────────────────────

def load_category_config(config_path: Path | None = None) -> list[dict[str, Any]]:
    """Load discussion category definitions from YAML."""
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


def _get_category_config(categories: list[dict], category_name: str) -> dict | None:
    """Find the config for a specific category."""
    for cat in categories:
        if cat["name"] == category_name:
            return cat
    return None


# ── Prompt construction ─────────────────────────────────────────────────────

ANALYSE_SYSTEM = """You are a discussion analysis system. Given a discussion's event history, you must:

1. Evaluate which milestones have been achieved based on the events.
2. Infer the current workflow state from the milestone profile.
3. Generate a concise narrative summary of the discussion's arc.

Rules:
1. A milestone is "achieved" only if the events clearly evidence it. Cite the event IDs.
2. Assign a confidence score (0.0-1.0) for each milestone based on evidence strength.
3. The workflow state should reflect where the discussion currently stands, not where it's been.
4. The summary should be 2-4 sentences covering the arc from first contact to current status.
5. If a discussion seems stalled (no recent activity), mention that in the summary.
6. Only use the "stale" state (where available) for discussions with NO activity in the last 3+ months AND no explicit terminal outcome (passed, signed, etc.). A few weeks of inactivity is normal — not stale.

Respond with JSON only."""


def _build_analyse_prompt(
    discussion: dict[str, Any],
    events: list[dict[str, Any]],
    category_config: dict[str, Any],
    feedback: list[dict[str, Any]] | None = None,
    children: list[dict[str, Any]] | None = None,
) -> str:
    """Build the user prompt for analysing a single discussion."""

    # Format events
    events_lines = []
    for ev in events:
        parts = [
            f"[{ev['id']}]",
            f"date={ev.get('event_date', '?')}",
            f"type={ev['type']}",
        ]
        if ev.get("actor"):
            parts.append(f"actor={ev['actor']}")
        if ev.get("target"):
            parts.append(f"target={ev['target']}")
        if ev.get("detail"):
            parts.append(f'"{ev["detail"]}"')
        events_lines.append(" | ".join(parts))
    events_text = "\n".join(events_lines)

    # Format milestones
    milestones = category_config.get("milestones", [])
    milestones_text = ""
    if milestones:
        milestones_text = "\nMilestones to evaluate:\n"
        for m in milestones:
            if isinstance(m, dict):
                milestones_text += f'  - {m["name"]}: {m["description"]}\n'
            else:
                milestones_text += f"  - {m}\n"

    # Format workflow states
    states = category_config.get("workflow_states", [])
    terminal = set(category_config.get("terminal_states", []))
    states_text = ""
    if states:
        state_parts = [f"{s}*" if s in terminal else s for s in states]
        states_text = f"\nWorkflow states: {' → '.join(state_parts)}  (* = terminal)\n"

    # Format feedback overrides
    feedback_text = ""
    if feedback:
        feedback_text = "\nUser feedback/corrections to apply:\n"
        for fb in feedback:
            feedback_text += f"  - {fb['action']}: {fb.get('new_value', fb.get('reason', ''))}\n"

    # Format child discussion summaries
    children_text = ""
    if children:
        children_text = "\nSub-discussions:\n"
        for child in children:
            state = child.get("current_state") or "?"
            summary = child.get("summary") or "No summary yet"
            children_text += f'  - "{child["title"]}" [{child.get("category", "?")}] state={state}: {summary}\n'

    return f"""Analyse this discussion and determine milestones, current state, and summary.

Discussion: "{discussion['title']}" [{discussion['category']}]
Company: {discussion.get('company_name', 'unknown')}
Participants: {discussion.get('participants', '[]')}
First seen: {discussion.get('first_seen', '?')}
Last seen: {discussion.get('last_seen', '?')}
{milestones_text}{states_text}{feedback_text}{children_text}
Event history (chronological):
{events_text}

Respond with this exact JSON structure:
{{
  "milestones": [
    {{
      "name": "milestone_name",
      "achieved": true,
      "achieved_date": "YYYY-MM-DD",
      "evidence_event_ids": ["evt_abc123"],
      "confidence": 0.9
    }}
  ],
  "workflow_state": "state-name",
  "summary": "2-4 sentence narrative summary of the discussion arc."
}}

Notes:
- Include ALL milestones from the list above, marking unachieved ones with "achieved": false.
- For unachieved milestones, set achieved_date to null, evidence_event_ids to [], confidence to 0.
- The workflow_state must be one of the states listed above.
- The summary should tell the story: who initiated, what's happened, where things stand now."""


# ── Data gathering ──────────────────────────────────────────────────────────

def _get_discussions_to_analyse(
    conn: sqlite3.Connection,
    limit: int | None = None,
    force: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
) -> list[dict[str, Any]]:
    """Get discussions that need analysis (milestones + state + summary).

    A discussion needs analysis if:
    - It has events but no milestones yet, OR
    - force=True, OR
    - It has new events since last analysis
    """
    conditions = ["1=1"]
    params: list[Any] = []

    if company_domain:
        conditions.append("c.domain = ?")
        params.append(company_domain)

    if company_label:
        conditions.append("d.company_id IN (SELECT company_id FROM company_labels WHERE label = ?)")
        params.append(company_label)

    if not force:
        # Discussions with events but no milestones, or with events newer than last analysis
        conditions.append("""(
            NOT EXISTS (SELECT 1 FROM milestones m WHERE m.discussion_id = d.id)
            OR EXISTS (
                SELECT 1 FROM event_ledger el
                WHERE el.discussion_id = d.id
                  AND el.created_at > COALESCE(
                      (SELECT MAX(m2.last_evaluated_at) FROM milestones m2 WHERE m2.discussion_id = d.id),
                      '1970-01-01'
                  )
            )
        )""")

    where = " AND ".join(conditions)
    sql = f"""SELECT d.id, d.title, d.category, d.current_state, d.summary,
                     d.participants, d.first_seen, d.last_seen,
                     c.name as company_name, c.domain as company_domain
              FROM discussions d
              LEFT JOIN companies c ON d.company_id = c.id
              WHERE {where}
              ORDER BY d.last_seen DESC"""
    if limit:
        sql += f" LIMIT {limit}"

    rows = fetchall(conn, sql, tuple(params))
    return [dict(r) for r in rows]


def _get_events_for_discussion(
    conn: sqlite3.Connection, discussion_id: int,
) -> list[dict[str, Any]]:
    """Get all events for a discussion, chronologically."""
    rows = fetchall(
        conn,
        """SELECT * FROM event_ledger
           WHERE discussion_id = ?
           ORDER BY event_date ASC, created_at ASC""",
        (discussion_id,),
    )
    return [dict(r) for r in rows]


def _get_feedback_for_discussion(
    conn: sqlite3.Connection, discussion_id: int,
) -> list[dict[str, Any]]:
    """Get unapplied feedback for a discussion."""
    rows = fetchall(
        conn,
        """SELECT * FROM feedback
           WHERE target_id = ? AND target_type = 'discussion' AND applied = 0
           ORDER BY created_at ASC""",
        (str(discussion_id),),
    )
    return [dict(r) for r in rows]


# ── Saving results ──────────────────────────────────────────────────────────

def _save_milestones(
    conn: sqlite3.Connection,
    discussion_id: int,
    milestones: list[dict[str, Any]],
) -> None:
    """Save milestone evaluations for a discussion."""
    now = datetime.now(timezone.utc).isoformat()
    for m in milestones:
        conn.execute(
            """INSERT INTO milestones (discussion_id, name, achieved, achieved_date,
               evidence_event_ids, confidence, last_evaluated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(discussion_id, name) DO UPDATE SET
               achieved = excluded.achieved,
               achieved_date = excluded.achieved_date,
               evidence_event_ids = excluded.evidence_event_ids,
               confidence = excluded.confidence,
               last_evaluated_at = excluded.last_evaluated_at""",
            (
                discussion_id,
                m.get("name", ""),
                1 if m.get("achieved") else 0,
                m.get("achieved_date"),
                json.dumps(m.get("evidence_event_ids", [])),
                m.get("confidence", 0.0),
                now,
            ),
        )


def _save_state_and_summary(
    conn: sqlite3.Connection,
    discussion_id: int,
    workflow_state: str | None,
    summary: str | None,
    model_used: str,
) -> None:
    """Update the discussion's workflow state and summary."""
    now = datetime.now(timezone.utc).isoformat()

    # Get current state to check for transitions
    current = fetchone(
        conn,
        "SELECT current_state FROM discussions WHERE id = ?",
        (discussion_id,),
    )
    old_state = current["current_state"] if current else None

    conn.execute(
        """UPDATE discussions SET
           current_state = ?,
           summary = ?,
           model_used = ?,
           updated_at = ?
           WHERE id = ?""",
        (workflow_state, summary, model_used, now, discussion_id),
    )

    # Record state transition if state changed
    if workflow_state and workflow_state != old_state:
        conn.execute(
            """INSERT INTO discussion_state_history
               (discussion_id, state, entered_at, reasoning, model_used, detected_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (discussion_id, workflow_state, now,
             f"Derived from milestone analysis", model_used, now),
        )

    # Propagate last_seen up to parent discussion
    parent_row = fetchone(
        conn, "SELECT parent_id FROM discussions WHERE id = ?", (discussion_id,),
    )
    if parent_row and parent_row["parent_id"]:
        conn.execute(
            """UPDATE discussions SET
               last_seen = MAX(last_seen, (
                   SELECT MAX(last_seen) FROM discussions WHERE parent_id = ?
               )),
               updated_at = ?
               WHERE id = ?""",
            (parent_row["parent_id"], now, parent_row["parent_id"]),
        )


# ── Public entry point ──────────────────────────────────────────────────────

def _clean_analysis(
    conn: sqlite3.Connection,
    company_domain: str | None = None,
) -> int:
    """Delete milestones and state history for discussions in scope."""
    if company_domain:
        disc_ids = [r[0] for r in fetchall(
            conn,
            """SELECT d.id FROM discussions d
               JOIN companies c ON d.company_id = c.id
               WHERE c.domain = ? COLLATE NOCASE""",
            (company_domain,),
        )]
    else:
        disc_ids = [r[0] for r in fetchall(conn, "SELECT id FROM discussions")]

    if not disc_ids:
        return 0

    placeholders = ",".join("?" for _ in disc_ids)
    params = tuple(disc_ids)
    conn.execute(f"DELETE FROM milestones WHERE discussion_id IN ({placeholders})", params)
    conn.execute(f"DELETE FROM discussion_state_history WHERE discussion_id IN ({placeholders})", params)
    conn.execute(
        f"UPDATE discussions SET current_state = NULL, summary = NULL WHERE id IN ({placeholders})",
        params,
    )
    conn.commit()
    logger.info("Cleaned analysis for %d discussions", len(disc_ids))
    return len(disc_ids)


def analyse_discussions_propose(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
    concurrency: int = 1,
) -> tuple[dict[str, Any] | None, list[int]]:
    """Run LLM calls and return a ProposedChanges-compatible dict without writing to DB.

    Returns (proposed_dict, feedback_ids_to_mark_applied) or (None, []).
    """
    if clean:
        _clean_analysis(conn, company_domain=company_domain)

    if categories_config is None:
        categories_config = load_category_config(config_path)

    discussions = _get_discussions_to_analyse(
        conn, limit=limit, force=force or clean, company_domain=company_domain,
        company_label=company_label,
    )
    if not discussions:
        logger.info("No discussions to analyse")
        return None, []

    logger.info("Analysing %d discussions", len(discussions))

    # Enrich system prompt with learned rules
    from email_manager.analysis.feedback import format_rules_block, LAYER_ANALYSIS
    system_prompt = ANALYSE_SYSTEM + format_rules_block(conn, LAYER_ANALYSIS)

    # Pre-fetch all context (fast DB reads) before LLM calls
    work_items: list[tuple[dict, str, list | None]] = []  # (disc, prompt, feedback)
    for disc in discussions:
        events = _get_events_for_discussion(conn, disc["id"])
        if not events:
            continue
        category_config = _get_category_config(categories_config, disc["category"])
        if not category_config:
            category_config = {
                "name": disc["category"], "milestones": [],
                "workflow_states": ["active", "resolved", "stalled"],
                "terminal_states": ["resolved"],
            }
        feedback = _get_feedback_for_discussion(conn, disc["id"])
        children = fetchall(conn,
            "SELECT id, title, category, current_state, summary, last_seen FROM discussions WHERE parent_id = ? ORDER BY last_seen DESC",
            (disc["id"],))
        children = [dict(c) for c in children] if children else None
        prompt = _build_analyse_prompt(disc, events, category_config, feedback or None, children=children)
        work_items.append((disc, prompt, feedback))

    if not work_items:
        return None, []

    # Collect LLM results into discussion_updates
    discussion_updates: list[dict] = []
    feedback_ids_to_apply: list[int] = []

    if concurrency > 1:
        import asyncio

        sem = asyncio.Semaphore(concurrency)

        async def _analyse_one(disc: dict, prompt: str) -> dict | None:
            async with sem:
                try:
                    return await backend.acomplete_json(system_prompt, prompt)
                except Exception as e:
                    logger.error("Async LLM failed for discussion %d: %s", disc["id"], e)
                    return None

        async def _run():
            tasks = [_analyse_one(d, p) for d, p, _ in work_items]
            return await asyncio.gather(*tasks)

        results = asyncio.run(_run())

        for idx, ((disc, _, feedback), result) in enumerate(zip(work_items, results)):
            if on_progress:
                on_progress(idx, len(work_items), disc["title"][:40])
            if result is None:
                continue
            discussion_updates.append({
                "discussion_id": disc["id"],
                "state": result.get("workflow_state"),
                "summary": result.get("summary"),
                "milestones": result.get("milestones", []),
            })
            if feedback:
                feedback_ids_to_apply.extend(fb["id"] for fb in feedback)
    else:
        for i, (disc, prompt, feedback) in enumerate(work_items):
            if on_progress:
                on_progress(i, len(work_items), disc["title"][:40])
            try:
                result = backend.complete_json(system_prompt, prompt)
            except Exception as e:
                logger.error("LLM call failed for discussion %d (%s): %s", disc["id"], disc["title"], e)
                continue
            discussion_updates.append({
                "discussion_id": disc["id"],
                "state": result.get("workflow_state"),
                "summary": result.get("summary"),
                "milestones": result.get("milestones", []),
            })
            if feedback:
                feedback_ids_to_apply.extend(fb["id"] for fb in feedback)

    if on_progress:
        on_progress(len(work_items), len(work_items), "")

    if not discussion_updates:
        return None, []

    return {"discussion_updates": discussion_updates}, feedback_ids_to_apply


def analyse_discussions(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    categories_config: list[dict[str, Any]] | None = None,
    config_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
    concurrency: int = 1,
) -> int:
    """Analyse discussions: evaluate milestones, infer state, generate summary.

    Args:
        concurrency: Max concurrent LLM calls. >1 enables parallel analysis.

    Returns the number of discussions analysed.
    """
    from email_manager.ai.agent_backend import ProposedChanges, apply_changes
    from email_manager.analysis.feedback import compute_prompt_hash, format_rules_block, LAYER_ANALYSIS

    proposed_dict, feedback_ids = analyse_discussions_propose(
        conn, backend, categories_config=categories_config,
        config_path=config_path, limit=limit, force=force, clean=clean,
        company_domain=company_domain, company_label=company_label,
        on_progress=on_progress, concurrency=concurrency,
    )
    if not proposed_dict:
        return 0

    p_hash = compute_prompt_hash(ANALYSE_SYSTEM + format_rules_block(conn, LAYER_ANALYSIS))

    # Group discussion_updates by company for per-company processing_runs
    updates_by_company: dict[str, list[dict]] = {}
    for update in proposed_dict.get("discussion_updates", []):
        disc_id = update.get("discussion_id")
        if not isinstance(disc_id, int):
            continue
        row = fetchone(
            conn,
            "SELECT c.domain, c.id FROM discussions d JOIN companies c ON d.company_id = c.id WHERE d.id = ?",
            (disc_id,),
        )
        if row:
            updates_by_company.setdefault(row["domain"], []).append(update)

    total_updates = 0
    for domain, updates in updates_by_company.items():
        proposed = ProposedChanges({"discussion_updates": updates})
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (domain,))
        cid = row["id"] if row else 0

        counts = apply_changes(
            conn, proposed, cid, domain,
            mode="staged:analyse_discussions", model=backend.model_name,
            prompt_hash=p_hash,
            token_tracker=getattr(backend, "token_tracker", None),
        )
        total_updates += counts.get("updates", 0)

    # Mark feedback as applied
    for fb_id in feedback_ids:
        conn.execute("UPDATE feedback SET applied = 1 WHERE id = ?", (fb_id,))
    if feedback_ids:
        conn.commit()

    # Propagate last_seen to parent discussions
    for update in proposed_dict.get("discussion_updates", []):
        disc_id = update.get("discussion_id")
        if isinstance(disc_id, int):
            parent_row = fetchone(
                conn, "SELECT parent_id FROM discussions WHERE id = ?", (disc_id,),
            )
            if parent_row and parent_row["parent_id"]:
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """UPDATE discussions SET
                       last_seen = MAX(last_seen, (
                           SELECT MAX(last_seen) FROM discussions WHERE parent_id = ?
                       )),
                       updated_at = ?
                       WHERE id = ?""",
                    (parent_row["parent_id"], now, parent_row["parent_id"]),
                )
    conn.commit()

    return total_updates
