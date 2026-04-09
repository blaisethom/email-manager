"""Propose next actions for active (non-terminal) discussions.

For each discussion that hasn't reached a terminal state, the LLM suggests
the next concrete step to take based on the current milestones, events,
and workflow state. Actions can include waiting until a specific date.
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

logger = logging.getLogger("email_manager.analysis.propose_actions")


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
        return data.get("categories", [])
    return data if isinstance(data, list) else []


def _get_category_config(categories: list[dict], name: str) -> dict | None:
    for cat in categories:
        if cat["name"] == name:
            return cat
    return None


# ── Prompt ──────────────────────────────────────────────────────────────────

PROPOSE_SYSTEM = """You are a business advisor assistant. Given a discussion's current state, milestones, and recent event history, propose the most important next action the user should take.

Rules:
1. Be specific and actionable — "Send follow-up email to John at john@acme.com asking for NDA status" not "Follow up".
2. If the right action is to wait (e.g. waiting for a response, waiting for a scheduled meeting), say so and specify a wait_until date (YYYY-MM-DD) after which to check in.
3. Consider the time since the last activity — if it's been a while, a follow-up or check-in may be appropriate.
4. The priority should be "high" (needs action this week), "medium" (needs action soon), or "low" (can wait).
5. If you can identify who should do it (from the participants), set the assignee email.
6. Provide brief reasoning (1-2 sentences) explaining why this is the right next step.
7. Propose 1-3 actions, ordered by priority. Most discussions need just 1.

Respond with JSON only."""


def _build_propose_prompt(
    discussion: dict[str, Any],
    events: list[dict[str, Any]],
    milestones: list[dict[str, Any]],
    category_config: dict[str, Any] | None,
    today: str,
) -> str:
    # Format milestones
    achieved = [m for m in milestones if m.get("achieved")]
    pending = [m for m in milestones if not m.get("achieved")]

    achieved_text = ", ".join(
        f'{m["name"]} ({m.get("achieved_date", "?")})'
        for m in achieved
    ) or "none"

    pending_text = ", ".join(m["name"] for m in pending) or "none"

    # Format recent events (last 10)
    recent = events[-10:] if len(events) > 10 else events
    events_text = "\n".join(
        f'  {ev.get("event_date", "?")} {ev["type"]}: {(ev.get("detail") or "")[:100]}'
        for ev in recent
    )

    # Workflow states
    states_text = ""
    if category_config:
        states = category_config.get("workflow_states", [])
        terminal = set(category_config.get("terminal_states", []))
        state_parts = [f"{s}*" if s in terminal else s for s in states]
        states_text = f"\nWorkflow progression: {' → '.join(state_parts)}  (* = terminal)"

    last_event_date = events[-1].get("event_date", "?") if events else "?"

    return f"""Propose the next action(s) for this discussion.

Today's date: {today}

Discussion: "{discussion['title']}" [{discussion.get('category', 'other')}]
Company: {discussion.get('company_name', 'unknown')}
Current state: {discussion.get('current_state', '?')}
{states_text}

Milestones achieved: {achieved_text}
Milestones pending: {pending_text}

Last activity: {last_event_date}
Summary: {discussion.get('summary', 'No summary available.')}

Recent events:
{events_text}

Participants: {discussion.get('participants', '[]')}

Respond with this exact JSON structure:
{{
  "actions": [
    {{
      "action": "Specific action to take",
      "reasoning": "Why this is the right next step",
      "priority": "high|medium|low",
      "wait_until": "YYYY-MM-DD or null",
      "assignee": "email@example.com or null"
    }}
  ]
}}"""


# ── Data gathering ──────────────────────────────────────────────────────────

def _get_active_discussions(
    conn: sqlite3.Connection,
    categories_config: list[dict[str, Any]],
    company_domain: str | None = None,
    company_label: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Get discussions not in a terminal state."""
    # Build set of terminal states per category
    terminal_states: set[str] = set()
    for cat in categories_config:
        terminal_states.update(cat.get("terminal_states", []))

    conditions = ["d.current_state IS NOT NULL"]
    params: list[Any] = []

    if company_domain:
        conditions.append("c.domain = ? COLLATE NOCASE")
        params.append(company_domain)

    if company_label:
        conditions.append("d.company_id IN (SELECT company_id FROM company_labels WHERE label = ?)")
        params.append(company_label)

    # Exclude terminal states
    if terminal_states:
        placeholders = ",".join("?" for _ in terminal_states)
        conditions.append(f"d.current_state NOT IN ({placeholders})")
        params.extend(terminal_states)

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


def _get_events_for_discussion(conn: sqlite3.Connection, discussion_id: int) -> list[dict]:
    rows = fetchall(
        conn,
        "SELECT * FROM event_ledger WHERE discussion_id = ? ORDER BY event_date ASC",
        (discussion_id,),
    )
    return [dict(r) for r in rows]


def _get_milestones_for_discussion(conn: sqlite3.Connection, discussion_id: int) -> list[dict]:
    rows = fetchall(
        conn,
        "SELECT * FROM milestones WHERE discussion_id = ? ORDER BY achieved DESC, achieved_date ASC NULLS LAST",
        (discussion_id,),
    )
    return [dict(r) for r in rows]


# ── Public entry point ──────────────────────────────────────────────────────

def propose_actions(
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
) -> int:
    """Propose next actions for active discussions.

    Returns the number of discussions with proposed actions.
    """
    if categories_config is None:
        categories_config = load_category_config(config_path)

    if clean:
        if company_domain:
            conn.execute(
                """DELETE FROM proposed_actions WHERE discussion_id IN (
                    SELECT d.id FROM discussions d
                    JOIN companies c ON d.company_id = c.id
                    WHERE c.domain = ? COLLATE NOCASE
                )""",
                (company_domain,),
            )
        else:
            conn.execute("DELETE FROM proposed_actions")
        conn.commit()

    discussions = _get_active_discussions(
        conn, categories_config, company_domain=company_domain,
        company_label=company_label, limit=limit,
    )

    if not force and not clean:
        # Skip discussions that already have recent proposed actions (within 24h)
        discussions = [
            d for d in discussions
            if not fetchone(
                conn,
                """SELECT 1 FROM proposed_actions
                   WHERE discussion_id = ? AND created_at > datetime('now', '-1 day')""",
                (d["id"],),
            )
        ]

    if not discussions:
        logger.info("No discussions need action proposals")
        return 0

    logger.info("Proposing actions for %d discussions", len(discussions))
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    for i, disc in enumerate(discussions):
        if on_progress:
            on_progress(i, len(discussions), disc["title"][:40])

        events = _get_events_for_discussion(conn, disc["id"])
        milestones = _get_milestones_for_discussion(conn, disc["id"])

        if not events:
            continue

        category_config = _get_category_config(categories_config, disc.get("category", ""))

        user_prompt = _build_propose_prompt(disc, events, milestones, category_config, today)

        try:
            result = backend.complete_json(PROPOSE_SYSTEM, user_prompt)
        except Exception as e:
            logger.error("LLM call failed for discussion %d (%s): %s", disc["id"], disc["title"], e)
            continue

        actions = result.get("actions", [])
        if not actions:
            continue

        # Clear old proposed actions for this discussion
        conn.execute("DELETE FROM proposed_actions WHERE discussion_id = ?", (disc["id"],))

        for action in actions:
            conn.execute(
                """INSERT INTO proposed_actions
                   (discussion_id, action, reasoning, priority, wait_until, assignee, model_used, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    disc["id"],
                    action.get("action", ""),
                    action.get("reasoning"),
                    action.get("priority", "medium"),
                    action.get("wait_until"),
                    action.get("assignee"),
                    backend.model_name,
                    now,
                ),
            )

        conn.commit()
        total += 1

        logger.info(
            "Discussion %d (%s): %d actions proposed",
            disc["id"], disc["title"][:40], len(actions),
        )

    if on_progress:
        on_progress(len(discussions), len(discussions), "")

    return total
