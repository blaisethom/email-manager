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

def propose_actions_propose(
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
) -> dict[str, Any] | None:
    """Run LLM calls and return a ProposedChanges-compatible dict without writing to DB.

    Returns None if there's nothing to do.
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
        discussions = [
            d for d in discussions
            if not fetchone(
                conn,
                """SELECT 1 FROM proposed_actions pa
                   WHERE pa.discussion_id = ?
                     AND pa.created_at > COALESCE(
                         (SELECT MAX(m.last_evaluated_at) FROM milestones m WHERE m.discussion_id = ?),
                         '1970-01-01'
                     )""",
                (d["id"], d["id"]),
            )
        ]

    if not discussions:
        logger.info("No discussions need action proposals")
        return None

    logger.info("Proposing actions for %d discussions", len(discussions))
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Enrich system prompt with learned rules
    from email_manager.analysis.feedback import format_rules_block, LAYER_ACTIONS
    system_prompt = PROPOSE_SYSTEM + format_rules_block(conn, LAYER_ACTIONS)

    # Pre-fetch context and build prompts
    work_items: list[tuple[dict, str]] = []
    for disc in discussions:
        events = _get_events_for_discussion(conn, disc["id"])
        if not events:
            continue
        milestones = _get_milestones_for_discussion(conn, disc["id"])
        category_config = _get_category_config(categories_config, disc.get("category", ""))
        prompt = _build_propose_prompt(disc, events, milestones, category_config, today)
        work_items.append((disc, prompt))

    if not work_items:
        return None

    # Collect LLM results into discussion_updates
    discussion_updates: list[dict] = []

    if concurrency > 1:
        import asyncio

        sem = asyncio.Semaphore(concurrency)

        async def _propose_one(prompt: str) -> dict | None:
            async with sem:
                try:
                    return await backend.acomplete_json(system_prompt, prompt)
                except Exception as e:
                    logger.error("Async LLM failed for propose_actions: %s", e)
                    return None

        async def _run():
            return await asyncio.gather(*[_propose_one(p) for _, p in work_items])

        results = asyncio.run(_run())

        for idx, ((disc, _), result) in enumerate(zip(work_items, results)):
            if on_progress:
                on_progress(idx, len(work_items), disc["title"][:40])
            if result is None:
                continue
            actions = result.get("actions", [])
            if actions:
                discussion_updates.append({
                    "discussion_id": disc["id"],
                    "proposed_actions": actions,
                })
    else:
        for i, (disc, prompt) in enumerate(work_items):
            if on_progress:
                on_progress(i, len(work_items), disc["title"][:40])
            try:
                result = backend.complete_json(system_prompt, prompt)
            except Exception as e:
                logger.error("LLM call failed for discussion %d (%s): %s", disc["id"], disc["title"], e)
                continue
            actions = result.get("actions", [])
            if actions:
                discussion_updates.append({
                    "discussion_id": disc["id"],
                    "proposed_actions": actions,
                })

    if on_progress:
        on_progress(len(work_items), len(work_items), "")

    if not discussion_updates:
        return None

    return {"discussion_updates": discussion_updates}


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
    concurrency: int = 1,
) -> int:
    """Propose next actions for active discussions.

    Args:
        concurrency: Max concurrent LLM calls. >1 enables parallel proposals.

    Returns the number of discussions with proposed actions.
    """
    from email_manager.ai.agent_backend import ProposedChanges, apply_changes
    from email_manager.analysis.feedback import compute_prompt_hash, format_rules_block, LAYER_ACTIONS

    _started = datetime.now(timezone.utc).isoformat()
    proposed_dict = propose_actions_propose(
        conn, backend, categories_config=categories_config,
        config_path=config_path, limit=limit, force=force, clean=clean,
        company_domain=company_domain, company_label=company_label,
        on_progress=on_progress, concurrency=concurrency,
    )
    if not proposed_dict:
        return 0

    p_hash = compute_prompt_hash(PROPOSE_SYSTEM + format_rules_block(conn, LAYER_ACTIONS))

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

    total_actions = 0
    for domain, updates in updates_by_company.items():
        proposed = ProposedChanges({"discussion_updates": updates})
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (domain,))
        cid = row["id"] if row else 0

        counts = apply_changes(
            conn, proposed, cid, domain,
            mode="staged:propose_actions", model=backend.model_name,
            prompt_hash=p_hash, started_at=_started,
            token_tracker=getattr(backend, "token_tracker", None),
        )
        total_actions += counts.get("actions", 0)

    return total_actions
