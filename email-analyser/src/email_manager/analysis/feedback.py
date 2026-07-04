"""Feedback and learned rules helpers for the analysis pipeline.

Provides functions to query learned rules and inject them into LLM prompts,
and compute prompt hashes for versioning.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

from email_manager.db import fetchall


# ── Layer constants ──────────────────────────────────────────────────────────

# These map to the 'layer' column in learned_rules and feedback tables
LAYER_EVENTS = "events"
LAYER_DISCUSSIONS = "discussions"
LAYER_ANALYSIS = "discussion_updates"
LAYER_ACTIONS = "actions"
LAYER_QUICK_UPDATE = "quick_update"
LAYER_AGENT = "agent"


# ── Learned rules ────────────────────────────────────────────────────────────

def get_learned_rules(conn: sqlite3.Connection, layer: str) -> list[dict[str, Any]]:
    """Get active learned rules for a given layer."""
    rows = fetchall(
        conn,
        "SELECT * FROM learned_rules WHERE layer = ? AND active = 1 ORDER BY id",
        (layer,),
    )
    return [dict(r) for r in rows]


def get_company_rules(conn: sqlite3.Connection, layer: str, company_id: int) -> list[dict[str, Any]]:
    """Get active learned rules for a specific company (category = '__company_{id}__')."""
    tag = f"__company_{company_id}__"
    rows = fetchall(
        conn,
        "SELECT * FROM learned_rules WHERE layer = ? AND category = ? AND active = 1 ORDER BY id",
        (layer, tag),
    )
    return [dict(r) for r in rows]


def format_rules_block(
    conn: sqlite3.Connection,
    layer: str,
    company_id: int | None = None,
    global_only: bool = False,
) -> str:
    """Build a prompt block with learned rules for injection into system prompts.

    When global_only=True, returns only rules with no category (global rules).
    When company_id is provided, returns global rules plus rules specific to that
    company (category = '__company_{id}__'). Company-specific rules for other
    companies are always excluded.

    Returns empty string if no rules exist for this layer.
    """
    all_rules = get_learned_rules(conn, layer)
    if not all_rules:
        return ""

    if global_only:
        rules = [r for r in all_rules if not r.get("category")]
    elif company_id is not None:
        company_tag = f"__company_{company_id}__"
        rules = [r for r in all_rules if not r.get("category") or r["category"] == company_tag]
    else:
        rules = all_rules

    if not rules:
        return ""

    lines = ["\n\nLearned corrections from past reviews:"]
    for rule in rules:
        lines.append(f"- {rule['rule_text']}")

    return "\n".join(lines)


# ── Few-shot examples ────────────────────────────────────────────────────────

def get_few_shot_examples(
    conn: sqlite3.Connection,
    layer: str,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """Get few-shot examples for a given layer and optional category."""
    if category:
        rows = fetchall(
            conn,
            """SELECT * FROM few_shot_examples
               WHERE layer = ? AND (category = ? OR category IS NULL)
               ORDER BY id""",
            (layer, category),
        )
    else:
        rows = fetchall(
            conn,
            "SELECT * FROM few_shot_examples WHERE layer = ? ORDER BY id",
            (layer,),
        )
    return [dict(r) for r in rows]


def format_examples_block(
    conn: sqlite3.Connection,
    layer: str,
    category: str | None = None,
) -> str:
    """Build a prompt block with few-shot examples.

    Returns empty string if no examples exist.
    """
    examples = get_few_shot_examples(conn, layer, category)
    if not examples:
        return ""

    lines = ["\n\nExamples from past corrections:"]
    for ex in examples:
        lines.append(f"\nInput:\n{ex['input_text'][:500]}")
        if ex.get("wrong_output"):
            lines.append(f"Wrong output:\n{ex['wrong_output'][:300]}")
        lines.append(f"Correct output:\n{ex['correct_output'][:500]}")

    return "\n".join(lines)


# ── Prompt hashing ───────────────────────────────────────────────────────────

def compute_prompt_hash(system_prompt: str) -> str:
    """Compute a short content hash of a system prompt for versioning.

    When the hash changes between runs, the prompt has changed and the stage
    may need re-running. The hash includes any injected learned rules.
    """
    return hashlib.sha256(system_prompt.encode()).hexdigest()[:16]
