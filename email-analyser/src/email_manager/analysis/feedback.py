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


def format_rules_block(conn: sqlite3.Connection, layer: str) -> str:
    """Build a prompt block with learned rules for injection into system prompts.

    Returns empty string if no rules exist for this layer.
    """
    rules = get_learned_rules(conn, layer)
    if not rules:
        return ""

    lines = ["\n\nLearned corrections from past reviews:"]
    for rule in rules:
        category_note = f" [{rule['category']}]" if rule.get("category") else ""
        lines.append(f"- {rule['rule_text']}{category_note}")

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
