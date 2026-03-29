from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from email_manager.ai.base import LLMBackend
from email_manager.ai.prompts import (
    CATEGORISE_SYSTEM,
    CATEGORISE_USER,
    format_email_for_prompt,
)
from email_manager.db import fetchall, fetchone


def categorise_emails(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    batch_size: int = 10,
    on_progress: callable = None,
) -> int:
    # Get emails not yet categorised
    unprocessed = fetchall(
        conn,
        """SELECT e.id, e.message_id, e.subject, e.from_address, e.from_name,
                  e.body_text, e.date
           FROM emails e
           LEFT JOIN pipeline_runs pr ON e.id = pr.email_id AND pr.stage = 'categorise'
           WHERE pr.id IS NULL
           ORDER BY e.date DESC""",
    )

    if not unprocessed:
        return 0

    total_processed = 0

    # Process in batches
    for i in range(0, len(unprocessed), batch_size):
        batch = unprocessed[i : i + batch_size]

        try:
            _process_batch(conn, backend, batch)
            total_processed += len(batch)
        except Exception as e:
            # Retry individually on batch failure
            for email_row in batch:
                try:
                    _process_batch(conn, backend, [email_row])
                    total_processed += 1
                except Exception as inner_e:
                    _mark_error(conn, email_row["id"], "categorise", backend.model_name, str(inner_e))

        if on_progress:
            on_progress(total_processed, len(unprocessed))

    conn.commit()
    return total_processed


def _process_batch(
    conn: sqlite3.Connection, backend: LLMBackend, batch: list
) -> None:
    # Get existing projects for context
    existing = fetchall(conn, "SELECT name FROM projects ORDER BY name")
    existing_names = ", ".join(r["name"] for r in existing) if existing else "(none yet)"

    # Format emails for prompt
    emails_text = "\n".join(
        format_email_for_prompt(dict(row), idx) for idx, row in enumerate(batch)
    )

    prompt = CATEGORISE_USER.format(
        existing_projects=existing_names,
        emails=emails_text,
    )

    result = backend.complete_json(CATEGORISE_SYSTEM, prompt)
    assignments = result.get("assignments", [])

    now = datetime.now(timezone.utc).isoformat()

    for assignment in assignments:
        idx = assignment.get("email_index", 0)
        if idx >= len(batch):
            continue

        email_id = batch[idx]["id"]
        projects = assignment.get("projects", [])

        for proj in projects:
            proj_name = proj.get("name", "").strip()
            if not proj_name:
                continue
            confidence = proj.get("confidence", 0.5)

            # Get or create project
            project_id = _get_or_create_project(conn, proj_name)

            # Assign email to project
            conn.execute(
                """INSERT OR REPLACE INTO email_projects (email_id, project_id, confidence, assigned_by)
                VALUES (?, ?, ?, ?)""",
                (email_id, project_id, confidence, f"ai:{backend.model_name}"),
            )

        # Mark as processed
        conn.execute(
            """INSERT OR REPLACE INTO pipeline_runs (stage, email_id, status, model_used, started_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
            ("categorise", email_id, "complete", backend.model_name, now, now),
        )


def _get_or_create_project(conn: sqlite3.Connection, name: str) -> int:
    row = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (name,))
    if row:
        return row["id"]

    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO projects (name, created_at, is_auto) VALUES (?, ?, 1)",
        (name, now),
    )
    return cursor.lastrowid


def _mark_error(
    conn: sqlite3.Connection, email_id: int, stage: str, model: str, error: str
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO pipeline_runs (stage, email_id, status, model_used, started_at, completed_at, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (stage, email_id, "error", model, now, now, error[:500]),
    )
    conn.commit()
