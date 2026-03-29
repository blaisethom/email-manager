from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from email_manager.ai.base import LLMBackend
from email_manager.ai.prompts import THREAD_SUMMARY_SYSTEM, THREAD_SUMMARY_USER
from email_manager.db import fetchall


def summarise_threads(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    on_progress: callable = None,
) -> int:
    # Get threads that need summarisation (no summary yet, or new emails since last summary)
    threads = fetchall(
        conn,
        """SELECT t.thread_id, t.subject, t.participants, t.email_count
           FROM threads t
           WHERE t.summary IS NULL AND t.email_count > 0
           ORDER BY t.last_date DESC""",
    )

    if not threads:
        return 0

    total_processed = 0

    for thread_row in threads:
        try:
            # Get all emails in this thread
            emails = fetchall(
                conn,
                """SELECT from_name, from_address, date, body_text
                   FROM emails WHERE thread_id = ? ORDER BY date ASC""",
                (thread_row["thread_id"],),
            )

            if not emails:
                continue

            messages_text = "\n---\n".join(
                f"From: {e['from_name'] or e['from_address']} ({e['date'][:10]})\n{(e['body_text'] or '')[:300]}"
                for e in emails
            )

            participants = thread_row["participants"] or "[]"
            try:
                participants_list = json.loads(participants)
                participants_str = ", ".join(participants_list[:10])
            except (json.JSONDecodeError, TypeError):
                participants_str = participants

            prompt = THREAD_SUMMARY_USER.format(
                subject=thread_row["subject"] or "(no subject)",
                participants=participants_str,
                messages=messages_text[:3000],  # cap total size
            )

            result = backend.complete_json(THREAD_SUMMARY_SYSTEM, prompt)

            summary = result.get("summary", "")
            key_decisions = json.dumps(result.get("key_decisions", []))
            status = result.get("status", "unknown")

            conn.execute(
                """UPDATE threads SET summary = ?, summary_model = ?
                   WHERE thread_id = ?""",
                (summary, backend.model_name, thread_row["thread_id"]),
            )

            total_processed += 1

        except Exception:
            pass  # Skip failed threads silently

        conn.commit()

        if on_progress:
            on_progress(total_processed, len(threads))

    return total_processed
