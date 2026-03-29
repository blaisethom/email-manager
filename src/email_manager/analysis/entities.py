from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from email_manager.ai.base import LLMBackend
from email_manager.ai.prompts import (
    ENTITY_EXTRACTION_SYSTEM,
    ENTITY_EXTRACTION_USER,
    format_email_for_prompt,
)
from email_manager.db import fetchall


def extract_entities(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    batch_size: int = 10,
    on_progress: callable = None,
) -> int:
    unprocessed = fetchall(
        conn,
        """SELECT e.id, e.message_id, e.subject, e.from_address, e.from_name,
                  e.body_text, e.date
           FROM emails e
           LEFT JOIN pipeline_runs pr ON e.id = pr.email_id AND pr.stage = 'extract_entities'
           WHERE pr.id IS NULL
           ORDER BY e.date DESC""",
    )

    if not unprocessed:
        return 0

    total_processed = 0
    now = datetime.now(timezone.utc).isoformat()

    for i in range(0, len(unprocessed), batch_size):
        batch = unprocessed[i : i + batch_size]

        try:
            emails_text = "\n".join(
                format_email_for_prompt(dict(row), idx) for idx, row in enumerate(batch)
            )
            prompt = ENTITY_EXTRACTION_USER.format(emails=emails_text)
            result = backend.complete_json(ENTITY_EXTRACTION_SYSTEM, prompt)

            for extraction in result.get("extractions", []):
                idx = extraction.get("email_index", 0)
                if idx >= len(batch):
                    continue
                email_id = batch[idx]["id"]

                for entity in extraction.get("entities", []):
                    conn.execute(
                        """INSERT INTO entities (email_id, entity_type, value, context, confidence)
                        VALUES (?, ?, ?, ?, ?)""",
                        (
                            email_id,
                            entity.get("type", "topic"),
                            entity.get("value", ""),
                            entity.get("context", ""),
                            entity.get("confidence", 0.5),
                        ),
                    )

                conn.execute(
                    """INSERT OR REPLACE INTO pipeline_runs (stage, email_id, status, model_used, started_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    ("extract_entities", email_id, "complete", backend.model_name, now, now),
                )

            total_processed += len(batch)
        except Exception as e:
            # Mark batch as errored individually
            for row in batch:
                conn.execute(
                    """INSERT OR REPLACE INTO pipeline_runs (stage, email_id, status, model_used, started_at, completed_at, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    ("extract_entities", row["id"], "error", backend.model_name, now, now, str(e)[:500]),
                )

        conn.commit()

        if on_progress:
            on_progress(total_processed, len(unprocessed))

    return total_processed
