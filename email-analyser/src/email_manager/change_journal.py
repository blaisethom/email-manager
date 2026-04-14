"""Change journal: tracks what changed so downstream stages know what to process."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def record_change(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
    change_type: str,
    source_stage: str | None = None,
) -> None:
    """Record a single change journal entry."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO change_journal (entity_type, entity_id, change_type, source_stage, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (entity_type, entity_id, change_type, source_stage, now),
    )


def record_changes(
    conn: sqlite3.Connection,
    entries: list[tuple[str, str, str, str | None]],
) -> None:
    """Record multiple journal entries. Each tuple: (entity_type, entity_id, change_type, source_stage)."""
    if not entries:
        return
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO change_journal (entity_type, entity_id, change_type, source_stage, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        [(et, eid, ct, ss, now) for et, eid, ct, ss in entries],
    )


def get_dirty_company_domains(conn: sqlite3.Connection) -> list[str]:
    """Return company domains that have unprocessed changes.

    Looks up thread_id and company entity types in the journal,
    resolves thread changes to company domains via email addresses
    and the companies table.
    """
    # Direct company-level changes
    direct = conn.execute(
        """SELECT DISTINCT entity_id FROM change_journal
           WHERE entity_type = 'company' AND processed_at IS NULL"""
    ).fetchall()
    domains = {r[0] for r in direct}

    # Thread-level changes -> resolve to company domains
    thread_rows = conn.execute(
        """SELECT DISTINCT entity_id FROM change_journal
           WHERE entity_type = 'thread' AND processed_at IS NULL"""
    ).fetchall()
    if thread_rows:
        thread_ids = [r[0] for r in thread_rows]
        placeholders = ",".join("?" for _ in thread_ids)
        # Find company domains from email addresses in those threads
        company_rows = conn.execute(
            f"""SELECT DISTINCT c.domain
                FROM emails e
                JOIN company_contacts cc ON (
                    e.from_address = cc.contact_email
                    OR e.to_addresses LIKE '%%' || cc.contact_email || '%%'
                )
                JOIN companies c ON cc.company_id = c.id
                WHERE e.thread_id IN ({placeholders})""",
            thread_ids,
        ).fetchall()
        domains.update(r[0] for r in company_rows)

    return sorted(domains)


def mark_processed(
    conn: sqlite3.Connection,
    entity_type: str | None = None,
    entity_ids: list[str] | None = None,
    source_stage: str | None = None,
) -> int:
    """Mark journal entries as processed.

    Can filter by entity_type, specific entity_ids, and/or source_stage.
    Returns number of entries marked.
    """
    now = datetime.now(timezone.utc).isoformat()
    conditions = ["processed_at IS NULL"]
    params: list[str] = []

    if entity_type:
        conditions.append("entity_type = ?")
        params.append(entity_type)

    if entity_ids:
        placeholders = ",".join("?" for _ in entity_ids)
        conditions.append(f"entity_id IN ({placeholders})")
        params.extend(entity_ids)

    if source_stage:
        conditions.append("source_stage = ?")
        params.append(source_stage)

    where = " AND ".join(conditions)
    cursor = conn.execute(
        f"UPDATE change_journal SET processed_at = ? WHERE {where}",
        [now] + params,
    )
    return cursor.rowcount
