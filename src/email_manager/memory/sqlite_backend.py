from __future__ import annotations

import json
import sqlite3

from email_manager.memory.base import ContactMemory


class SQLiteMemoryBackend:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def store(self, memory: ContactMemory) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO contact_memories
            (email, name, relationship, summary, discussions, key_facts,
             model_used, strategy_used, version, generated_at, emails_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                memory.email,
                memory.name,
                memory.relationship,
                memory.summary,
                json.dumps(memory.discussions),
                json.dumps(memory.key_facts),
                memory.model_used,
                memory.strategy_used,
                memory.version,
                memory.generated_at,
                memory.emails_hash,
            ),
        )
        self._conn.commit()

    def load(self, email: str) -> ContactMemory | None:
        row = self._conn.execute(
            "SELECT * FROM contact_memories WHERE email = ?", (email,)
        ).fetchone()
        if not row:
            return None
        return _row_to_memory(row)

    def load_all(self) -> list[ContactMemory]:
        rows = self._conn.execute(
            "SELECT * FROM contact_memories ORDER BY generated_at DESC"
        ).fetchall()
        return [_row_to_memory(r) for r in rows]

    def delete(self, email: str) -> None:
        self._conn.execute("DELETE FROM contact_memories WHERE email = ?", (email,))
        self._conn.commit()


def _row_to_memory(row) -> ContactMemory:
    return ContactMemory(
        email=row["email"],
        name=row["name"],
        relationship=row["relationship"] or "unknown",
        summary=row["summary"] or "",
        discussions=json.loads(row["discussions"]) if row["discussions"] else [],
        key_facts=json.loads(row["key_facts"]) if row["key_facts"] else [],
        generated_at=row["generated_at"] or "",
        model_used=row["model_used"] or "",
        strategy_used=row["strategy_used"] or "",
        version=row["version"] or 1,
        emails_hash=row["emails_hash"] or "",
    )
